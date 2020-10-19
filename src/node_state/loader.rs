use futures::{Async, Future, Poll};

use super::{Common, NextState};
use crate::election::Role;
use crate::log::{Log, LogIndex};
use crate::{Error, Io, Result};

/// ノード起動時に、前回の状況を復元(ロード)を行う.
pub struct Loader<IO: Io> {
    phase: Phase<IO::LoadBallot, IO::LoadLog>,
}
impl<IO: Io> Loader<IO> {
    pub fn new(common: &mut Common<IO>) -> Self {
        let phase = Phase::A(common.load_ballot());
        Loader { phase }
    }
    pub fn handle_timeout(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        // ロードにはタイムアウトは存在しないので、無条件で延長し続ける
        common.set_timeout(Role::Follower);
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        while let Async::Ready(phase) = track!(self.phase.poll().map_err(Error::from))? {
            let next = match phase {
                Phase::A(ballot) => {
                    // 1) 前回の投票状況を復元
                    if let Some(ballot) = ballot {
                        common.set_ballot(ballot);
                    }
                    let future = common.load_log(LogIndex::new(0), None);
                    Phase::B(future) // => ログ復元へ
                }
                Phase::B(log) => {
                    // 2) ローカルログの内容を復元
                    match log {
                        Log::Prefix(prefix) => {
                            // 2-1) スナップショットを読み込んだ
                            track!(common.handle_log_snapshot_installed(
                                prefix.tail,
                                prefix.config.clone(),
                            ))?;
                            track!(common.handle_log_snapshot_loaded(prefix))?;

                            let suffix_head = common.log().tail().index;
                            let future = common.load_log(suffix_head, None);
                            Phase::B(future) // => スナップショット以降のログ取得へ
                        }
                        Log::Suffix(suffix) => {
                            // 2-2) ログの末尾までを読み込んだ
                            //
                            // NOTE:
                            // ここで必要なのは「ローカルログの長さ」だけなので、
                            // 本来は後半部分を全て読み込む必要はない.
                            //
                            // もしこれに起因した現実的な性能問題が発生するようであれば、
                            // 「ローカルログの長さ取得」を行うための専用メソッドを、
                            // `Io`トレイトに追加しても良いかもしれない.
                            track!(common.handle_log_appended(&suffix))?;

                            // FIXME:
                            // 起動直後にcandidate状態に遷移してしまうと、
                            // 前回停止時からtermが変わっていない場合に、
                            // 常に再選挙が行われてしまうことになるので、修正したい.
                            // (致命的な問題がある訳ではないが)
                            //
                            // candidateに遷移するのは`index==0`の場合のみ、とか？
                            // 若干起動時の待ちが増える可能性はあるが、全部follower、として起動する、
                            // というのもありかもしれない.
                            let next = common.transit_to_candidate();
                            return Ok(Some(next));
                        }
                    }
                }
            };
            self.phase = next;
        }
        Ok(None)
    }
}

#[derive(Debug)]
enum Phase<A, B> {
    A(A),
    B(B),
}
impl<A, B> Future for Phase<A, B>
where
    A: Future<Error = Error>,
    B: Future<Error = Error>,
{
    type Item = Phase<A::Item, B::Item>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self {
            Phase::A(f) => track!(f.poll()).map(|t| t.map(Phase::A)),
            Phase::B(f) => track!(f.poll()).map(|t| t.map(Phase::B)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometrics::metrics::MetricBuilder;

    use crate::election::Term;
    use crate::log::{LogEntry, LogPosition, LogPrefix, LogSuffix};
    use crate::metrics::NodeStateMetrics;
    use crate::node::NodeId;
    use crate::test_util::tests::TestIoBuilder;
    use trackable::result::TestResult;

    #[test]
    fn it_works() -> TestResult {
        let node_id: NodeId = "node1".into();
        let metrics = track!(NodeStateMetrics::new(&MetricBuilder::new()))?;
        let io = TestIoBuilder::new().add_member(node_id.clone()).finish();
        let mut handle = io.handle();
        let cluster = io.cluster.clone();
        let mut common = Common::new(node_id, io, cluster.clone(), metrics);
        let mut loader = Loader::new(&mut common);

        // prefix には空の snapshot があり、tail は 1 を指している。
        // suffix には position 1 から 1 エントリが保存されている。
        // term は変更なし。
        let term = Term::new(1);
        let suffix_head = LogIndex::new(1);
        let prefix_tail = LogPosition {
            prev_term: term,
            index: suffix_head,
        };
        handle.set_initial_log_prefix(LogPrefix {
            tail: prefix_tail,
            config: cluster,
            snapshot: vec![],
        });
        handle.set_initial_log_suffix(
            suffix_head,
            LogSuffix {
                head: LogPosition {
                    prev_term: term,
                    index: suffix_head,
                },
                entries: vec![LogEntry::Noop { term }],
            },
        );
        loop {
            if let Some(next) = track!(loader.run_once(&mut common))? {
                assert!(next.is_candidate());
                // term は変化なし
                assert_eq!(term, common.log().tail().prev_term);
                // 追記されたログエントリの tail が 1 つ先に進んでいる
                assert_eq!(LogIndex::new(2), common.log().tail().index);
                // consumed と committed は prefix の状態のまま
                assert_eq!(prefix_tail.index, common.log().consumed_tail().index);
                assert_eq!(prefix_tail.index, common.log().committed_tail().index);
                break;
            }
        }

        Ok(())
    }

    #[test]
    fn it_fails_if_log_suffix_contains_older_term() -> TestResult {
        let node_id: NodeId = "node1".into();
        let metrics = track!(NodeStateMetrics::new(&MetricBuilder::new()))?;
        let io = TestIoBuilder::new().add_member(node_id.clone()).finish();
        let mut handle = io.handle();
        let cluster = io.cluster.clone();
        let mut common = Common::new(node_id, io, cluster.clone(), metrics);
        let mut loader = Loader::new(&mut common);

        // 古い term のログが紛れ込んでいるとエラーになる
        let term = Term::new(308);
        let suffix_head = LogIndex::new(28_405_496);
        let prefix_tail = LogPosition {
            prev_term: term,
            index: suffix_head,
        };
        handle.set_initial_log_prefix(LogPrefix {
            tail: prefix_tail,
            config: cluster,
            snapshot: vec![],
        });
        handle.set_initial_log_suffix(
            suffix_head,
            LogSuffix {
                head: LogPosition {
                    prev_term: term,
                    index: suffix_head,
                },
                entries: vec![
                    LogEntry::Noop { term },
                    LogEntry::Noop {
                        term: Term::new(term.as_u64() - 1),
                    },
                ],
            },
        );

        // Error: Other (cause; assertion failed: `self.last_record().head.prev_term < tail.prev_term`; last_record.head=LogPosition { prev_term: Term(308), index: LogIndex(28405496) }, tail=LogPosition { prev_term: Term(307), index: LogIndex(28405498) })
        //HISTORY:
        //  [0] at src/log/history.rs:104
        //  [1] at src/node_state/common/mod.rs:78
        //  [2] at src/node_state/loader.rs:58
        //  [3] at src/node_state/loader.rs:198
        assert!(loader.run_once(&mut common).is_err());

        Ok(())
    }
}
