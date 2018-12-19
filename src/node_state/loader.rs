use futures::{Async, Future, Poll};

use super::{Common, NextState};
use election::Role;
use log::{Log, LogIndex};
use {Error, Io, Result};

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
        // TODO: この二行の意味はきちんと説明して分けるべき
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
                    /*
                    issue12関連でNone = ballotを想定した挙動になっているか確認すること
                    とはいえまっさらなfrugalosクラスタだと、Loaderではじめるのか Followerではじめるのか、それとも？
                    前回の状況を復元ということは、初回は違うパスか？
                     */

                    // LogIndex::new(0)には何がある？
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
                            // ?? 長さだけというのはどういう意味か？
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
