use futures::{Async, Future, Poll};
use std::collections::VecDeque;

use self::rpc_builder::{RpcCallee, RpcCaller};
use super::candidate::Candidate;
use super::follower::Follower;
use super::leader::Leader;
use super::{NextState, RoleState};
use crate::cluster::ClusterConfig;
use crate::election::{Ballot, Role, Term};
use crate::log::{Log, LogHistory, LogIndex, LogPosition, LogPrefix, LogSuffix};
use crate::message::{Message, MessageHeader, SequenceNumber};
use crate::metrics::NodeStateMetrics;
use crate::node::{Node, NodeId};
use crate::{Error, ErrorKind, Event, Io, Result};

mod rpc_builder;

/// 全ての状態に共通する処理をまとめた構造体.
pub struct Common<IO: Io> {
    local_node: Node,
    history: LogHistory,
    timeout: IO::Timeout,
    events: VecDeque<Event>,
    io: IO,
    unread_message: Option<Message>,
    seq_no: SequenceNumber,
    load_committed: Option<IO::LoadLog>,
    install_snapshot: Option<InstallSnapshot<IO>>,
    metrics: NodeStateMetrics,
}
impl<IO> Common<IO>
where
    IO: Io,
{
    /// 新しい`Common`インスタンスを生成する.
    pub fn new(
        node_id: NodeId,
        mut io: IO,
        config: ClusterConfig,
        metrics: NodeStateMetrics,
    ) -> Self {
        // 最初は（仮に）フォロワーだとしておく
        let timeout = io.create_timeout(Role::Follower);
        Common {
            local_node: Node::new(node_id),
            io,
            history: LogHistory::new(config),
            unread_message: None,
            seq_no: SequenceNumber::new(0),
            timeout,
            events: VecDeque::new(),
            load_committed: None,
            install_snapshot: None,
            metrics,
        }
    }

    /// 現在のクラスタの構成情報を返す.
    pub fn config(&self) -> &ClusterConfig {
        self.history.config()
    }

    /// ローカルログ（の歴史）を返す.
    pub fn log(&self) -> &LogHistory {
        &self.history
    }

    /// ローカルログのコミット済み領域の終端位置を返す.
    pub fn log_committed_tail(&self) -> LogPosition {
        self.history.committed_tail()
    }

    /// 現在の`Term` (選挙番号) を返す.
    pub fn term(&self) -> Term {
        self.local_node.ballot.term
    }

    /// ローカルノードの情報を返す.
    pub fn local_node(&self) -> &Node {
        &self.local_node
    }

    /// ローカルログへの追記イベントを処理する.
    pub fn handle_log_appended(&mut self, suffix: &LogSuffix) -> Result<()> {
        track!(self.history.record_appended(suffix))
    }

    /// ログのコミットイベントを処理する.
    pub fn handle_log_committed(&mut self, new_tail: LogIndex) -> Result<()> {
        track!(self.history.record_committed(new_tail))
    }

    /// ローカルログのロールバックイベントを処理する.
    pub fn handle_log_rollbacked(&mut self, new_tail: LogPosition) -> Result<()> {
        track!(self.history.record_rollback(new_tail))
    }

    /// ログのスナップショットインストール完了イベントを処理する.
    pub fn handle_log_snapshot_installed(
        &mut self,
        new_head: LogPosition,
        config: ClusterConfig,
    ) -> Result<()> {
        track!(self.history.record_snapshot_installed(new_head, config))
    }

    /// ログのスナップショットロードイベントを処理する.
    pub fn handle_log_snapshot_loaded(&mut self, prefix: LogPrefix) -> Result<()> {
        if self.history.committed_tail().index < prefix.tail.index {
            // タイミング次第では、進行中のスナップショットインストールを追い越して、
            // ロードが発生してしまうことがあるので、その場合でも`LogHistory`の整合性が崩れないように、
            // 先にインストールが完了したものとして処理してしまう.
            // (`consumed_tail.index <= committed_tail.index`の不変項を維持するため)
            //
            // NOTE: "タイミング次第"の例
            // - 1. インストールが物理的には完了
            //   - スナップショット地点以前のログは削除された
            //   - raftlog層への通知はまだ
            // - 2. スナップショット地点以前へのロード要求が発行された
            // - 3. ログは残っていないので、1のスナップショットをロードする
            //   => このメソッドに入ってくる
            // - 4. インストール完了が通知される
            track!(self
                .history
                .record_snapshot_installed(prefix.tail, prefix.config.clone(),))?;
        }
        track!(self.history.record_snapshot_loaded(&prefix))?;
        let event = Event::SnapshotLoaded {
            new_head: prefix.tail,
            snapshot: prefix.snapshot,
        };
        self.metrics.event_queue_len.increment();
        self.events.push_back(event);
        Ok(())
    }

    /// ローカルノードの投票状況を更新する.
    pub fn set_ballot(&mut self, new_ballot: Ballot) {
        if self.local_node.ballot != new_ballot {
            self.local_node.ballot = new_ballot.clone();
            self.metrics.event_queue_len.increment();
            self.events.push_back(Event::TermChanged { new_ballot });
        }
    }

    /// スナップショットをインストール中の場合には`true`を返す.
    ///
    /// このメソッドが`true`を返している間は、
    /// 新しいスナップショットのインストールを行うことはできない.
    pub fn is_snapshot_installing(&self) -> bool {
        self.install_snapshot.is_some()
    }

    /// Returns `true` if and only if a node is installing snapshot and should not do
    /// anything else until the running snapshot installation completes.
    /// This method should be used to determine the next state of a node.
    ///
    /// The difference between `is_snapshot_installing` and `is_focusing_on_installing_snapshot` is
    /// that a node can concurrently process multiple tasks while installing snapshot.
    ///
    /// Calls `is_snapshot_installing` if you want to confirm whether another snapshot installation
    /// is running or not.
    pub fn is_focusing_on_installing_snapshot(&self) -> bool {
        if let Some(ref snapshot) = self.install_snapshot {
            // This condition is a bit complicated.
            // See https://github.com/frugalos/raftlog/pull/16#discussion_r250061583.
            return self.log().tail().index < snapshot.summary.tail.index;
        }
        false
    }

    /// `Leader`状態に遷移する.
    pub fn transit_to_leader(&mut self) -> RoleState<IO> {
        self.metrics.transit_to_leader_total.increment();
        self.set_role(Role::Leader);
        self.notify_new_leader_elected();
        RoleState::Leader(Leader::new(self))
    }

    /// `Candidate`状態に遷移する.
    pub fn transit_to_candidate(&mut self) -> RoleState<IO> {
        self.metrics.transit_to_candidate_total.increment();
        let new_ballot = Ballot {
            term: (self.local_node.ballot.term.as_u64() + 1).into(),
            voted_for: self.local_node.id.clone(),
        };
        self.set_ballot(new_ballot);
        self.set_role(Role::Candidate);
        RoleState::Candidate(Candidate::new(self))
    }

    /// `Follower`状態に遷移する.
    pub fn transit_to_follower(
        &mut self,
        followee: NodeId,
        pending_vote: Option<MessageHeader>,
    ) -> RoleState<IO> {
        self.metrics.transit_to_follower_total.increment();
        let new_ballot = Ballot {
            term: self.local_node.ballot.term,
            voted_for: followee,
        };
        self.set_ballot(new_ballot);
        self.set_role(Role::Follower);
        self.notify_new_leader_elected();
        RoleState::Follower(Follower::new(self, pending_vote))
    }

    /// 新しいリーダーが選出されたことを通知する.
    pub fn notify_new_leader_elected(&mut self) {
        self.events.push_back(Event::NewLeaderElected);
    }

    /// 次のメッセージ送信に使用されるシーケンス番号を返す.
    ///
    /// このメソッド自体は単に値を返すのみであり、
    /// 番号のインクリメントを行うことはない.
    pub fn next_seq_no(&self) -> SequenceNumber {
        self.seq_no
    }

    /// `IO`への参照を返す.
    pub fn io(&self) -> &IO {
        &self.io
    }

    /// `IO`への破壊的な参照を返す.
    ///
    /// 使い方を間違えるとデータの整合性を破壊してしまう可能性があるので、
    /// 注意を喚起する意味を込めて`unsafe`とする.
    pub unsafe fn io_mut(&mut self) -> &mut IO {
        &mut self.io
    }

    /// 指定範囲のローカルログをロードする.
    pub fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> IO::LoadLog {
        self.io.load_log(start, end)
    }

    /// ローカルログの末尾部分に`suffix`を追記する.
    pub fn save_log_suffix(&mut self, suffix: &LogSuffix) -> IO::SaveLog {
        self.io.save_log_suffix(suffix)
    }

    /// 現在の投票状況を保存する.
    pub fn save_ballot(&mut self) -> IO::SaveBallot {
        self.io.save_ballot(self.local_node.ballot.clone())
    }

    /// 以前の投票状況を復元する.
    pub fn load_ballot(&mut self) -> IO::LoadBallot {
        self.io.load_ballot()
    }

    /// 指定されたロール用のタイムアウトを設定する.
    pub fn set_timeout(&mut self, role: Role) {
        self.timeout = self.io.create_timeout(role);
    }

    /// タイムアウトに達していないかを確認する.
    pub fn poll_timeout(&mut self) -> Result<Async<()>> {
        track!(self.timeout.poll())
    }

    /// ユーザに通知するイベントがある場合には、それを返す.
    pub fn next_event(&mut self) -> Option<Event> {
        self.metrics.event_queue_len.decrement();
        self.events.pop_front()
    }

    /// 受信メッセージがある場合には、それを返す.
    pub fn try_recv_message(&mut self) -> Result<Option<Message>> {
        if let Some(message) = self.unread_message.take() {
            Ok(Some(message))
        } else {
            track!(self.io.try_recv_message())
        }
    }

    /// ローカルログのスナップショットのインストールを開始する.
    pub fn install_snapshot(&mut self, snapshot: LogPrefix) -> Result<()> {
        track_assert!(
            self.history.head().index <= snapshot.tail.index,
            ErrorKind::InconsistentState
        );
        track_assert!(self.install_snapshot.is_none(), ErrorKind::Busy);

        let future = InstallSnapshot::new(self, snapshot);
        self.install_snapshot = Some(future);
        Ok(())
    }

    /// 受信メッセージに対する共通的な処理を実行する.
    pub fn handle_message(&mut self, message: Message) -> HandleMessageResult<IO> {
        if self.local_node.role == Role::Leader
            && !self.config().is_known_node(&message.header().sender)
        {
            // a) リーダは、不明なノードからのメッセージは無視
            //
            //  リーダ以外は、クラスタの構成変更を跨いで再起動が発生した場合に、
            //  停止時には知らなかった新構成を把握するために、
            //  不明なノードからもメッセージも受信する必要がある.
            HandleMessageResult::Handled(None)
        } else if message.header().term > self.local_node.ballot.term {
            // b) 相手のtermの方が大きい => 新しい選挙が始まっているので追従する
            let is_follower = self.local_node.ballot.voted_for != self.local_node.id;
            if is_follower && self.local_node.ballot.voted_for != message.header().sender {
                // リーダをフォロー中(i.e., 定期的にハートビートを受信できている)の場合には、
                // そのリーダを信じて、現在の選挙を維持する.
                //
                // これはクラスタ構成変更時に、旧構成のメンバによって、延々と新選挙の開始が繰り返されてしまう
                // 可能性がある問題への対処となる.
                // この問題の詳細は論文の「6 Cluster membership changes」の"The third issue is ..."部分を参照のこと.
                return HandleMessageResult::Handled(None);
            }

            self.local_node.ballot.term = message.header().term;
            let next_state = if let Message::RequestVoteCall(m) = message {
                if m.log_tail.is_newer_or_equal_than(self.history.tail()) {
                    // 送信者(候補者)のログは十分に新しいので、その人を支持する
                    let candidate = m.header.sender.clone();
                    self.transit_to_follower(candidate, Some(m.header))
                } else {
                    // ローカルログの方が新しいので、自分で立候補する
                    self.transit_to_candidate()
                }
            } else if let Message::AppendEntriesCall { .. } = message {
                // 新リーダが当選していたので、その人のフォロワーとなる
                let leader = message.header().sender.clone();
                self.unread_message = Some(message);
                self.transit_to_follower(leader, None)
            } else if self.local_node.role == Role::Leader {
                self.transit_to_candidate()
            } else {
                let local = self.local_node.id.clone();
                self.transit_to_follower(local, None)
            };
            HandleMessageResult::Handled(Some(next_state))
        } else if message.header().term < self.local_node.ballot.term {
            // c) 自分のtermの方が大きい => 選挙期間が古くなっていることを送信元の通知

            // NOTE: 返信メッセージの中身は重要ではないので、一番害の無さそうなものを送っておく
            self.rpc_callee(message.header()).reply_request_vote(false);
            HandleMessageResult::Handled(None)
        } else {
            // d) 同じ選挙期間に属するノードからのメッセージ
            match message {
                Message::RequestVoteCall { .. } if !self.is_following_sender(&message) => {
                    // 別の人をフォロー中に投票依頼が来た場合ので拒否
                    self.rpc_callee(message.header()).reply_request_vote(false);
                    HandleMessageResult::Handled(None)
                }
                Message::AppendEntriesCall { .. } if !self.is_following_sender(&message) => {
                    // リーダが確定したので、フォロー先を変更する
                    let leader = message.header().sender.clone();
                    self.unread_message = Some(message);
                    let next = self.transit_to_follower(leader, None);
                    HandleMessageResult::Handled(Some(next))
                }
                _ => HandleMessageResult::Unhandled(message), // 個別のロールに処理を任せる
            }
        }
    }

    /// バックグランド処理を一単位実行する.
    pub fn run_once(&mut self) -> Result<NextState<IO>> {
        loop {
            // スナップショットのインストール処理
            if let Async::Ready(Some(summary)) = track!(self.install_snapshot.poll())? {
                let SnapshotSummary {
                    tail: new_head,
                    config,
                } = summary;
                self.install_snapshot = None;
                self.events.push_back(Event::SnapshotInstalled { new_head });
                track!(self.history.record_snapshot_installed(new_head, config))?;
            }

            // コミット済みログの処理.
            if let Async::Ready(Some(log)) = track!(self.load_committed.poll())? {
                // コミット済みのログを取得したので、ユーザに（イベント経由で）通知する.
                self.load_committed = None;
                match log {
                    Log::Prefix(snapshot) => track!(self.handle_log_snapshot_loaded(snapshot))?,
                    Log::Suffix(slice) => track!(self.handle_committed(slice))?,
                }
            }

            if self.load_committed.is_some()
                || self.history.consumed_tail().index == self.history.committed_tail().index
            {
                // コミット済みのログの読み込み中 or 未処理のコミット済みログ領域がない
                break;
            }

            let start = self.history.consumed_tail().index;
            let end = self.history.committed_tail().index;
            self.load_committed = Some(self.load_log(start, Some(end)));
        }
        Ok(None)
    }

    /// RPCの要求用のインスタンスを返す.
    pub fn rpc_caller(&mut self) -> RpcCaller<IO> {
        RpcCaller::new(self)
    }

    /// RPCの応答用のインスタンスを返す.
    pub fn rpc_callee<'a>(&'a mut self, caller: &'a MessageHeader) -> RpcCallee<IO> {
        RpcCallee::new(self, caller)
    }

    fn handle_committed(&mut self, suffix: LogSuffix) -> Result<()> {
        let new_tail = suffix.tail();
        for (index, entry) in (suffix.head.index.as_u64()..)
            .map(LogIndex::new)
            .zip(suffix.entries.into_iter())
        {
            let event = Event::Committed { index, entry };
            self.events.push_back(event);
        }
        if new_tail.index >= self.log().head().index {
            // 「ローカルログの終端よりも先の地点のスナップショット」をインストールした後、
            // そのスナップショットのロードが行われるまでの間には、上の条件が`false`になる可能性がある.
            track!(self.history.record_consumed(new_tail.index))?;
        }
        Ok(())
    }
    fn set_role(&mut self, new_role: Role) {
        if self.local_node.role != new_role {
            self.local_node.role = new_role;
            self.events.push_back(Event::RoleChanged { new_role });
        }
    }
    fn is_following_sender(&self, message: &Message) -> bool {
        self.local_node.ballot.voted_for == message.header().sender
    }
}

pub enum HandleMessageResult<IO: Io> {
    Handled(Option<RoleState<IO>>),
    Unhandled(Message),
}

#[derive(Debug, Clone)]
struct SnapshotSummary {
    tail: LogPosition,
    config: ClusterConfig,
}

struct InstallSnapshot<IO: Io> {
    future: IO::SaveLog,
    summary: SnapshotSummary,
}
impl<IO: Io> InstallSnapshot<IO> {
    pub fn new(common: &mut Common<IO>, prefix: LogPrefix) -> Self {
        let summary = SnapshotSummary {
            tail: prefix.tail,
            config: prefix.config.clone(),
        };
        let future = common.io.save_log_prefix(prefix);
        InstallSnapshot { future, summary }
    }
}
impl<IO: Io> Future for InstallSnapshot<IO> {
    type Item = SnapshotSummary;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(track!(self.future.poll())?.map(|()| self.summary.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometrics::metrics::MetricBuilder;
    use trackable::result::TestResult;

    use crate::log::{LogEntry, LogPrefix};
    use crate::metrics::NodeStateMetrics;
    use crate::test_util::tests::TestIoBuilder;

    #[test]
    fn is_snapshot_installing_works() -> TestResult {
        let node_id: NodeId = "node1".into();
        let metrics = track!(NodeStateMetrics::new(&MetricBuilder::new()))?;
        let io = TestIoBuilder::new()
            .add_member(node_id.clone())
            .add_member("node2".into())
            .add_member("node3".into())
            .finish();
        let cluster = io.cluster.clone();
        let mut common = Common::new(node_id, io, cluster.clone(), metrics);
        let prefix = LogPrefix {
            tail: LogPosition::default(),
            config: cluster,
            snapshot: Vec::default(),
        };

        assert!(!common.is_snapshot_installing());
        common.install_snapshot(prefix)?;
        assert!(common.is_snapshot_installing());

        Ok(())
    }

    #[test]
    fn is_focusing_on_installing_snapshot_works() -> TestResult {
        let node_id: NodeId = "node1".into();
        let metrics = track!(NodeStateMetrics::new(&MetricBuilder::new()))?;
        let io = TestIoBuilder::new()
            .add_member(node_id.clone())
            .add_member("node2".into())
            .add_member("node3".into())
            .finish();
        let cluster = io.cluster.clone();
        let mut common = Common::new(node_id, io, cluster.clone(), metrics);
        let prev_term = Term::new(0);
        let node_prefix = LogPrefix {
            tail: LogPosition {
                prev_term,
                index: LogIndex::new(3),
            },
            config: cluster.clone(),
            snapshot: vec![0],
        };
        let log_suffix = LogSuffix {
            head: LogPosition {
                prev_term,
                index: LogIndex::new(3),
            },
            entries: vec![
                LogEntry::Command {
                    term: prev_term,
                    command: Vec::default(),
                },
                LogEntry::Command {
                    term: prev_term,
                    command: Vec::default(),
                },
                LogEntry::Command {
                    term: prev_term,
                    command: Vec::default(),
                },
            ],
        };
        // The prefix of a leader is a bit ahead.
        let leader_prefix = LogPrefix {
            tail: LogPosition {
                prev_term,
                index: LogIndex::new(5),
            },
            config: cluster,
            snapshot: vec![1],
        };

        assert!(!common.is_focusing_on_installing_snapshot());
        // Applies a prefix before tests.
        common.handle_log_snapshot_loaded(node_prefix)?;
        common.install_snapshot(leader_prefix)?;
        // The node is installing a snapshot and focusing on the installation.
        assert!(common.is_focusing_on_installing_snapshot());
        // Appends new log entries.
        // Now `committed_tail` < `the tail of a prefix(snapshot)` < `appended_tail`
        common.handle_log_appended(&log_suffix)?;
        assert_eq!(
            common.log().tail(),
            LogPosition {
                prev_term,
                index: LogIndex::new(6)
            }
        );
        // The node is not focusing on the installation.
        assert!(!common.is_focusing_on_installing_snapshot());

        Ok(())
    }
}
