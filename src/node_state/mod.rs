use futures::task::{Context, Poll};
use futures::Stream;
use std::pin::Pin;
use std::time::Instant;

pub use self::common::Common;

use self::candidate::Candidate;
use self::common::HandleMessageResult;
use self::follower::Follower;
use self::leader::Leader;
use self::loader::Loader;
use crate::cluster::ClusterConfig;
use crate::message::Message;
use crate::metrics::NodeStateMetrics;
use crate::node::NodeId;
use crate::{Event, Io, Result};

mod candidate;
mod common;
mod follower;
mod leader;
mod loader;

/// 次に遷移する状態.
///
/// `None`の場合には、遷移はせずに同じ状態が維持される.
type NextState<IO> = Option<RoleState<IO>>;

/// ローカルノード用の状態(状態機械).
pub struct NodeState<IO: Io> {
    pub common: Common<IO>,
    pub role: RoleState<IO>,
    started_at: Instant,
    pub metrics: NodeStateMetrics,
}
impl<IO: Io> NodeState<IO> {
    pub fn load(
        node_id: NodeId,
        config: ClusterConfig,
        io: IO,
        metrics: NodeStateMetrics
    ) -> Self {
        let mut common = Common::new(node_id, io, config, metrics.clone());
        let role = RoleState::Loader(Loader::new(&mut common));
        let started_at = Instant::now();
        NodeState {
            common,
            role,
            started_at,
            metrics,
        }
    }
    pub fn is_loading(&self) -> bool {
        self.role.is_loader()
    }
    pub fn start_election(&mut self) {
        if let RoleState::Follower(_) = self.role {
            let next = self.common.transit_to_candidate();
            self.handle_role_change(next);
        }
    }
    fn handle_timeout(&mut self) -> Result<Option<RoleState<IO>>> {
        match self.role {
            RoleState::Loader(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Follower(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Candidate(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Leader(ref mut t) => track!(t.handle_timeout(&mut self.common)),
        }
    }
    fn handle_message(
        &mut self,
        message: Message,
    ) -> Result<Option<RoleState<IO>>> {
        if let RoleState::Loader(_) = self.role {
            // ロード中に届いたメッセージは全て破棄
            return Ok(None);
        }
        match self.common.handle_message(message) {
            HandleMessageResult::Handled(next) => Ok(next),
            HandleMessageResult::Unhandled(message) => match self.role {
                RoleState::Loader(_) => unreachable!(),
                RoleState::Follower(ref mut t) => {
                    track!(t.handle_message(&mut self.common, message))
                }
                RoleState::Candidate(ref mut t) => {
                    track!(t.handle_message(&mut self.common, &message))
                }
                RoleState::Leader(ref mut t) => {
                    track!(t.handle_message(&mut self.common, message))
                }
            },
        }
    }
    fn handle_role_change(&mut self, next: RoleState<IO>) {
        // For now, we don't require the metrics of other state transitions.
        match (&self.role, &next) {
            (RoleState::Candidate(_), RoleState::Leader(_)) => {
                let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                self.metrics
                    .candidate_to_leader_duration_seconds
                    .observe(elapsed);
                self.started_at = Instant::now();
            }
            (RoleState::Candidate(_), RoleState::Follower(_)) => {
                let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                self.metrics
                    .candidate_to_follower_duration_seconds
                    .observe(elapsed);
                self.started_at = Instant::now();
            }
            (RoleState::Loader(_), RoleState::Candidate(_)) => {
                let elapsed = prometrics::timestamp::duration_to_seconds(self.started_at.elapsed());
                self.metrics
                    .loader_to_candidate_duration_seconds
                    .observe(elapsed);
                self.started_at = Instant::now();
            }
            (RoleState::Leader(_), RoleState::Leader(_))
            | (RoleState::Follower(_), RoleState::Follower(_))
            | (RoleState::Candidate(_), RoleState::Candidate(_))
            | (RoleState::Loader(_), RoleState::Loader(_)) => {}
            _ => self.started_at = Instant::now(),
        }
        self.role = next;
    }
}
impl<IO: Io> Stream for NodeState<IO> {
    type Item = Result<Event>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut did_something = true;
        while did_something {
            did_something = false;
            // イベントチェック
            if let Some(e) = this.common.next_event() {
                return Poll::Ready(Some(Ok(e)));
            }

            // タイムアウト処理
            if let Poll::Ready(result) = track!((&mut this.common).poll_timeout(cx)) {
                let _ = track!(result)?;
                did_something = true;
                this.metrics.poll_timeout_total.increment();
                if let Some(next) = track!(this.handle_timeout())? {
                    this.handle_role_change(next);
                }
                if let Some(e) = this.common.next_event() {
                    return Poll::Ready(Some(Ok(e)));
                }
            }

            // 共通タスク
            if let Some(next) = track!(this.common.run_once(cx))? {
                did_something = true;
                this.handle_role_change(next);
            }
            if let Some(e) = this.common.next_event() {
                return Poll::Ready(Some(Ok(e)));
            }

            // 各状態固有のタスク
            let result = match this.role {
                RoleState::Loader(ref mut t) => track!(t.run_once(&mut this.common, cx))?,
                RoleState::Follower(ref mut t) => track!(t.run_once(&mut this.common, cx))?,
                RoleState::Candidate(ref mut t) => track!(t.run_once(&mut this.common, cx))?,
                RoleState::Leader(ref mut t) => track!(t.run_once(&mut this.common, cx))?,
            };
            if let Some(next) = result {
                did_something = true;
                this.handle_role_change(next);
            }
            if let Some(e) = this.common.next_event() {
                return Poll::Ready(Some(Ok(e)));
            }

            // 受信メッセージ処理
            if let Some(message) = track!(this.common.try_recv_message(cx))? {
                did_something = true;
                if let Some(next) = track!(this.handle_message(message))? {
                    this.handle_role_change(next);
                }
                if let Some(e) = this.common.next_event() {
                    return Poll::Ready(Some(Ok(e)));
                }
            }
        }
        Poll::Pending
    }
}

/// 各役割固有の状態.
pub enum RoleState<IO: Io> {
    /// ノード起動時にストレージから前回の状況を復元するための状態
    Loader(Loader<IO>),

    /// フォロワー (詳細はRaftの論文を参照)
    Follower(Follower<IO>),

    /// 立候補者 (詳細はRaftの論文を参照)
    Candidate(Candidate<IO>),

    /// リーダ (詳細はRaftの論文を参照)
    Leader(Leader<IO>),
}

impl<IO: Io> RoleState<IO> {
    /// Returns true if this role state is `Loader`.
    pub fn is_loader(&self) -> bool {
        matches!(self, RoleState::Loader(_))
    }

    /// Returns true if this role state is `Candidate`.
    #[cfg(test)]
    pub fn is_candidate(&self) -> bool {
        matches!(self, RoleState::Candidate(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker_ref;
    use prometrics::metrics::MetricBuilder;

    use crate::test_util::tests::TestIoBuilder;

    #[tokio::test(flavor = "multi_thread")]
    async fn node_state_is_loading_works() {
        let metrics = NodeStateMetrics::new(&MetricBuilder::new()).expect("Never fails");
        let io = TestIoBuilder::new().finish();
        let cluster = io.cluster.clone();
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let node = NodeState::load("test".into(), cluster, io, metrics);
        assert!(node.is_loading());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn role_state_is_loader_works() {
        let metrics = NodeStateMetrics::new(&MetricBuilder::new()).expect("Never fails");
        let io = TestIoBuilder::new().finish();
        let cluster = io.cluster.clone();
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let mut common = Common::new("test".into(), io, cluster, metrics);
        let state = RoleState::Loader(Loader::new(&mut common, &mut cx));
        assert!(state.is_loader());
        assert!(!state.is_candidate());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn role_state_is_candidate_works() {
        let metrics = NodeStateMetrics::new(&MetricBuilder::new()).expect("Never fails");
        let io = TestIoBuilder::new().finish();
        let cluster = io.cluster.clone();
        let waker = noop_waker_ref();
        let mut cx = Context::from_waker(waker);
        let mut common = Common::new("test".into(), io, cluster, metrics);
        let state = RoleState::Candidate(Candidate::new(&mut common, &mut cx));
        assert!(!state.is_loader());
        assert!(state.is_candidate());
    }
}
