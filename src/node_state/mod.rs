use futures::{Async, Poll, Stream};

pub use self::common::Common;

use self::candidate::Candidate;
use self::common::HandleMessageResult;
use self::follower::Follower;
use self::leader::Leader;
use self::loader::Loader;
use cluster::ClusterConfig;
use message::Message;
use node::NodeId;
use {Error, Event, Io, Result};

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
}
impl<IO: Io> NodeState<IO> {
    pub fn load(node_id: NodeId, config: ClusterConfig, io: IO) -> Self {
        let mut common = Common::new(node_id, io, config);
        let role = RoleState::Loader(Loader::new(&mut common));
        NodeState { common, role }
    }
    pub fn is_loading(&self) -> bool {
        self.role.is_loader()
    }
    pub fn start_election(&mut self) {
        if let RoleState::Follower(_) = self.role {
            let next = self.common.transit_to_candidate();
            self.role = next;
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
    fn handle_message(&mut self, message: Message) -> Result<Option<RoleState<IO>>> {
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
                RoleState::Leader(ref mut t) => track!(t.handle_message(&mut self.common, message)),
            },
        }
    }
}
impl<IO: Io> Stream for NodeState<IO> {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut did_something = true;
        while did_something {
            did_something = false;

            // イベントチェック
            if let Some(e) = self.common.next_event() {
                return Ok(Async::Ready(Some(e)));
            }

            // タイムアウト処理
            if let Async::Ready(()) = track!(self.common.poll_timeout())? {
                did_something = true;
                if let Some(next) = track!(self.handle_timeout())? {
                    self.role = next;
                }
                if let Some(e) = self.common.next_event() {
                    return Ok(Async::Ready(Some(e)));
                }
            }

            // 共通タスク
            if let Some(next) = track!(self.common.run_once())? {
                did_something = true;
                self.role = next;
            }
            if let Some(e) = self.common.next_event() {
                return Ok(Async::Ready(Some(e)));
            }

            // 各状態固有のタスク
            let result = match self.role {
                RoleState::Loader(ref mut t) => track!(t.run_once(&mut self.common))?,
                RoleState::Follower(ref mut t) => track!(t.run_once(&mut self.common))?,
                RoleState::Candidate(ref mut t) => track!(t.run_once(&mut self.common))?,
                RoleState::Leader(ref mut t) => track!(t.run_once(&mut self.common))?,
            };
            if let Some(next) = result {
                did_something = true;
                self.role = next;
            }
            if let Some(e) = self.common.next_event() {
                return Ok(Async::Ready(Some(e)));
            }

            // 受信メッセージ処理
            if let Some(message) = track!(self.common.try_recv_message())? {
                did_something = true;
                if let Some(next) = track!(self.handle_message(message))? {
                    self.role = next;
                }
                if let Some(e) = self.common.next_event() {
                    return Ok(Async::Ready(Some(e)));
                }
            }
        }
        Ok(Async::NotReady)
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
        if let RoleState::Loader(_) = self {
            true
        } else {
            false
        }
    }

    /// Returns true if this role state is `Candidate`.
    #[cfg(test)]
    pub fn is_candidate(&self) -> bool {
        if let RoleState::Candidate(_) = self {
            true
        } else {
            false
        }
    }
}
