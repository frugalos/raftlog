use futures::Future;

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle, FollowerSnapshot};
use crate::message::{Message, MessageHeader};
use crate::{Io, Result};

/// 初期化処理を行うフォロワーのサブ状態.
///
/// 以下を行う:
///
/// - 1. 投票状況を保存
/// - 2. もし保存処理中に投票先から`RequestVoteCall`を受信したら、保存後にそれに返答(投票)
pub struct FollowerInit<IO: Io> {
    future: IO::SaveBallot,
    pending_vote: Option<MessageHeader>,
}
impl<IO: Io> FollowerInit<IO> {
    pub fn new(common: &mut Common<IO>, pending_vote: Option<MessageHeader>) -> Self {
        let future = common.save_ballot();
        FollowerInit {
            future,
            pending_vote,
        }
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        match message {
            Message::RequestVoteCall(m) => {
                // pending_vote can be overwritten here because the latest vote should be used.
                self.pending_vote = Some(m.header);
            }
            Message::AppendEntriesCall(m) => {
                common.rpc_callee(&m.header).reply_busy();
            }
            _ => {}
        }
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        let item = track!(self.future.poll())?;
        if item.is_ready() {
            if let Some(header) = self.pending_vote.take() {
                common.rpc_callee(&header).reply_request_vote(true);
            }
            // We must complete the active snapshot before appending new log entries
            // to keep the consistency of the state of a node if the node is busy
            // installing snapshot.
            // See https://github.com/frugalos/raftlog/issues/15.
            let next = if common.is_focusing_on_installing_snapshot() {
                RoleState::Follower(Follower::Snapshot(FollowerSnapshot::new()))
            } else {
                RoleState::Follower(Follower::Idle(FollowerIdle::new()))
            };
            Ok(Some(next))
        } else {
            Ok(None)
        }
    }
}
