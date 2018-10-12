use futures::Future;

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle};
use message::{Message, MessageHeader};
use {Io, Result};

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
    pub fn new(common: &mut Common<IO>) -> Self {
        let future = common.save_ballot();
        FollowerInit {
            future,
            pending_vote: None,
        }
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        match message {
            Message::RequestVoteCall(m) => {
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
            let next = FollowerIdle::new();
            Ok(Some(RoleState::Follower(Follower::Idle(next))))
        } else {
            Ok(None)
        }
    }
}
