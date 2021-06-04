use futures::{Async, Future};

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle};
use crate::log::LogPosition;
use crate::message::{AppendEntriesCall, Message};
use crate::{Io, Result};

/// ローカルログの削除を行うサブ状態
pub struct FollowerDelete<IO: Io> {
    future: IO::DeleteLog,
    from: LogPosition,
    message: AppendEntriesCall,    
}
impl<IO: Io> FollowerDelete<IO> {
    pub fn new(common: &mut Common<IO>, from: LogPosition, message: AppendEntriesCall) -> Self {
        let future = common.delete_suffix_from(from.index);
        FollowerDelete {
            future,
            from,
            message,
        }
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        if let Message::AppendEntriesCall(m) = message {
            common.rpc_callee(&m.header).reply_busy();
        }
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        if let Async::Ready(_) = track!(self.future.poll())? {
            track!(common.handle_log_rollbacked(self.from))?;
            common
                .rpc_callee(&self.message.header)
                .reply_append_entries(self.from);

            let next = Follower::Idle(FollowerIdle::new());
            Ok(Some(RoleState::Follower(next)))
        } else {
            Ok(None)
        }
    }
}
