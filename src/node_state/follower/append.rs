use futures::{Async, Future};

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle};
use log::LogPosition;
use message::{AppendEntriesCall, Message};
use {Io, Result};

/// ローカルログへの追記を行うフォロワーのサブ状態.
///
/// `AppendEntriesCall`で送られてきたエントリの追記を行う.
///
/// `AppendEntriesCall`が妥当な内容かどうかの判定や、
/// 細かい調整処理は`FollowerIdle`内で行われ、
/// ここが担当するのは、あくまでもログ追記処理のみ.
pub struct FollowerAppend<IO: Io> {
    future: Option<IO::SaveLog>,
    new_log_tail: LogPosition,
    message: AppendEntriesCall,
}
impl<IO: Io> FollowerAppend<IO> {
    pub fn new(common: &mut Common<IO>, mut message: AppendEntriesCall) -> Self {
        // メッセージ群の順序は逆転する可能性があるので、
        // それによってインデックスの巻き戻りが発生しないように調整.
        let mut new_log_tail = message.suffix.tail();
        if new_log_tail.index < common.log().tail().index {
            new_log_tail = common.log().tail();
        }
        if message.suffix.tail().index < message.committed_log_tail {
            message.committed_log_tail = message.suffix.tail().index;
        }
        if message.committed_log_tail < common.log_committed_tail().index {
            message.committed_log_tail = common.log_committed_tail().index;
        }

        let future = if new_log_tail.index == common.log().tail().index {
            // 新規追加分がない場合は、保存処理を省略して最適化
            // (AppendEntriesCallは、単にハートビートの用途でも使用されるので、空のケースは珍しくない)
            None
        } else {
            Some(common.save_log_suffix(&message.suffix))
        };
        FollowerAppend {
            future,
            new_log_tail,
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
            if self.new_log_tail == self.message.suffix.tail() {
                track!(common.handle_log_appended(&self.message.suffix))?;
            }
            track!(common.handle_log_committed(self.message.committed_log_tail))?;
            common
                .rpc_callee(&self.message.header)
                .reply_append_entries(self.message.suffix.tail());
            let next = Follower::Idle(FollowerIdle::new());
            Ok(Some(RoleState::Follower(next)))
        } else {
            Ok(None)
        }
    }
}
