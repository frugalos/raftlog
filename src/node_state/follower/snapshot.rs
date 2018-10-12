use std::marker::PhantomData;

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle};
use message::Message;
use {Io, Result};

/// ローカルログへのスナップショット保存を処理するためのフォロワーのサブ状態.
///
/// `InstallSnapshotCast`で送られてきたスナップショットを処理する.
///
/// 正確には、スナップショットの処理自体は共通モジュールで行われるため、
/// ここでの目的は「スナップショットの保存中に、新たなログ追記等が行われないようにする」
/// ということになる.
///
/// なお、既にコミット済みの地点に対するスナップショットのインストール中に、
/// 新規ログ追加が行われても問題は発生しないので、
/// このサブ状態が使われるのは「未コミット地点に対するスナップショット」の
/// インストールをリーダから指示された場合、のみである.
pub struct FollowerSnapshot<IO: Io> {
    _phantom: PhantomData<IO>,
}
impl<IO: Io> FollowerSnapshot<IO> {
    pub fn new() -> Self {
        FollowerSnapshot {
            _phantom: PhantomData,
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
        if common.is_snapshot_installing() {
            Ok(None)
        } else {
            let next = Follower::Idle(FollowerIdle::new());
            Ok(Some(RoleState::Follower(next)))
        }
    }
}
