use self::append::FollowerAppend;
use self::idle::FollowerIdle;
use self::init::FollowerInit;
use self::snapshot::FollowerSnapshot;
use super::{Common, NextState};
use crate::election::Role;
use crate::message::{Message, MessageHeader};
use crate::{Io, Result};

mod append;
mod idle;
mod init;
mod snapshot;

/// 別の人(ノード)に投票しているフォロワー.
///
/// リーダーから送られてきたメッセージを処理して、ログの同期を行う.
///
/// タイムアウト時間内にリーダからメッセージを受信しなかった場合には、
/// その選挙期間は完了したものと判断して、自身が立候補して次の選挙を始める.
pub enum Follower<IO: Io> {
    /// 初期化状態 (主に投票状況の保存を行う).
    Init(FollowerInit<IO>),

    /// リーダからのメッセージ処理が可能な状態.
    Idle(FollowerIdle<IO>),

    /// ローカルログへの追記中.
    Append(FollowerAppend<IO>),

    /// ローカルログへのスナップショット保存中.
    Snapshot(FollowerSnapshot<IO>),
}
impl<IO: Io> Follower<IO> {
    pub fn new(common: &mut Common<IO>, pending_vote: Option<MessageHeader>) -> Self {
        common.set_timeout(Role::Follower);
        let follower = FollowerInit::new(common, pending_vote);
        Follower::Init(follower)
    }
    pub fn handle_timeout(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        Ok(Some(common.transit_to_candidate()))
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        if let Message::AppendEntriesCall { .. } = message {
            common.set_timeout(Role::Follower);
            if unsafe { common.io_mut().is_busy() } {
                common.rpc_callee(message.header()).reply_busy();
                return Ok(None);
            }
        }

        match *self {
            Follower::Init(ref mut t) => track!(t.handle_message(common, message)),
            Follower::Idle(ref mut t) => track!(t.handle_message(common, message)),
            Follower::Append(ref mut t) => track!(t.handle_message(common, message)),
            Follower::Snapshot(ref mut t) => track!(t.handle_message(common, message)),
        }
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        match *self {
            Follower::Init(ref mut t) => track!(t.run_once(common)),
            Follower::Idle(_) => Ok(None),
            Follower::Append(ref mut t) => track!(t.run_once(common)),
            Follower::Snapshot(ref mut t) => track!(t.run_once(common)),
        }
    }
}
