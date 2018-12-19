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
/// 状態の定義は `RoleState` になり、`Loader`, `Follower`, `Candidate`, `Leader`の４つがある。
/// それぞれの詳細については `RoleState` の定義を参考にされたい。
pub struct NodeState<IO: Io> {
    pub common: Common<IO>,
    pub role: RoleState<IO>,
}
impl<IO: Io> NodeState<IO> {
    /// `node_id`を持つノードを作成し、Loaderとして`config`に参加させる。
    /// `io`はI/O処理の実装である。
    pub fn load(node_id: NodeId, config: ClusterConfig, io: IO) -> Self {
        let mut common = Common::new(node_id, io, config);
        let role = RoleState::Loader(Loader::new(&mut common));
        NodeState { common, role }
    }

    /// 現在の状態がLoaderかどうかを確認する。
    pub fn is_loading(&self) -> bool {
        if let RoleState::Loader(_) = self.role {
            true
        } else {
            false
        }
    }

    /// Follower -> Candidateへと遷移し、選挙を開始する。
    /// 本当に選挙を開始する？？ roleを変えるだけ？
    /// 現在の状態がFollowerでなければ何もしない。
    pub fn start_election(&mut self) {
        if let RoleState::Follower(_) = self.role {
            let next = self.common.transit_to_candidate();
            self.role = next;
        }
    }

    /// 各状態におけるタイムアウト処理を行う。
    /// - Loader: None（ということは留まる？）
    /// - Follower: Candidateに遷移する。
    /// - Candidate: Candidateに遷移する。
    /// - Leader: None(ということはLeaderに留まるのか？ 論文的にリーダーでタイムアウトするとはなに）
    fn handle_timeout(&mut self) -> Result<Option<RoleState<IO>>> {
        match self.role {
            RoleState::Loader(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Follower(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Candidate(ref mut t) => track!(t.handle_timeout(&mut self.common)),
            RoleState::Leader(ref mut t) => track!(t.handle_timeout(&mut self.common)),
        }
    }

    /// RPC用の命令 `message` を、各状態に基づき対応する。
    fn handle_message(&mut self, message: Message) -> Result<Option<RoleState<IO>>> {
        if let RoleState::Loader(_) = self.role {
            // ロード中に届いたメッセージは全て破棄
            // TODO: なぜ？？
            return Ok(None);
        }
        match self.common.handle_message(message) {
            // commonレイヤーで処理できるもの
            // （一覧 ...）
            // についてはここで処理を完了する。
            HandleMessageResult::Handled(next) => Ok(next),

            // commondレイヤーで処理できないもの
            // （一覧 ...）
            // については、現在の状態で適切に処理する。
            HandleMessageResult::Unhandled(message) => match self.role {
                RoleState::Loader(_) => unreachable!(),
                RoleState::Follower(ref mut t) => {
                    track!(t.handle_message(&mut self.common, message))
                }
                RoleState::Candidate(ref mut t) => {
                    track!(t.handle_message(&mut self.common, &message)) // これだけ &message で良いのか？
                }
                RoleState::Leader(ref mut t) => {
                    track!(t.handle_message(&mut self.common, message)),
                }
            },
        }
    }
}
impl<IO: Io> Stream for NodeState<IO> {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // did_somethingより、roleが変わったのでもう一周する
        // とかの方が良さそうではある。実際の処理ってそうなってるのか？
        let mut did_something = true;
        while did_something {
            did_something = false;

            // イベントチェック
            if let Some(e) = self.common.next_event() {
                return Ok(Async::Ready(Some(e)));
            }

            // タイムアウト処理
            if let Async::Ready(()) = track!(self.common.poll_timeout())? {
                // 既にタイムアウトしているならば以下の処理
                did_something = true;
                if let Some(next) = track!(self.handle_timeout())? {
                    self.role = next;
                }
                // この処理はここでやる必要があるのか？
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
    /// TODO: Initという名前にしなかった理由は？
    Loader(Loader<IO>),

    /// フォロワー (詳細はRaftの論文を参照)
    Follower(Follower<IO>),

    /// 立候補者 (詳細はRaftの論文を参照)
    Candidate(Candidate<IO>),

    /// リーダ (詳細はRaftの論文を参照)
    Leader(Leader<IO>),
}
