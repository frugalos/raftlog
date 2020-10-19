use futures::Future;

use crate::election::{Ballot, Role};
use crate::log::{Log, LogIndex, LogPrefix, LogSuffix};
use crate::message::Message;
use crate::{Error, Result};

/// Raftの実行に必要なI/O機能を提供するためのトレイト.
///
/// 機能としてはおおまかに以下の三つに区分される:
///
/// - **ストレージ**
///   - ローカルノードの状態やログを保存するための永続ストレージ
///   - Raftが完全に正しく動作するためには、このストレージは完全に信頼できるものである必要がある
///      - 一度書き込まれたデータは(明示的に削除されない限り)失われたり、壊れたりすることは無い
///      - 実際には、それを達成するのは困難なので、信頼性とストレージコストのトレードオフとなる
/// - **チャンネル**
///   - ノード間通信(RPC)用のメッセージ送受信チャンネル
///   - このチャンネルの信頼性はある程度低くても良い
///      - メッセージ群の順番の入れ替わりや、欠損、重複配送、は許容される
///      - ただし、メッセージの改竄や捏造、はNG
/// - **タイマー**
///   - タイムアウト管理用のタイマー
pub trait Io {
    /// ローカルノードの投票状況を保存するための`Future`.
    type SaveBallot: Future<Item = (), Error = Error>;

    /// ノーカルノードの投票情報を取得ための`Future`.
    type LoadBallot: Future<Item = Option<Ballot>, Error = Error>;

    /// ローカルログを保存するための`Future`.
    type SaveLog: Future<Item = (), Error = Error>;

    /// ローカルログを取得するための`Future`.
    type LoadLog: Future<Item = Log, Error = Error>;

    /// タイムアウトを表現するための`Future`.
    type Timeout: Future<Item = (), Error = Error>;

    /// ローカルノードに対して送信されたメッセージの受信を試みる.
    ///
    /// # 注意
    ///
    /// このメソッドが`Err`を返した場合には、ローカルのRaftノードが
    /// 停止してしまうので、時間経過によって自動的には回復しない
    /// 致命的なものを除いては、`Err`は返さないことが望ましい.
    fn try_recv_message(&mut self) -> Result<Option<Message>>;

    /// メッセージを送信する.
    ///
    /// もしメッセージ送信に何らかの理由で失敗した場合でも、単に無視される.
    /// 仮にチャンネルの致命的な問題が発生している場合には、次の`try_recv_message`メソッドの
    /// 呼び出しで`Err`を返すこと.
    fn send_message(&mut self, message: Message);

    /// ローカルノードの投票状況を保存する.
    fn save_ballot(&mut self, ballot: Ballot) -> Self::SaveBallot;

    /// ローカルノードの前回の投票状況を取得する.
    fn load_ballot(&mut self) -> Self::LoadBallot;

    /// ローカルログの前半部分(i.e., スナップショット)を保存する.
    ///
    /// 保存に成功した場合は、それ以前のログ領域は破棄してしまって構わない.
    fn save_log_prefix(&mut self, prefix: LogPrefix) -> Self::SaveLog;

    /// ローカルログの末尾部分を保存(追記)する.
    ///
    /// `suffix`の開始位置が、現在のログの末尾よりも前方の場合は、
    /// 新しい開始位置よりも後ろの古いエントリは削除してしまって構わない.
    /// (リーダの入れ替えにより、ログの未コミット部分で競合が発生したことを示している)
    fn save_log_suffix(&mut self, suffix: &LogSuffix) -> Self::SaveLog;

    /// ローカルログの指定範囲のエントリを取得する.
    ///
    /// 範囲は`start`から始まり、`end`を含まない最後のエントリまでを取得する.
    /// `end`の値が`None`の場合には、ログの末端までを取得する.
    ///
    /// なお、`end`が指定されているにも関わらず、指定よりも少ないエントリしか
    /// 取得できなかった場合には「取得できたエントリのみを返す」ないし「エラーを返す」の
    /// どちらの挙動も許容される.
    ///
    /// ただし、`start`とは異なる位置から、エントリの取得を開始することは許可されない.
    fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog;

    /// 選挙における役割に応じた時間のタイムアウトオブジェクトを生成する.
    fn create_timeout(&mut self, role: Role) -> Self::Timeout;

    /// I/O処理を行う余裕があるかどうかを返す.
    ///
    /// これが`true`を返している間は、フォロワーの同期処理は実施されない.
    fn is_busy(&mut self) -> bool {
        false
    }
}
