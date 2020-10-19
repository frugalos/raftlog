use trackable::error::TrackableError;
use trackable::error::{ErrorKind as TrackableErrorKind, ErrorKindExt};

/// クレート固有の`Error`型.
#[derive(Debug, Clone, TrackableError)]
pub struct Error(TrackableError<ErrorKind>);
impl From<std::io::Error> for Error {
    fn from(f: std::io::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}
impl From<prometrics::Error> for Error {
    fn from(f: prometrics::Error) -> Self {
        ErrorKind::Other.cause(f).into()
    }
}

/// 発生し得るエラーの種類.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// リーダのみが処理可能な操作が、リーダではないノードに対して行われた.
    ///
    /// このエラーを受け取った場合、利用者はリーダノードに対して、
    /// 同じ要求をリトライすべきである.
    NotLeader,

    /// リソースに空きが無くて、要求を受け付けることができない.
    ///
    /// このエラーを受け取った場合、利用者はある程度時間を空ける、ないし、
    /// 現在実行中の処理の完了を確認してから、同様の要求をリトライすべきである.
    ///
    /// 典型的には、あるスナップショットのインストール中に、
    /// 別のスナップショットのインストールが要求された場合に、
    /// このエラーが返される.
    Busy,

    /// 入力が不正.
    ///
    /// このエラーを受け取った場合、利用者は可能であれば、
    /// 入力値を適切なものに修正して、同様の操作をリトライすることが望ましい.
    InvalidInput,

    /// 不整合な状態に陥った.
    ///
    /// プログラムのバグやI/O周りの重大な問題(e.g., データ改善)により、
    /// 本来発生するはずのない状態が生じてしまった.
    ///
    /// このエラーを受け取った場合、利用者はそのノードの使用を停止して、
    /// どのような問題が発生しているかを詳細に調査すべきである.
    ///
    /// もし使用を継続した場合には、最悪のケースでは、コミット済みのログ領域が
    /// 別のエントリによって上書きされてしまうこともあり得る.
    InconsistentState,

    /// その他エラー.
    ///
    /// 主に`Io`トレイトの実装のために設けられたエラー区分.
    ///
    /// このエラーを受け取った場合、利用者はそのノードの使用を停止して、
    /// どのような問題が発生しているかを詳細に調査すべきである.
    Other,
}
impl TrackableErrorKind for ErrorKind {}
