//! RPC用のメッセージ群.
//!
//! なおRaftの論文に倣って"RPC"という呼称を採用しているが、
//! 実際にここで想定されている通信モデルは、RPCではなく
//! 非同期のメッセージ送受信モデル、となっている.
use crate::election::Term;
use crate::log::{LogIndex, LogPosition, LogPrefix, LogSuffix};
use crate::node::NodeId;

/// RPC用のメッセージ全般.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum Message {
    RequestVoteCall(RequestVoteCall),
    RequestVoteReply(RequestVoteReply),
    AppendEntriesCall(AppendEntriesCall),
    AppendEntriesReply(AppendEntriesReply),
    InstallSnapshotCast(InstallSnapshotCast),
}
impl Message {
    /// メッセージのヘッダを返す.
    pub fn header(&self) -> &MessageHeader {
        match self {
            Message::RequestVoteCall(m) => &m.header,
            Message::RequestVoteReply(m) => &m.header,
            Message::AppendEntriesCall(m) => &m.header,
            Message::AppendEntriesReply(m) => &m.header,
            Message::InstallSnapshotCast(m) => &m.header,
        }
    }

    pub(crate) fn set_destination(&mut self, dst: &NodeId) {
        match self {
            Message::RequestVoteCall(m) => {
                m.header.destination = dst.clone();
            }
            Message::RequestVoteReply(m) => {
                m.header.destination = dst.clone();
            }
            Message::AppendEntriesCall(m) => {
                m.header.destination = dst.clone();
            }
            Message::AppendEntriesReply(m) => {
                m.header.destination = dst.clone();
            }
            Message::InstallSnapshotCast(m) => {
                m.header.destination = dst.clone();
            }
        }
    }
}
impl From<RequestVoteCall> for Message {
    fn from(f: RequestVoteCall) -> Self {
        Message::RequestVoteCall(f)
    }
}
impl From<RequestVoteReply> for Message {
    fn from(f: RequestVoteReply) -> Self {
        Message::RequestVoteReply(f)
    }
}
impl From<AppendEntriesCall> for Message {
    fn from(f: AppendEntriesCall) -> Self {
        Message::AppendEntriesCall(f)
    }
}
impl From<AppendEntriesReply> for Message {
    fn from(f: AppendEntriesReply) -> Self {
        Message::AppendEntriesReply(f)
    }
}
impl From<InstallSnapshotCast> for Message {
    fn from(f: InstallSnapshotCast) -> Self {
        Message::InstallSnapshotCast(f)
    }
}

/// メッセージのヘッダ.
#[derive(Debug, Clone)]
pub struct MessageHeader {
    /// メッセージの送信元.
    pub sender: NodeId,

    // FIXME: ヘッダには含めないようにする
    /// メッセージの宛先
    pub destination: NodeId,

    /// シーケンス番号.
    pub seq_no: SequenceNumber,

    /// 送信者の現在の`Term`.
    pub term: Term,
}

/// `RequestVoteRPC`の要求メッセージ.
#[derive(Debug, Clone)]
pub struct RequestVoteCall {
    /// メッセージヘッダ.
    pub header: MessageHeader,

    /// 送信者のログの終端位置.
    pub log_tail: LogPosition,
}

/// `RequestVoteRPC`の応答メッセージ.
#[derive(Debug, Clone)]
pub struct RequestVoteReply {
    /// メッセージヘッダ.
    pub header: MessageHeader,

    /// 投票を行ったかどうか.
    pub voted: bool,
}

/// `AppendEntriesRPC`の要求メッセージ.
#[derive(Debug, Clone)]
pub struct AppendEntriesCall {
    /// メッセージヘッダ.
    pub header: MessageHeader,

    /// コミット済みログの終端インデックス.
    pub committed_log_tail: LogIndex,

    /// 追記対象となるログの末尾部分.
    pub suffix: LogSuffix,
}

/// `AppendEntriesRPC`の応答メッセージ.
#[derive(Debug, Clone)]
pub struct AppendEntriesReply {
    /// メッセージヘッダ.
    pub header: MessageHeader,

    /// 応答者(follower)のログの終端位置.
    ///
    /// これは「実際のログの終端」というよりは、
    /// 「リーダに次に送って貰いたい末尾部分の開始位置」的な意味合いを有する.
    ///
    /// それを考慮すると`next_head`といった名前の方が適切かもしれない.
    pub log_tail: LogPosition,

    /// 応答者が忙しいかどうか.
    ///
    /// この値が`true`の場合には、
    /// followerの`log_tail`が遅れていたとしても、
    /// リーダはログの同期のための追加のメッセージ送信を行わない.
    pub busy: bool,
}

/// `InstallSnapshotRPC`用のメッセージ.
///
/// 論文中では、これも他のRPC同様に"要求・応答"形式となっているが、
/// 他のRPCとは異なり、これに関しては本質的には応答は不要なので、
/// ここでは一方的な送信のみをサポートしている.
#[derive(Debug, Clone)]
pub struct InstallSnapshotCast {
    /// メッセージヘッダ.
    pub header: MessageHeader,

    /// 保存対象となるログの前半部分(i.e., スナップショット).
    pub prefix: LogPrefix,
}

/// メッセージのシーケンス番号.
///
/// この番号はノード毎に管理され、要求系のメッセージ送信の度にインクリメントされる.
/// 応答系のメッセージでは、対応する要求メッセージのシーケンス番号が使用される.
///
/// シーケンス番号は、一つの`Term`内では単調増加することが保証されている.
/// 逆に言えば、複数の`Term`を跨いだ場合には、シーケンス番号が増加する保証は無い.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SequenceNumber(u64);
impl SequenceNumber {
    /// 新しい`SequenceNumber`インスタンスを生成する.
    pub fn new(num: u64) -> Self {
        SequenceNumber(num)
    }

    /// シーケンス番号の値を返す.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
