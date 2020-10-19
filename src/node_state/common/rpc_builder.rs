use super::Common;
use crate::log::{LogPosition, LogPrefix, LogSuffix};
use crate::message::{self, AppendEntriesReply, Message, MessageHeader, SequenceNumber};
use crate::node::NodeId;
use crate::Io;

/// RPC要求メッセージの送信を補助するためのビルダ.
pub struct RpcCaller<'a, IO: 'a + Io> {
    common: &'a mut Common<IO>,
}
impl<'a, IO: 'a + Io> RpcCaller<'a, IO> {
    pub fn new(common: &'a mut Common<IO>) -> Self {
        RpcCaller { common }
    }
    pub fn broadcast_request_vote(mut self) {
        let header = self.make_header(&NodeId::new(String::new())); // ブロードキャストノード時に空文字列を宛先に指定
        let log_tail = self.common.history.tail();
        let request = message::RequestVoteCall {
            header: header.clone(),
            log_tail,
        }
        .into();
        let self_reply = message::RequestVoteReply {
            header,
            voted: true,
        }
        .into();
        self.broadcast(request, self_reply);
    }
    pub fn broadcast_append_entries(mut self, suffix: LogSuffix) {
        let header = self.make_header(&NodeId::new(String::new())); // ブロードキャストノード時に空文字列を宛先に指定
        let request = message::AppendEntriesCall {
            header: header.clone(),
            committed_log_tail: self.common.history.committed_tail().index,
            suffix,
        }
        .into();
        let self_reply = AppendEntriesReply {
            header,
            log_tail: self.common.history.tail(),
            busy: false,
        }
        .into();
        self.broadcast(request, self_reply);
    }
    pub fn send_append_entries(mut self, peer: &NodeId, suffix: LogSuffix) {
        let message = message::AppendEntriesCall {
            header: self.make_header(peer),
            committed_log_tail: self.common.history.committed_tail().index,
            suffix,
        }
        .into();
        self.common.io.send_message(message);
    }
    pub fn send_install_snapshot(mut self, peer: &NodeId, prefix: LogPrefix) {
        let header = self.make_header(peer);
        let message = message::InstallSnapshotCast { header, prefix }.into();
        self.common.io.send_message(message);
    }

    fn make_header(&mut self, destination: &NodeId) -> MessageHeader {
        let seq_no = self.common.seq_no;
        self.common.seq_no = SequenceNumber::new(seq_no.as_u64() + 1);
        MessageHeader {
            sender: self.common.local_node.id.clone(),
            destination: destination.clone(),
            seq_no,
            term: self.common.local_node.ballot.term,
        }
    }
    fn broadcast(&mut self, mut message: Message, self_reply: Message) {
        let mut do_self_reply = false;
        for peer in self.common.history.config().members() {
            if *peer == self.common.local_node.id {
                do_self_reply = true;
            } else {
                message.set_destination(peer);
                self.common.io.send_message(message.clone());
            }
        }
        if do_self_reply {
            self.common.unread_message = Some(self_reply);
        }
    }
}

/// RPC応答メッセージの送信を補助するためのビルダ.
pub struct RpcCallee<'a, IO: 'a + Io> {
    common: &'a mut Common<IO>,
    caller: &'a MessageHeader,
}
impl<'a, IO: 'a + Io> RpcCallee<'a, IO> {
    pub fn new(common: &'a mut Common<IO>, caller: &'a MessageHeader) -> Self {
        RpcCallee { common, caller }
    }
    pub fn reply_request_vote(self, voted: bool) {
        let header = self.make_header();
        let message = message::RequestVoteReply { header, voted }.into();
        self.common.io.send_message(message);
    }
    pub fn reply_append_entries(self, log_tail: LogPosition) {
        let message = AppendEntriesReply {
            header: self.make_header(),
            log_tail,
            busy: false,
        }
        .into();
        self.common.io.send_message(message);
    }
    pub fn reply_busy(self) {
        let message = AppendEntriesReply {
            header: self.make_header(),
            log_tail: self.common.history.tail(),
            busy: true,
        }
        .into();
        self.common.io.send_message(message);
    }

    fn make_header(&self) -> MessageHeader {
        MessageHeader {
            sender: self.common.local_node.id.clone(),
            destination: self.caller.sender.clone(),
            seq_no: self.caller.seq_no,
            term: self.common.local_node.ballot.term,
        }
    }
}
