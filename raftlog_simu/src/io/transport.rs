use raftlog::message::Message;
use raftlog::node::NodeId;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::rc::Rc;

use crate::io::configs::ChannelConfig;
use crate::types::SharedRng;
use crate::Result;

/// シミュレータ用のRPCメッセージブローカー.
///
/// シミュレータにおいては、全てのノードは同じプロセス内に存在するので、
/// ネットワークを跨いだデータ転送は発生しない.
#[derive(Clone)]
pub struct MessageBroker {
    config: ChannelConfig,
    rng: SharedRng,
    channels: Rc<RefCell<HashMap<NodeId, Channel>>>,
}
impl MessageBroker {
    /// 新しい`MessageBroker`インスタンスを生成する.
    pub fn new(config: ChannelConfig, rng: SharedRng) -> Self {
        MessageBroker {
            config,
            rng,
            channels: Rc::new(RefCell::new(HashMap::new())),
        }
    }

    /// `local_node`に対して送信されたメッセージがあるなら取得する.
    pub fn try_recv_message(&mut self, local_node: &NodeId) -> Result<Option<Message>> {
        let message = self
            .channels
            .borrow_mut()
            .get_mut(&local_node)
            .and_then(|q| q.try_recv_message());
        Ok(message)
    }

    /// 指定の宛先に対して、メッセージを送信する.
    pub fn send_message(&mut self, destination: &NodeId, message: Message) {
        if self.config.drop.occurred(&mut self.rng) {
            return;
        }
        let delay = self.config.delay.choose(&mut self.rng);
        self.channels
            .borrow_mut()
            .entry(destination.to_owned())
            .or_insert_with(Channel::new)
            .send_message(message.clone(), delay);
        if self.config.duplicate.occurred(&mut self.rng) {
            self.send_message(destination, message);
        }
    }
}

struct Channel {
    clock: u64,
    queue: BinaryHeap<DelayedMessage>,
}
impl Channel {
    pub fn new() -> Self {
        Channel {
            clock: 0,
            queue: BinaryHeap::new(),
        }
    }
    pub fn try_recv_message(&mut self) -> Option<Message> {
        self.clock += 1;
        if let Some(m) = self.queue.pop() {
            if m.arrival_time <= self.clock {
                Some(m.message)
            } else {
                self.queue.push(m);
                None
            }
        } else {
            None
        }
    }
    pub fn send_message(&mut self, message: Message, delay: u64) {
        let message = DelayedMessage {
            message,
            arrival_time: self.clock + delay,
        };
        self.queue.push(message);
    }
}

struct DelayedMessage {
    arrival_time: u64,
    message: Message,
}
impl Ord for DelayedMessage {
    fn cmp(&self, other: &Self) -> Ordering {
        other.arrival_time.cmp(&self.arrival_time)
    }
}
impl PartialOrd for DelayedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.arrival_time.partial_cmp(&self.arrival_time)
    }
}
impl Eq for DelayedMessage {}
impl PartialEq for DelayedMessage {
    fn eq(&self, other: &Self) -> bool {
        self.arrival_time == other.arrival_time
    }
}
