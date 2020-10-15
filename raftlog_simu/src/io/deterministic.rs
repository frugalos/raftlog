use raftlog::election::{Ballot, Role};
use raftlog::log::{LogIndex, LogPrefix, LogSuffix};
use raftlog::message::Message;
use raftlog::node::NodeId;
use raftlog::Io;

use crate::io::configs::{ChannelConfig, StorageConfig, TimerConfig};
use crate::io::futures;
use crate::io::{MessageBroker, Storage, Timer};
use crate::types::SharedRng;
use crate::Result;

/// `DeterministicIo`のビルダ.
pub struct DeterministicIoBuilder {
    channel: ChannelConfig,
    storage: StorageConfig,
    timer: TimerConfig,
    rng: SharedRng,
    broker: MessageBroker,
}
impl DeterministicIoBuilder {
    /// 新しい`DeterministicIoBuilder`インスタンスを生成する.
    pub fn new(rng: SharedRng) -> Self {
        let broker = MessageBroker::new(ChannelConfig::default(), rng.clone());
        DeterministicIoBuilder {
            channel: ChannelConfig::default(),
            storage: StorageConfig::default(),
            timer: TimerConfig::default(),
            rng,
            broker,
        }
    }

    /// チャンネル設定を更新する.
    ///
    /// 既に`build`メソッド経由で、`DeterministicIo`インスタンスを生成している場合は、
    /// それらと、このメソッド呼び出し以後に生成された`DeterministicIo`インスタンスの間で、
    /// メッセージ送受信を行うことはできないので注意が必要.
    pub fn channel(&mut self, config: ChannelConfig) -> &mut Self {
        self.broker = MessageBroker::new(config.clone(), self.rng.clone());
        self.channel = config;
        self
    }

    /// ストレージ設定を更新する.
    pub fn storage(&mut self, config: StorageConfig) -> &mut Self {
        self.storage = config;
        self
    }

    /// タイマー設定を更新する.
    pub fn timer(&mut self, config: TimerConfig) -> &mut Self {
        self.timer = config;
        self
    }

    /// 指定されたノード用の`DeterministicIo`インスタンスを生成する.
    pub fn build(&self, node: NodeId) -> DeterministicIo {
        let storage = Storage::new(self.storage.clone(), self.rng.clone(), node.clone());
        let timer = Timer::new(self.timer.clone(), self.rng.clone());
        DeterministicIo {
            node,
            broker: self.broker.clone(),
            storage,
            timer,
        }
    }
}

/// 決定論的な`Io`トレイト実装.
///
/// 構築時に指定した乱数生成期も含めて、全ての操作・入力列が等しいなら、
/// この`Io`実装は、常に同一の出力列を生成する.
#[derive(Clone)]
pub struct DeterministicIo {
    node: NodeId,
    broker: MessageBroker,
    storage: Storage,
    timer: Timer,
}
impl Io for DeterministicIo {
    type Timeout = futures::Timeout;
    type SaveBallot = futures::SaveBallot;
    type LoadBallot = futures::LoadBallot;
    type SaveLog = futures::SaveLog;
    type LoadLog = futures::LoadLog;
    fn create_timeout(&mut self, role: Role) -> Self::Timeout {
        self.timer.create_timeout(role)
    }
    fn try_recv_message(&mut self) -> Result<Option<Message>> {
        track!(self.broker.try_recv_message(&self.node))
    }
    fn send_message(&mut self, message: Message) {
        let destination = message.header().destination.clone();
        self.broker.send_message(&destination, message);
    }
    fn save_ballot(&mut self, ballot: Ballot) -> Self::SaveBallot {
        self.storage.save_ballot(ballot)
    }
    fn load_ballot(&mut self) -> Self::LoadBallot {
        self.storage.load_ballot()
    }
    fn save_log_prefix(&mut self, prefix: LogPrefix) -> Self::SaveLog {
        self.storage.save_log_prefix(prefix)
    }
    fn save_log_suffix(&mut self, suffix: &LogSuffix) -> Self::SaveLog {
        self.storage.save_log_suffix(suffix)
    }
    fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog {
        self.storage.load_log(start, end)
    }
}
