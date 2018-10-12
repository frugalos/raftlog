//! シミュレータ用のI/O実装.
pub use self::deterministic::{DeterministicIo, DeterministicIoBuilder};
pub use self::storage::Storage;
pub use self::timer::Timer;
pub use self::transport::MessageBroker;

pub mod configs;
pub mod futures;

mod deterministic;
mod storage;
mod timer;
mod transport;

/// I/O関連の構成設定.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct IoConfig {
    #[serde(default)]
    pub channel: configs::ChannelConfig,

    #[serde(default)]
    pub storage: configs::StorageConfig,

    #[serde(default)]
    pub timer: configs::TimerConfig,
}
