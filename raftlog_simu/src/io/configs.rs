//! I/O関連の構成設定を集めたモジュール.
use crate::types::{LogicalDuration, Probability, Range};

/// `Timer`用の構成設定.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimerConfig {
    /// 一つの選挙期間のタイムアウト尺.
    ///
    /// リーダからのハートビートを受信しない期間が、
    /// ここで指定された尺を超えた場合には、
    /// リーダがダウンしたものと判断されて、次の選挙が始まる.
    #[serde(default = "TimerConfig::default_election_timeout")]
    pub election_timeout: LogicalDuration,

    /// リーダがハートビートを発行する間隔.
    #[serde(default = "TimerConfig::default_heartbeat_interval")]
    pub heartbeat_interval: LogicalDuration,
}
impl TimerConfig {
    /// `election_timeout`フィールドのデフォルト値 (`1000`).
    pub fn default_election_timeout() -> LogicalDuration {
        1000
    }

    /// `heartbeat_interval`フィールドのデフォルト値 (`100`).
    pub fn default_heartbeat_interval() -> LogicalDuration {
        100
    }
}
impl Default for TimerConfig {
    fn default() -> Self {
        TimerConfig {
            election_timeout: TimerConfig::default_election_timeout(),
            heartbeat_interval: TimerConfig::default_heartbeat_interval(),
        }
    }
}

/// `Storage`用の構成設定.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// 投票状況の保存に要する時間.
    #[serde(default = "StorageConfig::default_save_ballot_time")]
    pub save_ballot_time: Range<LogicalDuration>,

    /// 投票状況の復元に要する時間.
    #[serde(default = "StorageConfig::default_load_ballot_time")]
    pub load_ballot_time: Range<LogicalDuration>,

    /// 個々のログエントリの保存に要する時間.
    ///
    /// 対象のエントリ数がNの場合には、時間はN倍になる.
    #[serde(default = "StorageConfig::default_save_log_entry_time")]
    pub save_log_entry_time: Range<LogicalDuration>,

    /// 個々のログエントリの読み込みに要する時間.
    ///
    /// 対象のエントリ数がNの場合には、時間はN倍になる.
    #[serde(default = "StorageConfig::default_load_log_entry_time")]
    pub load_log_entry_time: Range<LogicalDuration>,

    /// スナップショットの保存に要する時間.
    #[serde(default = "StorageConfig::default_save_log_snapshot_time")]
    pub save_log_snapshot_time: Range<LogicalDuration>,

    /// スナップショットの読み込みに要する時間.
    #[serde(default = "StorageConfig::default_load_log_snapshot_time")]
    pub load_log_snapshot_time: Range<LogicalDuration>,
}
impl StorageConfig {
    /// `1...5`
    pub fn default_save_ballot_time() -> Range<LogicalDuration> {
        Range { min: 1, max: 5 }
    }

    /// `1...5`
    pub fn default_load_ballot_time() -> Range<LogicalDuration> {
        Range { min: 1, max: 5 }
    }

    /// `1...5`
    pub fn default_save_log_entry_time() -> Range<LogicalDuration> {
        Range { min: 1, max: 5 }
    }

    /// `1...5`
    pub fn default_load_log_entry_time() -> Range<LogicalDuration> {
        Range { min: 1, max: 5 }
    }

    /// `100...500`
    pub fn default_save_log_snapshot_time() -> Range<LogicalDuration> {
        Range { min: 100, max: 500 }
    }

    /// `100...500`
    pub fn default_load_log_snapshot_time() -> Range<LogicalDuration> {
        Range { min: 100, max: 500 }
    }
}
impl Default for StorageConfig {
    fn default() -> Self {
        StorageConfig {
            save_ballot_time: StorageConfig::default_save_ballot_time(),
            load_ballot_time: StorageConfig::default_load_ballot_time(),
            save_log_entry_time: StorageConfig::default_save_log_entry_time(),
            load_log_entry_time: StorageConfig::default_load_log_entry_time(),
            save_log_snapshot_time: StorageConfig::default_save_log_snapshot_time(),
            load_log_snapshot_time: StorageConfig::default_load_log_snapshot_time(),
        }
    }
}

/// 通信チャンネルの構成設定.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    /// メッセージの消失率.
    ///
    /// `1.0`なら全てのメッセージが相手に届くことなく消失する.
    #[serde(default = "ChannelConfig::default_drop")]
    pub drop: Probability,

    /// メッセージの重複率.
    ///
    /// `1.0`なら(消失しなかった)全てのメッセージが複製される.
    #[serde(default = "ChannelConfig::default_duplicate")]
    pub duplicate: Probability,

    /// メッセージ遅延.
    #[serde(default = "ChannelConfig::default_delay")]
    pub delay: Range<LogicalDuration>,
}
impl ChannelConfig {
    /// `10..50`
    pub fn default_delay() -> Range<LogicalDuration> {
        Range { min: 10, max: 50 }
    }

    /// `0.05`
    pub fn default_drop() -> Probability {
        Probability { prob: 0.05 }
    }

    /// `0.01`
    pub fn default_duplicate() -> Probability {
        Probability { prob: 0.01 }
    }
}
impl Default for ChannelConfig {
    fn default() -> Self {
        ChannelConfig {
            delay: ChannelConfig::default_delay(),
            drop: ChannelConfig::default_drop(),
            duplicate: ChannelConfig::default_duplicate(),
        }
    }
}
