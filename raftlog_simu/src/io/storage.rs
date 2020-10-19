/// シミュレータ用のストレージ実装.
///
/// 全てのデータはメモリ上で保持する.
use raftlog::election::Ballot;
use raftlog::log::{Log, LogIndex, LogPosition, LogPrefix, LogSuffix};
use raftlog::node::NodeId;
use trackable::error::ErrorKindExt;

use crate::io::configs::StorageConfig;
use crate::io::futures::{DelayedResult, LoadBallot, LoadLog, SaveBallot, SaveLog};
use crate::types::SharedRng;
use crate::{ErrorKind, Result};

/// シミュレータ用のストレージ実装.
///
/// 全てのデータはメモリ上に保持される.
#[derive(Clone)]
pub struct Storage {
    config: StorageConfig,
    rng: SharedRng,
    ballot: Ballot,
    log_prefix: Option<LogPrefix>,
    log_suffix: LogSuffix,
}
impl Storage {
    /// 新しい`StorageConfig`インスタンスを生成する.
    pub fn new(config: StorageConfig, rng: SharedRng, node: NodeId) -> Self {
        Storage {
            config,
            rng,
            ballot: Ballot {
                term: 0.into(),
                voted_for: node,
            },
            log_prefix: None,
            log_suffix: LogSuffix::default(),
        }
    }

    /// 投票状況を保存する.
    pub fn save_ballot(&mut self, ballot: Ballot) -> SaveBallot {
        self.ballot = ballot;
        let time = self.config.save_ballot_time.choose(&mut self.rng);
        DelayedResult::ok((), time)
    }

    /// 投票状況を取得する
    pub fn load_ballot(&mut self) -> LoadBallot {
        let time = self.config.load_ballot_time.choose(&mut self.rng);
        DelayedResult::ok(Some(self.ballot.clone()), time)
    }

    /// ログの先頭部分(i.e., スナップショット)を保存する.
    pub fn save_log_prefix(&mut self, prefix: LogPrefix) -> SaveLog {
        if self.log_suffix.head.index < prefix.tail.index {
            if self.log_suffix.skip_to(prefix.tail.index).is_err() {
                // `prefix`がローカルログを完全に追い越している
                self.log_suffix.head = prefix.tail;
                self.log_suffix.entries.clear();
            }
            assert_eq!(prefix.tail.index, self.log_suffix.head.index);

            if prefix.tail.prev_term != self.log_suffix.head.prev_term {
                self.log_suffix.head.prev_term = prefix.tail.prev_term;
                self.log_suffix.entries.clear();
            }
        }
        self.log_prefix = Some(prefix);
        let time = self.config.save_log_snapshot_time.choose(&mut self.rng);
        DelayedResult::ok((), time)
    }

    /// ログの末尾部分を保存(追記)する.
    pub fn save_log_suffix(&mut self, suffix: &LogSuffix) -> SaveLog {
        let result = self.log_append(suffix);
        let time =
            self.config.save_log_entry_time.choose(&mut self.rng) * suffix.entries.len() as u64;
        DelayedResult::done(result, time)
    }

    /// 指定された範囲のログを読み込む.
    pub fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> LoadLog {
        if start < self.log_suffix.head.index {
            // スナップショット済み領域
            let result = self
                .log_prefix
                .clone()
                .ok_or_else(|| ErrorKind::Other.cause("No snapshot installed").into())
                .map(Log::Prefix);
            let time = self.config.load_log_snapshot_time.choose(&mut self.rng);
            DelayedResult::done(result, time)
        } else {
            // 通常のログ領域
            let time = self.config.load_log_entry_time.choose(&mut self.rng);
            let result = if let Some(end) = end {
                self.load_log_range(start, end)
            } else {
                self.load_log_suffix(start)
            };
            result
                .map(|suffix| {
                    let time = time * suffix.entries.len() as u64;
                    DelayedResult::ok(Log::Suffix(suffix), time)
                }).unwrap_or_else(|e| DelayedResult::err(e, time))
        }
    }

    fn log_append(&mut self, suffix: &LogSuffix) -> Result<()> {
        // ローカルログと`suffix`の領域に重複部分があるかをチェック
        // (未コミット分がロールバックされる可能性もあるので、
        // 必ずしも`suffix`の先端が、ローカルログの末端と一致する必要はない)
        let entries_offset = if self.log_suffix.head.index <= suffix.head.index {
            0
        } else {
            // スナップショットのインストールタイミング次第で、こちらに入ることがある
            self.log_suffix.head.index - suffix.head.index
        };
        track_assert!(
            suffix.head.index <= self.log_suffix.tail().index,
            ErrorKind::InconsistentState,
            "suffix.start={:?}, self.end={:?}",
            suffix.head.index,
            self.log_suffix.tail().index
        );

        // 整合性(prev_termの一致)チェック
        let offset = suffix.head.index + entries_offset - self.log_suffix.head.index;
        let prev_term = if offset == 0 {
            self.log_suffix.head.prev_term
        } else {
            self.log_suffix.entries[offset - 1].term()
        };
        track_assert_eq!(
            suffix.positions().nth(entries_offset).map(|p| p.prev_term),
            Some(prev_term),
            ErrorKind::InconsistentState,
            "suffix.start={:?}, self.start={:?}",
            suffix.positions().nth(entries_offset),
            self.log_suffix.head
        );

        // 末尾の余剰領域を削除(ロールバック)した上で、追記する
        self.log_suffix.entries.truncate(offset);
        self.log_suffix
            .entries
            .extend(suffix.entries.iter().skip(entries_offset).cloned());
        Ok(())
    }
    fn load_log_suffix(&mut self, start: LogIndex) -> Result<LogSuffix> {
        let offset = start - self.log_suffix.head.index;
        let entries = (&self.log_suffix.entries[offset..]).into();
        let prev_term = if offset == 0 {
            self.log_suffix.head.prev_term
        } else {
            self.log_suffix.entries[offset - 1].term()
        };
        let index = start;
        let head = LogPosition { prev_term, index };
        Ok(LogSuffix { head, entries })
    }
    fn load_log_range(&mut self, start: LogIndex, end: LogIndex) -> Result<LogSuffix> {
        track_assert!(
            end <= self.log_suffix.tail().index,
            ErrorKind::InconsistentState
        );
        track_assert!(
            self.log_suffix.head.index <= start && self.log_suffix.head.index <= end,
            ErrorKind::Other,
            "self.log_suffix.head={:?}, start={:?}, end={:?}",
            self.log_suffix.head.index,
            start,
            end
        );
        let entries_start = start - self.log_suffix.head.index;
        let entries_end = end - self.log_suffix.head.index;
        let entries = (&self.log_suffix.entries[entries_start..entries_end]).into();
        let prev_term = if entries_start == 0 {
            self.log_suffix.head.prev_term
        } else {
            self.log_suffix.entries[entries_start - 1].term()
        };
        let index = start;
        let head = LogPosition { prev_term, index };
        Ok(LogSuffix { head, entries })
    }
}
