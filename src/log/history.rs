use std::collections::VecDeque;
use trackable::error::ErrorKindExt;

use crate::cluster::ClusterConfig;
use crate::log::{LogEntry, LogIndex, LogPosition, LogPrefix, LogSuffix};
use crate::{ErrorKind, Result};

/// ローカルログの歴史(要約)を保持するためのデータ構造.
///
/// スナップショット地点以降のローカルログに関して発生した、
/// 重要な出来事(i.g., `Term`の変更)が記録されている.
///
/// それ以外に「ログの末尾(log_tail)」および「ログのコミット済み末尾(log_committed_tail)」、
/// 「ログの消費済み末尾(log_consumed_tail)」の三つの地点を保持している.
///
/// それらの関しては`log_consumed_tail <= log_committed_tail <= log_tail`の不変項が維持される.
#[derive(Debug, Clone)]
pub struct LogHistory {
    appended_tail: LogPosition,
    committed_tail: LogPosition,
    consumed_tail: LogPosition,
    records: VecDeque<HistoryRecord>,
}
impl LogHistory {
    /// 初期クラスタ構成を与えて、新しい`LogHistory`インスタンスを生成する.
    pub fn new(config: ClusterConfig) -> Self {
        let initial = HistoryRecord::new(LogPosition::default(), config);
        LogHistory {
            appended_tail: LogPosition::default(),
            committed_tail: LogPosition::default(),
            consumed_tail: LogPosition::default(),
            records: vec![initial].into(),
        }
    }

    /// ローカルログの先端位置を返す.
    pub fn head(&self) -> LogPosition {
        self.records[0].head
    }

    /// ローカルログの終端位置を返す.
    pub fn tail(&self) -> LogPosition {
        self.appended_tail
    }

    /// ローカルログのコミット済みの終端位置を返す.
    ///
    /// 「コミット済みの終端」==「未コミットの始端」
    pub fn committed_tail(&self) -> LogPosition {
        self.committed_tail
    }

    /// ローカルログの適用済みの終端位置を返す.
    pub fn consumed_tail(&self) -> LogPosition {
        self.consumed_tail
    }

    /// ローカルログに記録された最新のクラスタ構成を返す.
    pub fn config(&self) -> &ClusterConfig {
        &self.last_record().config
    }

    /// 最後に追加された`HistoryRecord`を返す.
    pub fn last_record(&self) -> &HistoryRecord {
        self.records.back().expect("Never fails")
    }

    /// 指定されたインデックスが属するレコードを返す.
    ///
    /// 既に削除された領域が指定された場合には`None`が返される.
    pub fn get_record(&self, index: LogIndex) -> Option<&HistoryRecord> {
        for r in self.records.iter().rev() {
            if r.head.index <= index {
                return Some(&r);
            }
        }
        None
    }

    /// `suffix`がローカルログに追記されたことを記録する.
    pub fn record_appended(&mut self, suffix: &LogSuffix) -> Result<()> {
        let entries_offset = if self.appended_tail.index <= suffix.head.index {
            0
        } else {
            // NOTE:
            // 追記中にスナップショットがインストールされた場合に、
            // 両者の先頭位置がズレることがあるので調整する
            self.appended_tail.index - suffix.head.index
        };
        for (i, e) in suffix.entries.iter().enumerate().skip(entries_offset) {
            let tail = LogPosition {
                prev_term: e.term(),
                index: suffix.head.index + i + 1,
            };
            if let LogEntry::Config { ref config, .. } = *e {
                if self.last_record().config != *config {
                    // クラスタ構成が変更された
                    let record = HistoryRecord::new(tail, config.clone());
                    self.records.push_back(record);
                }
            }
            if tail.prev_term != self.last_record().head.prev_term {
                // 新しい選挙期間(`Term`)に移った
                track_assert!(
                    self.last_record().head.prev_term < tail.prev_term,
                    ErrorKind::Other,
                    "last_record.head={:?}, tail={:?}",
                    self.last_record().head,
                    tail
                );
                let record = HistoryRecord::new(tail, self.last_record().config.clone());
                self.records.push_back(record);
            }
        }
        self.appended_tail = suffix.tail();
        Ok(())
    }

    /// `new_tail_index`までコミット済み地点が進んだことを記録する.
    pub fn record_committed(&mut self, new_tail_index: LogIndex) -> Result<()> {
        track_assert!(
            self.committed_tail.index <= new_tail_index,
            ErrorKind::Other
        );
        track_assert!(
            new_tail_index <= self.appended_tail.index,
            ErrorKind::Other,
            "new_tail_index={:?}, self.appended_tail.index={:?}",
            new_tail_index,
            self.appended_tail.index
        );
        let prev_term = track!(self
            .get_record(new_tail_index,)
            .ok_or_else(|| ErrorKind::Other.error(),))?
        .head
        .prev_term;
        self.committed_tail = LogPosition {
            prev_term,
            index: new_tail_index,
        };
        Ok(())
    }

    /// `new_tail`までのログに含まれるコマンドが消費されたことを記録する.
    ///
    /// ここでの"消費"とは「状態機械に入力として渡されて実行された」ことを意味する.
    pub fn record_consumed(&mut self, new_tail_index: LogIndex) -> Result<()> {
        track_assert!(self.consumed_tail.index <= new_tail_index, ErrorKind::Other);
        track_assert!(
            new_tail_index <= self.committed_tail.index,
            ErrorKind::Other
        );

        let prev_term =
            track!(self.get_record(new_tail_index).ok_or_else(
                || ErrorKind::Other.cause(format!("Too old index: {:?}", new_tail_index))
            ))?
            .head
            .prev_term;
        self.consumed_tail = LogPosition {
            prev_term,
            index: new_tail_index,
        };
        Ok(())
    }

    /// 「追記済み and 未コミット」な末尾領域がロールバック(破棄)されたことを記録する.
    ///
    /// ログの新しい終端は`new_tail`となる.
    pub fn record_rollback(&mut self, new_tail: LogPosition) -> Result<()> {
        track_assert!(new_tail.index <= self.appended_tail.index, ErrorKind::Other);
        track_assert!(
            self.committed_tail.index <= new_tail.index,
            ErrorKind::Other,
            "old={:?}, new={:?}",
            self.committed_tail,
            new_tail
        );
        track_assert_eq!(
            self.get_record(new_tail.index).map(|r| r.head.prev_term),
            Some(new_tail.prev_term),
            ErrorKind::InconsistentState
        );
        self.appended_tail = new_tail;

        if let Some(new_len) = self
            .records
            .iter()
            .position(|r| r.head.index > new_tail.index)
        {
            self.records.truncate(new_len);
        }
        Ok(())
    }

    /// スナップショットがインストールされたことを記録する.
    ///
    /// `new_head`はスナップショットに含まれない最初のエントリのIDで、
    /// `config`はスナップショット取得時のクラスタ構成、を示す.
    ///
    /// `new_head`は、現在のログの末尾を超えていても良いが、
    /// 現在のログの先頭以前のものは許容されない.
    /// (スナップショット地点から現在までの歴史が消失してしまうため)
    ///
    /// なお、`head`以前の記録は歴史から削除される.
    pub fn record_snapshot_installed(
        &mut self,
        new_head: LogPosition,
        config: ClusterConfig,
    ) -> Result<()> {
        track_assert!(
            self.head().index <= new_head.index,
            ErrorKind::InconsistentState,
            "self.head={:?}, new_head={:?}",
            self.head(),
            new_head
        );

        // スナップショット地点までの歴史は捨てる
        while self
            .records
            .front()
            .map_or(false, |r| r.head.index <= new_head.index)
        {
            self.records.pop_front();
        }

        // 新しいログの先頭をセット
        let record = HistoryRecord::new(new_head, config);
        self.records.push_front(record);

        if self.appended_tail.index < new_head.index {
            self.appended_tail = new_head;
        }
        if self.committed_tail.index < new_head.index {
            self.committed_tail = new_head;
        }
        Ok(())
    }

    /// スナップショットが読み込まれたことを記録する.
    ///
    /// ローカルログ内のスナップショット地点までのエントリは、消費されたものとして扱われる.
    pub fn record_snapshot_loaded(&mut self, snapshot: &LogPrefix) -> Result<()> {
        if self.consumed_tail.index < snapshot.tail.index {
            track_assert!(
                snapshot.tail.index <= self.committed_tail.index,
                ErrorKind::InconsistentState,
                "snapshot.tail.index={:?}, self.committed_tail.index={:?}",
                snapshot.tail.index,
                self.committed_tail.index
            );
            self.consumed_tail = snapshot.tail;
        }
        Ok(())
    }
}

/// `LogHistory`に保持されるレコード.
#[derive(Debug, Clone)]
pub struct HistoryRecord {
    /// 記録地点.
    pub head: LogPosition,

    /// 記録時のクラスタ構成.
    pub config: ClusterConfig,
}
impl HistoryRecord {
    fn new(head: LogPosition, config: ClusterConfig) -> Self {
        HistoryRecord { head, config }
    }
}
