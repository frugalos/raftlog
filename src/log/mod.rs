//! ノードローカルなログ関連の構成要素群.
use std::ops::{Add, AddAssign, Sub, SubAssign};

pub use self::history::{HistoryRecord, LogHistory};

use crate::cluster::ClusterConfig;
use crate::election::Term;
use crate::{ErrorKind, Result};

mod history;

/// ローカルログ.
#[derive(Debug)]
pub enum Log {
    /// ログの前半部分 (i.e., スナップショット).
    Prefix(LogPrefix),

    /// ログの後半部分>
    Suffix(LogSuffix),
}
impl From<LogPrefix> for Log {
    fn from(f: LogPrefix) -> Self {
        Log::Prefix(f)
    }
}
impl From<LogSuffix> for Log {
    fn from(f: LogSuffix) -> Self {
        Log::Suffix(f)
    }
}

/// ログの前半部分 (i.e., スナップショット).
#[derive(Debug, Clone)]
pub struct LogPrefix {
    /// 前半部分の終端位置.
    ///
    /// "終端位置" = "前半部分に含まれない最初の位置".
    pub tail: LogPosition,

    /// 前半部分に含まれる中で、最新の構成情報.
    pub config: ClusterConfig,

    /// 前半部分に含まれるコマンド群の適用後の状態機械のスナップショット.
    pub snapshot: Vec<u8>,
}

/// ログの後半部分.
///
/// 厳密には、常に"後半部分"、つまり「ある地点より後ろの全てのエントリ」を
/// 含んでいる訳ではない.
///
/// ただし、このデータ構造自体は、常に追記的なアクセスのために利用され、
/// "ログの途中の一部だけを更新する"といった操作は発生しないので、
/// "常にログの末尾に対して適用される"的な意味合いで`Suffix`と付けている.
#[derive(Debug, Clone)]
pub struct LogSuffix {
    /// ログの開始位置.
    ///
    /// `entries`のサイズが1以上の場合には、
    /// その最初のエントリの位置となる.
    pub head: LogPosition,

    /// 後半部分に属するエントリ群.
    pub entries: Vec<LogEntry>,
}
impl LogSuffix {
    /// ログの終端位置を返す.
    ///
    /// "終端位置" = "entriesに含まれない最初のエントリの位置".
    ///
    /// `entries`の最後の要素が、ログ全体の最後の要素と一致している場合には、
    /// "終端位置"は「次にログに追加される位置(= ログの末端)」となる.
    pub fn tail(&self) -> LogPosition {
        let prev_term = self
            .entries
            .last()
            .map_or(self.head.prev_term, LogEntry::term);
        let index = self.head.index + self.entries.len();
        LogPosition { prev_term, index }
    }

    /// 後半部分に含まれるエントリの位置を走査するためのイテレータを返す.
    pub fn positions(&self) -> LogPositions {
        LogPositions {
            suffix: self,
            offset: 0,
        }
    }

    /// `new_head`のまでスキップする.
    ///
    /// 現在の先頭から`new_head`までのエントリは破棄され、`new_head`が新しい先頭になる.
    ///
    /// # Errors
    ///
    /// 以下のいずれかの場合には`ErrorKind::InvalidInput`が返される:
    ///
    /// - `new_head < self.head.index`
    /// - `self.tail().index < new_head`
    pub fn skip_to(&mut self, new_head: LogIndex) -> Result<()> {
        track_assert!(self.head.index <= new_head, ErrorKind::InvalidInput);
        track_assert!(new_head <= self.tail().index, ErrorKind::InvalidInput);
        let count = new_head - self.head.index;
        if count == 0 {
            return Ok(());
        }
        let prev_term = self
            .entries
            .drain(0..count)
            .last()
            .expect("Never fails")
            .term();
        self.head.prev_term = prev_term;
        self.head.index += count;
        Ok(())
    }

    /// 終端を`new_tail`の位置まで切り詰める.
    ///
    /// # Errors
    ///
    /// `new_tail`が`LogSuffix`が保持する範囲の外の場合には、
    /// `ErrorKind::InvalidInput`を理由としたエラーが返される.
    pub fn truncate(&mut self, new_tail: LogIndex) -> Result<()> {
        track_assert!(self.head.index <= new_tail, ErrorKind::InvalidInput);
        track_assert!(new_tail <= self.tail().index, ErrorKind::InvalidInput);
        let delta = self.tail().index - new_tail;
        let new_len = self.entries.len() - delta;
        self.entries.truncate(new_len);
        Ok(())
    }

    /// 指定された範囲のログ領域を切り出して返す.
    ///
    /// # Errors
    ///
    /// `self`が指定範囲を包含していない場合には、
    /// `ErrorKind::InvalidInput`を理由としてエラーが返される.
    pub fn slice(&self, start: LogIndex, end: LogIndex) -> Result<Self> {
        track_assert!(self.head.index <= start, ErrorKind::InvalidInput);
        track_assert!(start <= end, ErrorKind::InvalidInput);
        track_assert!(end <= self.tail().index, ErrorKind::InvalidInput);
        let slice_start = start - self.head.index;
        let slice_end = end - self.head.index;
        let slice_head = if start == self.head.index {
            self.head
        } else {
            let prev_term = self.entries[slice_start - 1].term();
            LogPosition {
                prev_term,
                index: start,
            }
        };
        let slice_entries = self.entries[slice_start..slice_end].into();
        Ok(LogSuffix {
            head: slice_head,
            entries: slice_entries,
        })
    }
}
impl Default for LogSuffix {
    fn default() -> Self {
        LogSuffix {
            head: LogPosition::default(),
            entries: Vec::new(),
        }
    }
}

/// `LogSuffix`に含まれるログの位置を走査するための`Iterator`実装.
#[derive(Debug)]
pub struct LogPositions<'a> {
    suffix: &'a LogSuffix,
    offset: usize,
}
impl<'a> Iterator for LogPositions<'a> {
    type Item = LogPosition;
    fn next(&mut self) -> Option<Self::Item> {
        if self.suffix.entries.len() < self.offset {
            None
        } else {
            let id = if self.offset == 0 {
                self.suffix.head
            } else {
                let i = self.offset - 1;
                let prev_term = self.suffix.entries[i].term();
                let index = self.suffix.head.index + self.offset;
                LogPosition { prev_term, index }
            };
            self.offset += 1;
            Some(id)
        }
    }
}

/// ログに格納されるエントリ.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum LogEntry {
    /// 特に内容を持たないエントリ.
    ///
    /// リーダ選出時には、最初にこのエントリがログに追加され、
    /// `Term`が変わったことを記録する.
    Noop { term: Term },

    /// クラスタ構成の変更を共有するためのエントリ.
    Config { term: Term, config: ClusterConfig },

    /// 状態機械の入力となるコマンドを格納したエントリ.
    Command { term: Term, command: Vec<u8> },
}
impl LogEntry {
    /// このエントリが発行された`Term`を返す.
    pub fn term(&self) -> Term {
        match *self {
            LogEntry::Noop { term } => term,
            LogEntry::Config { term, .. } => term,
            LogEntry::Command { term, .. } => term,
        }
    }
}

/// 提案ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProposalId {
    /// 提案が発行された時の`Term`.
    pub term: Term,

    /// 提案を保持するエントリのログ内でのインデックス.
    pub index: LogIndex,
}

/// ログの特定位置を識別するためのデータ構造.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LogPosition {
    /// 一つ前のインデックスのエントリの`Term`.
    pub prev_term: Term,

    /// この位置のインデックス.
    pub index: LogIndex,
}
impl LogPosition {
    /// `self`がログ上で、`other`と等しい、あるいは、より後方に位置している場合に`true`が返る.
    ///
    /// なお`self`と`other`が、それぞれ分岐したログ上に位置しており、
    /// 前後関係が判断できない場合には`false`が返される.
    ///
    /// # Examples
    ///
    /// ```
    /// use raftlog::log::LogPosition;
    ///
    /// // `a`の方がインデックスが大きい
    /// let a = LogPosition { prev_term: 10.into(), index: 5.into() };
    /// let b = LogPosition { prev_term: 10.into(), index: 3.into() };
    /// assert!(a.is_newer_or_equal_than(b));
    /// assert!(!b.is_newer_or_equal_than(a));
    ///
    /// // `a`の方が`Term`が大きい
    /// let a = LogPosition { prev_term: 20.into(), index: 3.into() };
    /// let b = LogPosition { prev_term: 10.into(), index: 3.into() };
    /// assert!(a.is_newer_or_equal_than(b));
    /// assert!(!b.is_newer_or_equal_than(a));
    ///
    /// // `a`の方がインデックスは大きいが、`b`の方が`Term`は大きい
    /// // => 順序が確定できない
    /// let a = LogPosition { prev_term: 5.into(), index: 10.into() };
    /// let b = LogPosition { prev_term: 10.into(), index: 3.into() };
    /// assert!(!a.is_newer_or_equal_than(b));
    /// assert!(!b.is_newer_or_equal_than(a));
    /// ```
    pub fn is_newer_or_equal_than(&self, other: LogPosition) -> bool {
        self.prev_term >= other.prev_term && self.index >= other.index
    }
}

/// あるログエントリのインデックス.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LogIndex(u64);
impl LogIndex {
    /// 新しい`LogIndex`インスタンスを生成する.
    pub fn new(index: u64) -> Self {
        LogIndex(index)
    }

    /// インデックスの値を返す.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl From<u64> for LogIndex {
    fn from(f: u64) -> Self {
        LogIndex::new(f)
    }
}
impl Add<usize> for LogIndex {
    type Output = Self;
    fn add(self, rhs: usize) -> Self::Output {
        LogIndex(self.0 + rhs as u64)
    }
}
impl AddAssign<usize> for LogIndex {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as u64;
    }
}
impl Sub for LogIndex {
    type Output = usize;
    fn sub(self, rhs: Self) -> Self::Output {
        (self.0 - rhs.0) as usize
    }
}
impl Sub<usize> for LogIndex {
    type Output = Self;
    fn sub(self, rhs: usize) -> Self::Output {
        LogIndex(self.0 - rhs as u64)
    }
}
impl SubAssign<usize> for LogIndex {
    fn sub_assign(&mut self, rhs: usize) {
        self.0 -= rhs as u64;
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn id(prev_term: u64, index: u64) -> LogPosition {
        LogPosition {
            prev_term: prev_term.into(),
            index: index.into(),
        }
    }
    fn noop(term: u64) -> LogEntry {
        LogEntry::Noop { term: term.into() }
    }

    #[test]
    fn log_suffix_end() {
        let suffix = LogSuffix::default();
        assert_eq!(suffix.tail().index.as_u64(), 0);

        let suffix = LogSuffix {
            head: LogPosition::default(),
            entries: vec![noop(0), noop(1)],
        };
        assert_eq!(suffix.tail().index.as_u64(), 2);
    }
    #[test]
    fn log_suffix_positions() {
        let suffix = LogSuffix::default();
        assert_eq!(suffix.positions().collect::<Vec<_>>(), [id(0, 0)]);

        let suffix = LogSuffix {
            head: LogPosition {
                prev_term: 0.into(),
                index: 30.into(),
            },
            entries: vec![noop(0), noop(2), noop(2)],
        };
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 30), id(0, 31), id(2, 32), id(2, 33)]
        );
    }
    #[test]
    fn log_suffix_skip_to() {
        let mut suffix = LogSuffix {
            head: LogPosition {
                prev_term: 0.into(),
                index: 30.into(),
            },
            entries: vec![noop(0), noop(2), noop(2)],
        };
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 30), id(0, 31), id(2, 32), id(2, 33)]
        );
        assert_eq!(suffix.entries.len(), 3);

        suffix.skip_to(31.into()).unwrap();
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 31), id(2, 32), id(2, 33)]
        );
        assert_eq!(suffix.entries.len(), 2);

        suffix.skip_to(33.into()).unwrap();
        assert_eq!(suffix.positions().collect::<Vec<_>>(), [id(2, 33)]);
        assert_eq!(suffix.entries.len(), 0);

        suffix.skip_to(33.into()).unwrap();
        assert_eq!(suffix.positions().collect::<Vec<_>>(), [id(2, 33)]);
        assert_eq!(suffix.entries.len(), 0);
    }
    #[test]
    fn log_suffix_truncate() {
        let mut suffix = LogSuffix {
            head: LogPosition {
                prev_term: 0.into(),
                index: 30.into(),
            },
            entries: vec![noop(0), noop(2), noop(2)],
        };
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 30), id(0, 31), id(2, 32), id(2, 33)]
        );
        assert_eq!(suffix.entries.len(), 3);

        suffix.truncate(31.into()).unwrap();
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 30), id(0, 31)]
        );
        assert_eq!(suffix.entries.len(), 1);
    }
    #[test]
    fn log_suffix_slice() {
        let suffix = LogSuffix {
            head: LogPosition {
                prev_term: 0.into(),
                index: 30.into(),
            },
            entries: vec![noop(0), noop(2), noop(2)],
        };
        assert_eq!(
            suffix.positions().collect::<Vec<_>>(),
            [id(0, 30), id(0, 31), id(2, 32), id(2, 33)]
        );
        assert_eq!(suffix.entries.len(), 3);

        let slice = suffix.slice(31.into(), 33.into()).unwrap();
        assert_eq!(
            slice.positions().collect::<Vec<_>>(),
            [id(0, 31), id(2, 32), id(2, 33)]
        );
        assert_eq!(slice.entries.len(), 2);
    }
}
