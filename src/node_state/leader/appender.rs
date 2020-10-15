use futures::{Async, Future};
use std::mem;

use super::super::Common;
use crate::log::{LogEntry, LogIndex, LogSuffix};
use crate::{Io, Result};

/// リーダのローカルログへの追記の管理を行う.
///
/// メモリ上のバッファを保持しており、
/// ストレージへの追記中に新たな追加要求が発行された場合には、
/// 新規分はそのバッファに積まれていく.
pub struct LogAppender<IO: Io> {
    task: Option<IO::SaveLog>,
    in_progress: Option<LogSuffix>,
    pendings: Vec<LogEntry>,
}
impl<IO: Io> LogAppender<IO> {
    pub fn new() -> Self {
        LogAppender {
            task: None,
            in_progress: None,
            pendings: Vec::new(),
        }
    }
    pub fn is_busy(&self) -> bool {
        self.in_progress.is_some()
    }
    pub fn append(&mut self, common: &mut Common<IO>, entries: Vec<LogEntry>) {
        if self.task.is_none() {
            let head = common.log().tail();
            let suffix = LogSuffix { head, entries };
            self.task = Some(common.save_log_suffix(&suffix));
            self.in_progress = Some(suffix);
        } else {
            self.pendings.extend(entries)
        }
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<Option<LogSuffix>> {
        if let Async::Ready(Some(())) = track!(self.task.poll())? {
            self.task = None;
            let suffix = self.in_progress.take().expect("Never fails");
            track!(common.handle_log_appended(&suffix))?;

            let pendings = mem::replace(&mut self.pendings, Vec::new());
            if !pendings.is_empty() {
                self.append(common, pendings);
            }
            Ok(Some(suffix))
        } else {
            Ok(None)
        }
    }

    /// 追記処理中のものも含めて、ローカルログの終端インデックス(i.e., 長さ)を返す
    pub fn unappended_log_tail(&self, common: &Common<IO>) -> LogIndex {
        let mut tail = common.log().tail().index;
        if let Some(ref suffix) = self.in_progress {
            tail += suffix.entries.len();
        }
        tail += self.pendings.len();
        tail
    }
}
