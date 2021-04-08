use futures::future::OptionFuture;
use futures::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::super::Common;
use crate::log::{LogEntry, LogIndex, LogSuffix};
use crate::{Io, Result};

/// リーダのローカルログへの追記の管理を行う.
///
/// メモリ上のバッファを保持しており、
/// ストレージへの追記中に新たな追加要求が発行された場合には、
/// 新規分はそのバッファに積まれていく.
pub struct LogAppender<IO: Io> {
    task: Option<Pin<Box<IO::SaveLog>>>,
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
    pub fn append(&mut self, common: &mut Common<IO>, entries: Vec<LogEntry>, cx: &mut Context) {
        if self.task.is_none() {
            let head = common.log().tail();
            let suffix = LogSuffix { head, entries };
            self.task = Some(Box::pin(common.save_log_suffix(&suffix, cx)));
            self.in_progress = Some(suffix);
        } else {
            self.pendings.extend(entries)
        }
    }
    pub fn run_once(
        &mut self,
        common: &mut Common<IO>,
        cx: &mut Context<'_>,
    ) -> Result<Option<LogSuffix>> {
        let mut task: OptionFuture<_> = self.task.as_mut().into();
        if let Poll::Ready(Some(result)) = track!(Pin::new(&mut task).poll(cx)) {
            let _ = track!(result)?;
            self.task = None;
            let suffix = self.in_progress.take().expect("Never fails");
            track!(common.handle_log_appended(&suffix))?;

            let pendings = mem::replace(&mut self.pendings, Vec::new());
            if !pendings.is_empty() {
                self.append(common, pendings, cx);
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
