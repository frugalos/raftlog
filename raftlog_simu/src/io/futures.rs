//! `Future`トレイトの実装群.
use futures::{Async, Future, Poll};
use raftlog::election::Ballot;
use raftlog::log::Log;

use crate::types::LogicalDuration;
use crate::Error;

pub type Timeout = DelayedResult<(), Error>;
pub type SaveBallot = DelayedResult<(), Error>;
pub type LoadBallot = DelayedResult<Option<Ballot>, Error>;
pub type SaveLog = DelayedResult<(), Error>;
pub type LoadLog = DelayedResult<Log, Error>;

/// 結果を得られるまでに、生成時に指定された論理時間の経過が必要となる`Result`型.
#[derive(Debug)]
pub struct DelayedResult<T, E> {
    result: Option<Result<T, E>>,
    delay: LogicalDuration,
}
impl<T, E> DelayedResult<T, E> {
    /// `value`を値とする、遅延された成功結果を返す.
    pub fn ok(value: T, delay: LogicalDuration) -> Self {
        DelayedResult::done(Ok(value), delay)
    }

    /// `error`を失敗理由とする、遅延された結果を返す.
    pub fn err(error: E, delay: LogicalDuration) -> Self {
        DelayedResult::done(Err(error), delay)
    }

    /// 遅延された結果を返す.
    pub fn done(result: Result<T, E>, delay: LogicalDuration) -> Self {
        DelayedResult {
            result: Some(result),
            delay,
        }
    }
}
impl<T, E> Future for DelayedResult<T, E> {
    type Item = T;
    type Error = E;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.delay == 0 {
            let value = self
                .result
                .take()
                .expect("Cannot poll DelayedResult twice")?;
            Ok(Async::Ready(value))
        } else {
            self.delay -= 1;
            Ok(Async::NotReady)
        }
    }
}
