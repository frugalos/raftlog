//! [Raft]アルゴリズムに基づく分散複製ログを提供するクレート.
//!
//! このクレート自体は、アルゴリズム実装のみに専念しており、
//! 実際に動作するシステムで利用するためには、`Io`トレイトの
//! 実装を別個用意する必要がある.
//!
//! [Raft]: https://raft.github.io/
#![warn(missing_docs)]
#[cfg(test)]
extern crate fibers;
extern crate futures;
extern crate prometrics;
#[macro_use]
extern crate trackable;

pub use crate::error::{Error, ErrorKind};
pub use crate::io::Io;
pub use crate::replicated_log::{Event, ReplicatedLog};

pub mod cluster;
pub mod election;
pub mod log;
pub mod message;
pub mod metrics;
pub mod node;

mod error;
mod io;
mod node_state;
mod replicated_log;
mod test_util;

/// クレート固有の`Result`型.
pub type Result<T> = ::std::result::Result<T, Error>;
