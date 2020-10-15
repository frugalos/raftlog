extern crate futures;
extern crate prometrics;
extern crate raftlog;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serdeconv;
#[macro_use]
extern crate trackable;

macro_rules! log {
    ($($arg:expr),*) => {
        {
            use ::std::fmt::Write;
            writeln!($($arg),*).unwrap();
        }
    }
}

pub use raftlog::{Error, ErrorKind, Result};

#[doc(no_inline)]
pub use crate::io::{DeterministicIo, DeterministicIoBuilder};
pub use crate::logger::Logger;
pub use crate::simulator::Simulator;
pub use crate::simulator_config::SimulatorConfig;

pub mod io;
pub mod machine;
pub mod process;
pub mod types;

mod logger;
mod simulator;
mod simulator_config;
