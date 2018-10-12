extern crate futures;
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
pub use io::{DeterministicIo, DeterministicIoBuilder};
pub use logger::Logger;
pub use simulator::Simulator;
pub use simulator_config::SimulatorConfig;

pub mod io;
pub mod machine;
pub mod process;
pub mod types;

mod logger;
mod simulator;
mod simulator_config;
