#[macro_use]
extern crate clap;
extern crate raftlog_simu;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serdeconv;
#[macro_use]
extern crate trackable;

use clap::Arg;
use trackable::error::Failure;

#[derive(Default, Serialize, Deserialize)]
struct Config {
    simulator: raftlog_simu::SimulatorConfig,
}

fn main() {
    let matches = app_from_crate!()
        .arg(
            Arg::with_name("CONFIG_FILE")
                .short("c")
                .long("config")
                .takes_value(true),
        ).arg(Arg::with_name("RANDOM_SEED").long("seed").takes_value(true))
        .arg(
            Arg::with_name("LOOP_COUNT")
                .long("loop-count")
                .takes_value(true),
        ).get_matches();

    //
    // 1. Load Configuration
    //
    let mut config = if let Some(config_file) = matches.value_of("CONFIG_FILE") {
        track_try_unwrap!(serdeconv::from_toml_file(config_file))
    } else {
        Config::default()
    };
    if let Some(seed) = matches.value_of("RANDOM_SEED") {
        let seed = track_try_unwrap!(seed.parse().map_err(Failure::from_error));
        config.simulator.seed = seed;
    }
    if let Some(loop_count) = matches.value_of("LOOP_COUNT") {
        config.simulator.loop_count =
            track_try_unwrap!(loop_count.parse().map_err(Failure::from_error));
    }
    println!("============ CONFIGURATION ============");
    println!("{}", track_try_unwrap!(serdeconv::to_toml_string(&config)));
    println!("=======================================\n");

    //
    // 2. Build Simulator
    //
    let mut simulator = raftlog_simu::Simulator::new(config.simulator);

    //
    // 3. Execute Simulator
    //
    println!("============ EXECUTION ============");
    track_try_unwrap!(simulator.run());
    println!("===================================");
}
