use futures::{Async, Stream};
use raftlog::log::LogEntry;
use raftlog::node::NodeId;
use raftlog::Event;
use rand::Rng;

use crate::machine::{MachineState, ReplicatedStateMachine};
use crate::types::SharedRng;
use crate::{DeterministicIoBuilder, ErrorKind, Logger, Result, SimulatorConfig};

/// シミュレータ.
pub struct Simulator {
    config: SimulatorConfig,
    rng: SharedRng,
    rsm: ReplicatedStateMachine,
    commit_history: Vec<Committed>,
    logger: Logger,
}
impl Simulator {
    pub fn new(config: SimulatorConfig) -> Self {
        let mut rng = config.make_rng();
        let mut io_builder = DeterministicIoBuilder::new(rng.clone());
        io_builder
            .channel(config.io.channel.clone())
            .storage(config.io.storage.clone())
            .timer(config.io.timer.clone());
        let members = config.choose_members(&mut rng);

        let mut logger = Logger::new();
        logger.set_indent(1);
        let rsm = ReplicatedStateMachine::new(logger.clone(), &members, io_builder);
        Simulator {
            config,
            rng,
            rsm,
            commit_history: Vec::new(),
            logger,
        }
    }
    pub fn run(&mut self) -> Result<()> {
        println!(
            "Initial members: {:?}",
            self.rsm.processes().keys().collect::<Vec<_>>()
        );
        for i in 0..self.config.loop_count {
            let result = track!(self.run_once());
            if !self.logger.is_empty() {
                println!("# {}:", i);
                println!("{}", self.logger.take_buffer());
            }
            result?;
        }
        Ok(())
    }

    fn run_once(&mut self) -> Result<()> {
        if self.config.propose_command.occurred(&mut self.rng) {
            track!(self.propose_command())?;
        }
        if self.config.manual_heartbeat.occurred(&mut self.rng) {
            track!(self.heartbeat())?;
        }
        if self.config.node_down.occurred(&mut self.rng) {
            track!(self.terminate_node())?;
        }
        if self.config.change_cluster.occurred(&mut self.rng) {
            track!(self.change_cluster_members())?;
        }
        if self.config.take_snapshot.occurred(&mut self.rng) {
            track!(self.take_snapshot())?;
        }
        if let Async::Ready(item) = track!(self.rsm.poll())? {
            let events = item.expect("Never fails");
            for (node_id, event) in events {
                track!(self.handle_event(&node_id, event))?;
            }
        }
        Ok(())
    }
    fn propose_command(&mut self) -> Result<()> {
        let command = self.rng.gen();
        log!(self.logger, "[PROPOSE]\tcommand: {}", command);
        track!(self.rsm.propose_command(command))?;
        Ok(())
    }
    fn change_cluster_members(&mut self) -> Result<()> {
        let new_members = self.config.choose_members(&mut self.rng);
        log!(
            self.logger,
            "[CHANGE_CONFIG]\tnew_members: {:?}",
            new_members
        );
        track!(self.rsm.change_cluster_members(&new_members))?;
        Ok(())
    }
    fn heartbeat(&mut self) -> Result<()> {
        log!(self.logger, "[HEARTBEAT]");
        track!(self.rsm.heartbeat())?;
        Ok(())
    }
    fn terminate_node(&mut self) -> Result<()> {
        log!(self.logger, "[TERMINATE]");
        let i = self.rng.gen_range(0, self.rsm.processes().len());
        let target = self
            .rsm
            .processes()
            .keys()
            .nth(i)
            .expect("Never fails")
            .clone();
        let restart = self.config.node_restart_interval.choose(&mut self.rng);
        track!(self.rsm.terminate_node(&target, restart))?;
        Ok(())
    }
    fn take_snapshot(&mut self) -> Result<()> {
        log!(self.logger, "[SNAPSHOT]");
        let i = self.rng.gen_range(0, self.rsm.processes().len());
        let target = self
            .rsm
            .processes()
            .keys()
            .nth(i)
            .expect("Never fails")
            .clone();
        track!(self.rsm.take_snapshot(&target))?;
        Ok(())
    }
    fn handle_event(&mut self, node_id: &NodeId, event: Event) -> Result<()> {
        if let Event::Committed { index, entry } = event {
            let index = index.as_u64() as usize;
            track_assert!(index <= self.commit_history.len(), ErrorKind::Other);
            let state = self
                .rsm
                .processes()
                .get(node_id)
                .expect("Never fails")
                .machine();
            if index == self.commit_history.len() {
                let committed = Committed { entry, state };
                self.commit_history.push(committed);
            } else {
                let e = &self.commit_history[index];
                track_assert_eq!(e.entry, entry, ErrorKind::Other);
                track_assert_eq!(e.state, state, ErrorKind::Other);
            }
        }
        Ok(())
    }
}

#[derive(PartialEq, Eq)]
struct Committed {
    entry: LogEntry,
    state: MachineState,
}
