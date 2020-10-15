//! シミュレータで使われる状態機械.
use futures::{Async, Poll, Stream};
use raftlog::cluster::ClusterMembers;
use raftlog::node::NodeId;
use raftlog::Event;
use std::collections::BTreeMap;
use trackable::error::ErrorKindExt;

use crate::process::Process;
use crate::types::LogicalDuration;
use crate::{DeterministicIoBuilder, Error, ErrorKind, Logger, Result};

/// 状態機械に発行されるコマンド(加算値).
pub type Command = u64;

/// 状態機械のスナップショット.
pub type Snapshot = MachineState;

/// 状態機械の状態.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MachineState {
    pub generation: u64,
    pub sum: u64,
}
impl MachineState {
    /// 新しい`State`インスタンスを生成する.
    pub fn new() -> Self {
        MachineState {
            generation: 0,
            sum: 0,
        }
    }

    /// コマンドを処理する.
    pub fn handle_command(&mut self, command: Command) {
        self.generation += 1;
        self.sum = self.sum.wrapping_add(command);
    }
}
impl Default for MachineState {
    fn default() -> Self {
        Self::new()
    }
}

/// 複製された状態機械.
pub struct ReplicatedStateMachine {
    processes: BTreeMap<NodeId, Process>,
    logger: Logger,
    io: DeterministicIoBuilder,
}
impl ReplicatedStateMachine {
    /// 新しい`ReplicatedStateMachine`インスタンスを生成する.
    pub fn new(logger: Logger, members: &ClusterMembers, io: DeterministicIoBuilder) -> Self {
        let processes = members
            .iter()
            .map(|node| {
                let mut logger = logger.clone();
                logger.set_id(node.clone());
                (
                    node.clone(),
                    Process::new(
                        logger,
                        node.clone(),
                        members.clone(),
                        io.build(node.clone()),
                    ),
                )
            }).collect();
        ReplicatedStateMachine {
            processes,
            logger,
            io,
        }
    }
    pub fn take_snapshot(&mut self, node: &NodeId) -> Result<()> {
        let process = track!(
            self.processes
                .get_mut(node)
                .ok_or_else(|| ErrorKind::Other.error())
        )?;
        track!(process.take_snapshot())
    }
    pub fn terminate_node(&mut self, node: &NodeId, restart: LogicalDuration) -> Result<()> {
        let process = track_assert_some!(self.processes.get_mut(node), ErrorKind::Other);
        track!(process.terminate(restart))
    }
    pub fn processes(&self) -> &BTreeMap<NodeId, Process> {
        &self.processes
    }
    pub fn propose_command(&mut self, command: Command) -> Result<bool> {
        for p in self.processes.values_mut() {
            if let Err(e) = p.propose_command(command) {
                track_assert_eq!(*e.kind(), ErrorKind::NotLeader, ErrorKind::Other);
            } else {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub fn change_cluster_members(&mut self, new_members: &ClusterMembers) -> Result<()> {
        let mut old_members = None;
        for p in self.processes.values_mut() {
            if let Err(e) = p.propose_config(new_members.clone()) {
                track_assert_eq!(*e.kind(), ErrorKind::NotLeader, ErrorKind::Other);
            } else {
                old_members = Some(p.primary_members().clone());
                break;
            }
        }
        if let Some(old_members) = old_members {
            log!(self.logger, "old members: {:?}", old_members);
            for node in new_members {
                if self.processes.contains_key(node) {
                    continue;
                }
                log!(self.logger, "Spawns new process: {:?}", node);
                let mut logger = self.logger.clone();
                logger.set_id(node.clone());
                self.processes.insert(
                    node.clone(),
                    Process::new_join(
                        logger,
                        node.clone(),
                        old_members.clone(),
                        self.io.build(node.clone()),
                    ),
                );
            }
        }
        Ok(())
    }
    pub fn heartbeat(&mut self) -> Result<()> {
        for p in self.processes.values_mut() {
            if let Err(e) = p.heartbeat() {
                track_assert_eq!(*e.kind(), ErrorKind::NotLeader, ErrorKind::Other);
            } else {
                break;
            }
        }
        Ok(())
    }
}
impl Stream for ReplicatedStateMachine {
    type Item = Vec<(NodeId, Event)>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let mut events = Vec::new();
        for (id, process) in &mut self.processes {
            if let Async::Ready(item) = track!(process.poll())? {
                if let Some(event) = item {
                    events.push((id.clone(), event));
                } else {
                    unreachable!()
                }
            }
        }
        if events.is_empty() {
            Ok(Async::NotReady)
        } else {
            Ok(Async::Ready(Some(events)))
        }
    }
}
