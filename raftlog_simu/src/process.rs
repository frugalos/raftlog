//! シミュレータ上で動作するプロセス.
use futures::{Async, Future, Poll, Stream};
use prometrics::metrics::MetricBuilder;
use raftlog::cluster::ClusterMembers;
use raftlog::log::{LogEntry, LogIndex, ProposalId};
use raftlog::message::SequenceNumber;
use raftlog::node::NodeId;
use raftlog::{Event, Io, ReplicatedLog};
use std::collections::VecDeque;

use crate::machine::{Command, MachineState};
use crate::types::LogicalDuration;
use crate::{DeterministicIo, Error, ErrorKind, Logger, Result};

/// プロセス.
///
/// いわゆるOSの"プロセス"とは異なり、
/// シミュレータ上で使用される実行単位に過ぎない.
///
/// 各プロセスは、それぞれの状態(i.e., `Machine`)と分散ログノード(i.e., `ReplicatedLog`)を有する.
pub struct Process {
    logger: Logger,
    node_id: NodeId,
    members: ClusterMembers,
    state: ProcessState,
}
impl Process {
    /// 新しい`Process`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        members: ClusterMembers,
        io: DeterministicIo,
    ) -> Self {
        let state = ProcessState::Alive(Alive::new(
            logger.clone(),
            node_id.clone(),
            members.clone(),
            io,
        ));
        Process {
            logger,
            node_id,
            members,
            state,
        }
    }

    pub fn new_join(
        logger: Logger,
        node_id: NodeId,
        old_members: ClusterMembers,
        io: DeterministicIo,
    ) -> Self {
        let state = ProcessState::Alive(Alive::new_join(
            logger.clone(),
            node_id.clone(),
            old_members.clone(),
            io,
        ));
        Process {
            logger,
            node_id,
            members: old_members,
            state,
        }
    }

    pub fn primary_members(&self) -> &ClusterMembers {
        match self.state {
            ProcessState::Alive(ref s) => s.rlog.local_history().config().primary_members(),
            ProcessState::Down(_) => &self.members,
        }
    }
    pub fn committed_tail(&self) -> LogIndex {
        match self.state {
            ProcessState::Alive(ref s) => s.rlog.local_history().committed_tail().index,
            ProcessState::Down(_) => unreachable!(),
        }
    }

    /// プロセスが管理する状態機械への参照を返す.
    pub fn machine(&self) -> MachineState {
        match self.state {
            ProcessState::Alive(ref s) => s.machine,
            ProcessState::Down(ref s) => s.machine,
        }
    }

    pub fn id(&self) -> &str {
        self.node_id.as_str()
    }

    pub fn logger(&self) -> Logger {
        self.logger.clone()
    }

    pub fn propose_command(&mut self, command: Command) -> Result<()> {
        match self.state {
            ProcessState::Alive(ref mut s) => s.propose_command(command),
            ProcessState::Down(ref mut s) => s.propose_command(command),
        }
    }
    pub fn propose_config(&mut self, new_members: ClusterMembers) -> Result<()> {
        match self.state {
            ProcessState::Alive(ref mut s) => s.propose_config(new_members),
            ProcessState::Down(ref mut s) => s.propose_config(new_members),
        }
    }
    pub fn heartbeat(&mut self) -> Result<()> {
        match self.state {
            ProcessState::Alive(ref mut s) => s.heartbeat(),
            ProcessState::Down(ref mut s) => s.heartbeat(),
        }
    }
    pub fn terminate(&mut self, restart: LogicalDuration) -> Result<()> {
        match self.state {
            ProcessState::Alive(_) => {
                log!(
                    self.logger,
                    "Terminated: This will restart after {}",
                    restart
                );
                let state = Down::new(self.machine(), self.io().clone(), restart);
                self.state = ProcessState::Down(state);
            }
            ProcessState::Down(_) => log!(self.logger, "Already terminated"),
        }
        Ok(())
    }
    pub fn take_snapshot(&mut self) -> Result<()> {
        match self.state {
            ProcessState::Alive(ref mut s) => s.take_snapshot(),
            ProcessState::Down(_) => {
                log!(self.logger, "Cannot take snapshot; This process is down");
                Ok(())
            }
        }
    }
    fn io(&self) -> &DeterministicIo {
        match self.state {
            ProcessState::Alive(ref s) => s.io(),
            ProcessState::Down(ref s) => s.io(),
        }
    }
}
impl Stream for Process {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        loop {
            let next = match self.state {
                ProcessState::Alive(ref mut s) => {
                    let polled = track!(s.poll())?;
                    if let Async::Ready(Some(Event::Committed {
                        entry: LogEntry::Config { ref config, .. },
                        ..
                    })) = polled
                    {
                        if config.state().is_stable() {
                            self.members = config.new_members().clone();
                            log!(
                                self.logger,
                                "Change config finished: new_members={:?}",
                                self.members
                            );
                        }
                    }
                    return Ok(polled);
                }
                ProcessState::Down(ref mut s) => {
                    if let Async::Ready(()) = track!(s.poll())? {
                        log!(self.logger, "Restarted");
                        ProcessState::Alive(Alive::new(
                            self.logger.clone(),
                            self.node_id.clone(),
                            self.members.clone(),
                            s.io().clone(),
                        ))
                    } else {
                        return Ok(Async::NotReady);
                    }
                }
            };
            self.state = next;
        }
    }
}

#[allow(clippy::large_enum_variant)]
enum ProcessState {
    Alive(Alive),
    Down(Down),
}

struct Down {
    machine: MachineState,
    io: DeterministicIo,
    restart: LogicalDuration,
}
impl Down {
    pub fn new(machine: MachineState, io: DeterministicIo, restart: LogicalDuration) -> Self {
        Down {
            machine,
            io,
            restart,
        }
    }
    pub fn propose_command(&mut self, _command: Command) -> Result<()> {
        track_panic!(ErrorKind::NotLeader, "This process is down");
    }
    pub fn propose_config(&mut self, _: ClusterMembers) -> Result<()> {
        track_panic!(ErrorKind::NotLeader, "This process is down");
    }
    pub fn heartbeat(&mut self) -> Result<()> {
        track_panic!(ErrorKind::NotLeader, "This process is down");
    }
    pub fn io(&self) -> &DeterministicIo {
        &self.io
    }
}
impl Future for Down {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.restart == 0 {
            Ok(Async::Ready(()))
        } else {
            self.restart -= 1;
            track!(self.io.try_recv_message())?; // 受信メッセージは捨てる
            Ok(Async::NotReady)
        }
    }
}

struct Alive {
    logger: Logger,
    machine: MachineState,
    next_commit: LogIndex,
    rlog: ReplicatedLog<DeterministicIo>,
    proposals: Vec<ProposalId>,
    heartbeats: VecDeque<SequenceNumber>,
}
impl Alive {
    /// 新しい`Alive`インスタンスを生成する.
    pub fn new(
        logger: Logger,
        node_id: NodeId,
        members: ClusterMembers,
        io: DeterministicIo,
    ) -> Self {
        let metric_builder = MetricBuilder::new();
        let machine = MachineState::new();
        let rlog = ReplicatedLog::new(node_id, members, io, &metric_builder).expect("Never fails");
        Alive {
            logger,
            machine,
            next_commit: LogIndex::default(),
            rlog,
            proposals: Vec::new(),
            heartbeats: VecDeque::new(),
        }
    }
    pub fn new_join(
        logger: Logger,
        node_id: NodeId,
        old_members: ClusterMembers,
        io: DeterministicIo,
    ) -> Self {
        let metric_builder = MetricBuilder::new();
        let machine = MachineState::new();
        let rlog =
            ReplicatedLog::new(node_id, old_members, io, &metric_builder).expect("Never fails");
        Alive {
            logger,
            machine,
            next_commit: LogIndex::default(),
            rlog,
            proposals: Vec::new(),
            heartbeats: VecDeque::new(),
        }
    }
    pub fn io(&self) -> &DeterministicIo {
        self.rlog.io()
    }
    pub fn propose_command(&mut self, command: Command) -> Result<()> {
        let command = serdeconv::to_json_string(&command)
            .expect("TODO")
            .into_bytes();
        let proposal_id = track!(self.rlog.propose_command(command))?;
        log!(
            self.logger,
            "Proposed: id={:?} (ongoings={})",
            proposal_id,
            self.proposals.len()
        );
        self.proposals.push(proposal_id);
        self.proposals.sort_by_key(|p| -(p.index.as_u64() as i64)); // 非効率(だけど実際上の問題はない)
        Ok(())
    }
    pub fn propose_config(&mut self, new_members: ClusterMembers) -> Result<()> {
        let proposal_id = track!(self.rlog.propose_config(new_members))?;
        log!(
            self.logger,
            "Proposed: id={:?} (ongoings={})",
            proposal_id,
            self.proposals.len()
        );
        self.proposals.push(proposal_id);
        self.proposals.sort_by_key(|p| -(p.index.as_u64() as i64)); // 非効率(だけど実際上の問題はない)
        Ok(())
    }
    pub fn heartbeat(&mut self) -> Result<()> {
        let seq_no = track!(self.rlog.heartbeat())?;
        log!(self.logger, "Heartbeat Syn: {:?}", seq_no);
        self.heartbeats.push_back(seq_no);
        Ok(())
    }
    pub fn take_snapshot(&mut self) -> Result<()> {
        if self.rlog.is_snapshot_installing() {
            log!(self.logger, "Cannot install new snapshot: Busy");
        } else if self.next_commit < self.rlog.local_history().head().index {
            // おそらくリーダから送られたスナップショットのインストール直後で、
            // まだ`Event::SnapshotLoaded`が発行されていない（のでローカルの状態にも未反映）
            log!(
                self.logger,
                "Cannot install new snapshot: Machine state is too old; next_commit={:?}, log_head={:?}",
                self.next_commit,
                self.rlog.local_history().head()
            );
        } else {
            let snapshot = serdeconv::to_json_string(&self.machine)
                .expect("TODO")
                .into_bytes();
            track!(self.rlog.install_snapshot(self.next_commit, snapshot))?;
            log!(
                self.logger,
                "Starts taking snapshot: new_head={:?}",
                self.next_commit
            );
        }
        Ok(())
    }

    fn handle_committed(&mut self, index: LogIndex, entry: &LogEntry) -> Result<()> {
        while let Some(proposal) = self.proposals.pop() {
            if index < proposal.index {
                self.proposals.push(proposal);
                break;
            } else {
                track_assert_eq!(proposal.index, index, ErrorKind::Other);
                if proposal.term != entry.term() {
                    log!(self.logger, "Rejected: {:?}", proposal);
                }
            }
        }

        if let LogEntry::Command { command, .. } = entry {
            let command = serdeconv::from_json_slice(command).expect("TODO");
            self.machine.handle_command(command);
            self.next_commit = index + 1;
        }
        Ok(())
    }
    fn check_heartbeat_ack(&mut self) -> Result<()> {
        while let Some(seq_no) = self.heartbeats.pop_front() {
            if seq_no <= self.rlog.last_heartbeat_ack() {
                log!(self.logger, "Heartbeat Ack: {:?}", seq_no);
            } else {
                self.heartbeats.push_front(seq_no);
                break;
            }
        }
        Ok(())
    }
}
impl Stream for Alive {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        let result = track!(self.rlog.poll())?;
        if let Async::Ready(Some(ref event)) = result {
            log!(self.logger, "Event: {:?}", event);
            match *event {
                Event::Committed { ref entry, index } => {
                    track!(self.handle_committed(index, entry))?;
                }
                Event::SnapshotLoaded {
                    ref snapshot,
                    new_head,
                } => {
                    while let Some(proposal) = self.proposals.pop() {
                        if new_head.index <= proposal.index {
                            self.proposals.push(proposal);
                            break;
                        }
                        log!(self.logger, "Rejected: {:?}", proposal);
                    }
                    self.next_commit = new_head.index;
                    self.machine = serdeconv::from_json_slice(&snapshot).expect("TODO");
                }
                _ => {}
            }
        }
        track!(self.check_heartbeat_ack())?;
        Ok(result)
    }
}
