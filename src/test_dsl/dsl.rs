use std::collections::{BTreeMap, BTreeSet};

use crate::cluster::ClusterMembers;
use crate::election::Role;
use crate::log::{self, LogPosition};
use crate::node::NodeId;
use crate::test_dsl::impl_io::*;
use crate::ReplicatedLog;

use prometrics::metrics::MetricBuilder;

use std::fmt;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct NodeName(u8);

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub enum LogEntry {
    Noop(u8 /* term */),
    Com(u8 /* term */),
}

#[derive(Clone, Debug)]
pub enum Pred {
    SnapShotIs(u8 /* prev_term */, u8 /* index */),
    RawLogIs(u8 /* term */, u8 /* index */, Vec<LogEntry>),
    IsLeader,
    IsFollower,
}

pub fn check(rlog: &ReplicatedLog<TestIo>, pred: Pred) -> bool {
    use Pred::*;

    match pred {
        IsLeader => rlog.local_node().role == Role::Leader,
        IsFollower => rlog.local_node().role == Role::Follower,
        SnapShotIs(prev_term, index) => {
            if let Some(snapshot) = &rlog.io().snapshot() {
                (snapshot.tail.prev_term.as_u64() == prev_term as u64)
                    && (snapshot.tail.index.as_u64() == index as u64)
            } else {
                false
            }
        }
        RawLogIs(term, index, entries) => {
            if let Some(rawlog) = &rlog.io().rawlog() {
                let head: LogPosition = rawlog.head;

                let head_check: bool = (head.prev_term.as_u64() == term as u64)
                    && (head.index.as_u64() == index as u64);

                if !head_check {
                    return false;
                }

                if rawlog.entries.len() != entries.len() {
                    return false;
                }

                for (x, y) in rawlog.entries.iter().zip(entries.iter()) {
                    match (x, y) {
                        (log::LogEntry::Noop { term: t1 }, LogEntry::Noop(t2)) => {
                            if t1.as_u64() != *t2 as u64 {
                                return false;
                            }
                        }
                        (log::LogEntry::Command { term: t1, .. }, LogEntry::Com(t2)) => {
                            if t1.as_u64() != *t2 as u64 {
                                return false;
                            }
                        }
                        (_, _) => {
                            return false;
                        }
                    }
                }
                true
            } else {
                false
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Command {
    Timeout(NodeName),
    SendBan(NodeName, NodeName), // arg0 does not send any message to arg1 hereafter
    SendAllow(NodeName, NodeName),
    RecvBan(NodeName, NodeName), // arg1 discrads messages from arg0 hereafter
    RecvAllow(NodeName, NodeName),
    Propose(NodeName),
    Heartbeat(NodeName),
    Reboot(NodeName, ClusterMembers),
    Step(NodeName),
    StepAll,
    RunUntil(Pred),
    Check(NodeName, Pred),
    Dump(NodeName),
    RunAllUntilStabilize,
    TakeSnapshot(NodeName),
}

pub type Service = BTreeMap<NodeName, ReplicatedLog<TestIo>>;

pub fn interpret(cs: &[Command], service: &mut Service) {
    for c in cs {
        interpret_command(c.clone(), service);
    }
}

fn interpret_command(c: Command, service: &mut Service) {
    use crate::futures::Stream;
    use Command::*;

    println!("\n Now executing {:?} ...", &c);

    match c {
        TakeSnapshot(node) => {
            let rlog = service.get_mut(&node).unwrap();
            let index = rlog.local_history().tail().index;
            rlog.install_snapshot(index, Vec::new()).unwrap();
        }
        Check(node, pred) => {
            let rlog = service.get_mut(&node).unwrap();
            assert!(check(&rlog, pred));
        }
        Heartbeat(node) => {
            let rlog = service.get_mut(&node).unwrap();
            rlog.heartbeat().unwrap();
        }
        Dump(node) => {
            let rlog = service.get_mut(&node).unwrap();
            println!("{:?}", rlog.local_node());
            dbg!(rlog.local_history());
            dbg!(rlog.io().snapshot());
            dbg!(rlog.io().rawlog());
        }
        StepAll => {
            println!("<StepAll>");
            for (n, io) in service.iter_mut() {
                println!("\n  <Step {}>", n.to_string());
                let ev = io.poll().unwrap();
                println!("  [{:?}] {:?}", n, ev);
                println!("  </Step {}>", n.to_string());
            }
            println!("</StepAll>");
        }
        Step(node) => {
            service.get_mut(&node).unwrap().poll().unwrap();
        }
        RunAllUntilStabilize => loop {
            let mut check = true;
            for (n, io) in service.iter_mut() {
                println!("\n  <Step {}>", n.to_string());
                unsafe {
                    io.io_mut().was_io_op = false;
                    let ev = io.poll().unwrap();
                    dbg!(&io.io_mut().was_io_op);
                    dbg!(&ev);
                    if io.io_mut().was_io_op || !ev.is_not_ready() {
                        check = false;
                    }
                }
                println!("  </Step {}>", n.to_string());
            }
            if check {
                return;
            }
        },
        Timeout(node) => {
            let io: &mut TestIo = unsafe { service.get_mut(&node).unwrap().io_mut() };
            io.invoke_timer();
        }
        SendBan(from, to) => {
            let io: &mut TestIo = unsafe { service.get_mut(&from).unwrap().io_mut() };
            io.add_send_ban_list(&to.to_string());
        }
        SendAllow(from, to) => {
            let io: &mut TestIo = unsafe { service.get_mut(&from).unwrap().io_mut() };
            io.del_send_ban_list(&to.to_string());
        }
        RecvBan(reciever, sender) => {
            let io: &mut TestIo = unsafe { service.get_mut(&reciever).unwrap().io_mut() };
            io.add_recv_ban_list(&sender.to_string());
        }
        RecvAllow(reciever, sender) => {
            let io: &mut TestIo = unsafe { service.get_mut(&reciever).unwrap().io_mut() };
            io.del_recv_ban_list(&sender.to_string());
        }
        Propose(node) => {
            service
                .get_mut(&node)
                .unwrap()
                .propose_command(Vec::new())
                .unwrap();
        }
        Reboot(node, cluster) => {
            let rlog = service.remove(&node).unwrap();
            let io: TestIo = rlog.release_io();
            let rlog = ReplicatedLog::new(
                NodeId::new(node.to_string()),
                cluster,
                io,
                &MetricBuilder::new(),
            )
            .unwrap();
            service.insert(node, rlog);
        }
        _ => {
            dbg!(&c);
        }
    }
}

pub fn build_complete_graph(names: &[NodeName]) -> (Service, ClusterMembers) {
    let nodes: BTreeSet<NodeId> = names.iter().map(|s| NodeId::new(s.to_string())).collect();

    let mut ios = BTreeMap::new();
    let mut service = BTreeMap::new();

    for node in &nodes {
        ios.insert(node.clone(), TestIo::new(node.clone()));
    }

    for src in &nodes {
        let mut io_src = ios.remove(&src).unwrap();
        for dst in &nodes {
            if src != dst {
                let io_dst = ios.get(&dst).unwrap();
                io_src.set_channel(dst.clone(), io_dst.copy_sender());
            }
        }
        ios.insert(src.clone(), io_src);
    }

    for name in names {
        let node = NodeId::new(name.to_string());
        let io = ios.remove(&node).unwrap();
        let rlog =
            ReplicatedLog::new(node.clone(), nodes.clone(), io, &MetricBuilder::new()).unwrap();
        service.insert(*name, rlog);
    }

    (service, nodes)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn scenarioA() {
        use Command::*;
        use LogEntry::*;

        let a = NodeName(0);
        let b = NodeName(1);
        let c = NodeName(2);
        let (mut service, cluster) = build_complete_graph(&[a, b, c]);

        interpret(
            &vec![
                RunAllUntilStabilize,
                Timeout(a),
                RunAllUntilStabilize,
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsFollower),
                Check(c, Pred::IsFollower),
                SendBan(b, a),
                RecvBan(b, a), // bからaには送らないし、bはaからは受け取らない
                SendBan(c, a),
                RecvBan(c, a), // cからaには送らないし、cはaからは受け取らない
                Propose(a),
                Propose(a),
                Propose(a),
                Propose(a),
                Propose(a), // aにデータをためて
                Timeout(b),
                Timeout(c), // bとcは新しいTermへ移る準備(aのfollowを外す)
                RunAllUntilStabilize,
                Timeout(b), // 一方でbとcの間ではbを新しいリーダーにする
                RunAllUntilStabilize,
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsLeader),
                Check(c, Pred::IsFollower),
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4)])),
                TakeSnapshot(b),
                RunAllUntilStabilize,
                Check(b, Pred::SnapShotIs(4, 2)),
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4)])), // BUG?: snapshotをとってもrawlogを削らない??
                SendAllow(b, a),
                RecvAllow(b, a), // 準備が終わったので、b <-> a 間を許可し
                SendAllow(c, a),
                RecvAllow(c, a), // c <-> a 間も許可する
                RunAllUntilStabilize,
                Dump(a),
                Dump(b),
                Heartbeat(b), // bからaとcへheartbeatを送る
                RunAllUntilStabilize,
                Dump(a),
                Reboot(a, cluster),   // rebootして
                RunAllUntilStabilize, // 初期化時にtermの不整合に気づく
            ],
            &mut service,
        );
    }

    #[test]
    fn scenarioB() {
        use Command::*;
        use LogEntry::*;

        let a = NodeName(0);
        let b = NodeName(1);
        let c = NodeName(2);
        let (mut service, cluster) = build_complete_graph(&[a, b, c]);

        interpret(
            &vec![
                RunAllUntilStabilize,
                Timeout(a),
                RunAllUntilStabilize,
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2)])),
                SendBan(b, a),
                RecvBan(b, a),
                SendBan(c, a),
                RecvBan(c, a),
                Propose(a),
                Propose(a), // aのlogを一通り長くしておく
                RunAllUntilStabilize,
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Com(2), Com(2)])),
                Timeout(b),
                Timeout(c), // b と c の a を追従する状態を解除する
                RunAllUntilStabilize,
                Timeout(b), // b を leaderにする
                RunAllUntilStabilize,
                SendAllow(b, a),
                RecvAllow(b, a),
                SendAllow(c, a),
                RecvAllow(c, a),
                Heartbeat(b),
                RunAllUntilStabilize,
                Dump(a),
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4), Com(2)])), // これが通る = バグ
                Reboot(a, cluster),   // rebootだけしておく
                RunAllUntilStabilize, // reboot後の様々な計算が行われて、ここでエラーが生じる
            ],
            &mut service,
        );
    }
}
