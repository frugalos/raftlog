//! テスト用のDSLを提供するモジュール

use std::collections::{BTreeMap, BTreeSet};

use crate::cluster::ClusterMembers;
use crate::election::Role;
use crate::log::{self, LogPosition};
use crate::node::NodeId;
use crate::test_dsl::impl_io::*;
use crate::ReplicatedLog;

use prometrics::metrics::MetricBuilder;

use std::fmt;

/// DSL中でノード名を表すために用いる構造体
/// 現時点ではu8のnewtypeに過ぎない
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct NodeName(u8);

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "node-{}", self.0)
    }
}

/// Raftlogにおけるログのエントリを抽象化した構造体
#[derive(Clone, Debug)]
pub enum LogEntry {
    /// Noop（新しいリーダーが選ばれた際にissueされるコマンド）の抽象
    Noop(u8 /* term */),

    /// Proposeされるコマンドを抽象したもの
    /// データの内容には興味がないのでomitしている
    Com(u8 /* term */),
}

/// DSLの実行中における
/// あるノードを検査するためのノード検証述語(predicate)
#[derive(Clone, Debug)]
pub enum Pred {
    /// Not(pred)がtrue iff predがfalse
    Not(Box<Pred>),

    /// ノードのsnapshotがNoneではなく
    /// かつ、prev_termとindexを値として持っているかを調べる
    /// Snapshotにはこの他にデータを表すバイナリフィールドもあるが
    /// 簡単のためにomitしてある
    SnapShotIs(u8 /* prev_term */, u8 /* index */),

    /// ノードのrawlog（ログのうちsnapshotになっていない部分）がNoneではなく
    /// かつ、`term`, `index`, `entries` から成り立っているかを調べる
    RawLogIs(
        u8,            /* term */
        u8,            /* index */
        Vec<LogEntry>, /* entries */
    ),

    /// ノードのstateがLeaderかどうかを調べる
    IsLeader,

    /// ノードのstateがFollowerかどうかを調べる
    IsFollower,

    /// Log中のTermの整合性を調べる。
    /// これは2つの段階からなる:
    /// 1. snapshotとrawlogが正しく接合している
    /// 2. rawlogのtermが昇順になっている
    LogTermConsistency,
}

/// 引数`rlog`で表される特定のノードが、述語`pred`を満たすかどうかを調べる
/// `pred`を満足する場合は`true`を返し、
/// そうでない場合は`false`を返す。
fn check(rlog: &ReplicatedLog<TestIo>, pred: Pred) -> bool {
    use Pred::*;

    match pred {
        Not(pred) => !check(rlog, *pred),
        IsLeader => rlog.local_node().role == Role::Leader,
        IsFollower => rlog.local_node().role == Role::Follower,
        LogTermConsistency => {
            let mut valid_glue = true;

            // snapshotとrawlogが両方ある場合は、
            // snapshotに続く形でrawlogが存在することを確認する
            if let Some(snapshot) = &rlog.io().snapshot() {
                if let Some(rawlog) = &rlog.io().rawlog() {
                    valid_glue = snapshot.tail.prev_term == rawlog.head.prev_term;
                }
            }

            // rawlogが存在する場合は、termが昇順になっていることを確認する
            let is_sorted = if let Some(rawlog) = &rlog.io().rawlog() {
                let term_v: Vec<u64> = rawlog
                    .entries
                    .iter()
                    .map(|entry| entry.term().as_u64())
                    .collect();
                let is_sorted = term_v.windows(2).all(|w| w[0] <= w[1]);
                is_sorted
            } else {
                false
            };

            valid_glue & is_sorted
        }
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
                dbg!(&rawlog);

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

/// DSLの基本構成要素となる命令（コマンド）を定義するenum
#[derive(Clone, Debug)]
pub enum Command {
    /// 指定されたノードをタイムアウトさせる
    Timeout(NodeName),

    /// `RecvBan(recv, sender)`を実行すると
    /// ノード`recv`では、`sender`から送られて来たデータが破棄される。
    /// これはデータ受信時にフィルタアウトされることになり、
    /// `recv`ではraftlogレベルではデータに対して一切計算が行われない。
    RecvBan(NodeName, NodeName),

    /// `RecvAllow(r, s)`を実行すると
    /// `r`で`s`からのデータを正常に受信できるようになる。
    RecvAllow(NodeName, NodeName),

    /// 指定されたノードからデータをproposeしようとする。
    /// 簡単のため、データは空のバイナリ列 vec!() になっている
    Propose(NodeName),

    /// 指定されたノードからハートビートをbroadcastする
    Heartbeat(NodeName),

    /// 指定されたノードを、指定されたcluster情報を使ってrebootする
    Reboot(NodeName, ClusterMembers),

    /// 指定されたノードを、raftlogの実装としての意味で1-stepだけ実行する
    Step(NodeName),

    /// 指定されたノードが、述語を満たすかどうかを検査する
    Check(NodeName, Pred),

    /// 指定されたノードの持つ内部情報を出力する
    Dump(NodeName),

    /// 指定されたノードでsnapshotをとる
    TakeSnapshot(NodeName),

    /// 全てのノードが安定するまで1-stepずつ実行する。
    /// 例えば2つのノード A, B があるならば
    /// loop {
    ///  Step(A);
    ///  Step(B);
    ///  check_stabilized();
    /// }
    /// のようになる。
    /// 「安定」というのはこれ以上loopを繰り返しても
    /// 何のI/O操作も行われず、計算状況が変化しなくなる
    /// ということを意味する。
    RunAllUntilStabilize,
}

/// ノード名と`ReplicatedLog`で表される実体を対応させるテーブル
pub type Service = BTreeMap<NodeName, ReplicatedLog<TestIo>>;

/// DSLのコマンド列（プログラム）を実行する関数
pub fn interpret(cs: &[Command], service: &mut Service) {
    for c in cs {
        interpret_command(c.clone(), service);
    }
}

/// DSLの一つのコマンドを実行する関数
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
            println!("[Node Info] {:?}", rlog.local_node());
            println!("[History] {:?}", rlog.local_history());
            println!("[Snapshot] {:?}", rlog.io().snapshot());
            println!("[Rawlog] {:?}", rlog.io().rawlog());
        }
        Step(node) => {
            service.get_mut(&node).unwrap().poll().unwrap();
        }
        RunAllUntilStabilize => loop {
            let mut check = true;
            for (_n, io) in service.iter_mut() {
                unsafe {
                    let events_len = io.io_mut().io_events().len();
                    let ev = io.poll().unwrap();
                    if events_len < io.io_mut().io_events().len() || !ev.is_not_ready() {
                        check = false;
                    }
                }
            }
            if check {
                return;
            }
        },
        Timeout(node) => {
            let io: &mut TestIo = unsafe { service.get_mut(&node).unwrap().io_mut() };
            io.invoke_timer();
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
    }
}

/// ノードネームの列が与えられた時に
/// 丁度それらを構成要素として含むようなraft clusterを構成する。
///
/// 全点間通信ができる状態にしてあるので
/// complete graph（w.r.t. ネットワークトポロジー）という語を用いている。
pub fn build_complete_graph(names: &[NodeName]) -> (Service, ClusterMembers) {
    let nodes: BTreeSet<NodeId> = names.iter().map(|s| NodeId::new(s.to_string())).collect();

    let mut ios = BTreeMap::new();
    let mut service = BTreeMap::new();

    for node in &nodes {
        ios.insert(node.clone(), TestIo::new(node.clone(), false));
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

    /// あるノードで、
    /// snapshot(prefix)の表す最終termと
    /// rawlog(suffix)の先頭エントリが期待するtermで
    /// ズレが生じる現象を再現させる。
    ///
    /// このテストではノードaでズレが生じるようにする。
    #[test]
    #[rustfmt::skip]
    fn issue18_scenario1() {
        use Command::*;
        use LogEntry::*;

        let a = NodeName(0);
        let b = NodeName(1);
        let c = NodeName(2);
        let (mut service, _cluster) = build_complete_graph(&[a, b, c]);

        interpret(
            &vec![
                RunAllUntilStabilize,
                Timeout(a),
                RunAllUntilStabilize,
                // ここまでで a がリーダーになっている

                // 実際にリーダーになっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsFollower),
                Check(c, Pred::IsFollower),

                RecvBan(a, b), RecvBan(a, c), // aはbからもcからも受け取らない
                RecvBan(b, a), // bはaからは受け取らない
                RecvBan(c, a), // cはaからは受け取らない

                // aが孤立している状況で
                // データをproposeすることで
                // aにデータをためる
                Propose(a), Propose(a), Propose(a), Propose(a), Propose(a),

                // bとcは新しいTermへ移る準備(aのfollowを外す)
                Timeout(b), Timeout(c),  RunAllUntilStabilize,

                // bを新しいリーダーにする
                Timeout(b), RunAllUntilStabilize,

                // 想定している状況になっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsLeader),
                Check(c, Pred::IsFollower),
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Com(2), Com(2), Com(2), Com(2), Com(2)])),
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4)])),

                // bで現在のrawlogをスナップショット化する
                TakeSnapshot(b), RunAllUntilStabilize,
                Check(b, Pred::SnapShotIs(4, 2)),

                // 準備が終わったので、全点間で通信ができるように戻す
                RecvAllow(a, b), RecvAllow(a, c),
                RecvAllow(b, a),
                RecvAllow(c, a),

                // bからaとcへheartbeatを送る
                Heartbeat(b), RunAllUntilStabilize,

                // aがstaleしていることがこの時点で判明して
                // bのsnapshotがaに移る
                // snapshotは、rawlog[2].term = 4 であることを要求しているが
                Check(a, Pred::SnapShotIs(4 /* term */ , 2 /* index */)),

                // 一方でbのrawlogは、snapshotの保存にあたって先頭部分が削除され次のようになる
                // snapshotの要求ではterm4から始まることになっているが、これに反する。
                Check(a, Pred::RawLogIs(2, 2, vec![Com(2), Com(2), Com(2), Com(2)])),

                // 最終的な確認として、Termの並びが"In"consistentであることを調べる。
                Check(a, Pred::Not(Box::new(Pred::LogTermConsistency))),

                // 不整合が実際に生じるのは、reboot時のエラーチェックである。
                // panicを確認するには、次のコマンド2つを実行すれば良い。
                // （既に直前のCheckで不整合が明らかになっているので実行しない）
                // Reboot(a, _cluster), Step(a)
            ],
            &mut service,
        );
    }

    /// あるノードで,
    /// rawlogの一部分だけを書き換えた結果として、
    /// termの昇順整列制約が敗れることを確認する。
    /// 上のscenario1ではsnapshot, rawlog間でズレが生じていたが
    /// rawlog単体でもズレが生じることを示す。
    ///
    /// このテストではノードaで不整合が起こることを確認する。
    #[test]
    #[rustfmt::skip]
    fn issue18_scenario2() {
        use Command::*;
        use LogEntry::*;

        let a = NodeName(0);
        let b = NodeName(1);
        let c = NodeName(2);
        let (mut service, _cluster) = build_complete_graph(&[a, b, c]);

        interpret(
            &vec![
                RunAllUntilStabilize,
                Timeout(a),
                RunAllUntilStabilize,
                // ここまでで a がリーダーになっている

                // 実際にリーダーになっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsFollower),
                Check(c, Pred::IsFollower),

                RecvBan(a, b), RecvBan(a, c), // aはbからもcからも受け取らない
                RecvBan(b, a), // bはaからは受け取らない
                RecvBan(c, a), // cはaからは受け取らない

                // aを独立させた上でデータを投入する
                Propose(a), Propose(a),
                RunAllUntilStabilize,
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Com(2), Com(2)])),

                // bとcの間のやりとりで、bを新しいリーダーにする
                Timeout(b), Timeout(c),
                RunAllUntilStabilize,
                Timeout(b), // b を leaderにする
                RunAllUntilStabilize,

                Check(a, Pred::IsLeader),
                Check(b, Pred::IsLeader),
                Check(c, Pred::IsFollower),
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4)])),

                // 全点間通信できるようにする
                RecvAllow(a, b), RecvAllow(a, c),
                RecvAllow(b, a),
                RecvAllow(c, a),

                // bからハートビートを送る
                Heartbeat(b), RunAllUntilStabilize,

                // aがstaleしているため
                // bの長さ2のrawlogにより上書きされるが
                // rawlog[1].term = 4 と rawlog[2].term = 2 で
                // 不整合が生じている
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4), Com(2)])),

                // 最終的な確認として、Termの並びが"In"consistentであることを調べる。
                Check(a, Pred::Not(Box::new(Pred::LogTermConsistency))),

                // この不整合は、実際にreboot処理を行うと
                // raftlog内部でエラーになる
                // Reboot(a, _cluster), Step(a),
            ],
            &mut service,
        );
    }
}
