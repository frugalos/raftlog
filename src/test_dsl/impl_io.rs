use crate::election::{Ballot, Role};
use crate::log::{Log, LogIndex, LogPrefix, LogSuffix};
use crate::message::Message;
use crate::node::NodeId;
use crate::{Error, Io, Result};
use futures::{Async, Future, Poll};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};

extern crate prometrics;

fn ballot_to_str(b: &Ballot) -> String {
    format!(
        "ballot(term: {}, for: {})",
        b.term.as_u64(),
        b.voted_for.as_str()
    )
}

fn message_to_string(m: &Message) -> String {
    use Message::*;
    match m {
        RequestVoteCall(vcall) => {
            format!(
                "[Vcall] {:?} {:?} {:?}",
                vcall.header.seq_no, vcall.header.term, vcall.log_tail
            )
        }
        RequestVoteReply(vreply) => {
            format!(
                "[Vrep] {:?} {:?} voted={:?}",
                vreply.header.seq_no, vreply.header.term, vreply.voted
            )
        }
        AppendEntriesCall(ecall) => {
            format!(
                "[AEcall] {:?} {:?} commited={:?}, suffix={:?}",
                ecall.header.seq_no, ecall.header.term, ecall.committed_log_tail, ecall.suffix
            )
        }
        AppendEntriesReply(ereply) => {
            format!(
                "[AErep] {:?} {:?} tail={:?}, busy={:?}",
                ereply.header.seq_no, ereply.header.term, ereply.log_tail, ereply.busy
            )
        }
        InstallSnapshotCast(snap) => {
            format!(
                "[Snap] {:?} {:?} snap={:?}",
                snap.header.seq_no, snap.header.term, snap.prefix
            )
        }
    }
}

/*
 * 関数 `over_write` は、issue18の確認のために、以下の frugalos raft のコード
 * https://github.com/frugalos/frugalos/blob/bdb58d47ef50e5f7038bdce4b6cd31736260d6e8/frugalos_raft/src/storage/log_suffix.rs#L164
 * を抽象化したもので、出来るだけ同じ動きになるようにする。
 */
fn over_write(now: &mut LogSuffix, new: &LogSuffix) {
    /*
     * 次のような上書き不能な形をしていないか検査する
     *  now = [...)
     *              [...) = new
     */
    assert!(new.head.index <= now.tail().index);

    let (offset, entries_offset) = if now.head.index <= new.head.index {
        /*
         * [ self ...
         *      [ next ...
         *      ^--offset
         */

        (new.head.index - now.head.index, 0)
    } else {
        /*
         * Strange case:
         *      [ self ...
         * [ next ...
         */
        // どういう場合にここに到達するのか想像できない
        // https://github.com/frugalos/frugalos/blob/bdb58d47ef50e5f7038bdce4b6cd31736260d6e8/frugalos_raft/src/storage/mod.rs#L299
        // ただし、上に曖昧なコメントが書かれているので、何かしらあるのかもしれない
        unimplemented!("How do you reach here? Please report us.");
    };

    dbg!((offset, entries_offset));

    // Suggestion: LogSuffixにこの計算をするmethodを追加した方が良い
    let prev_term = if offset == 0 {
        now.head.prev_term
    } else {
        now.entries[offset - 1].term()
    };

    // new[entries_offset..]でnow[offset..]を上書きして良いかを確認する。
    let prev_term_of_new = new.positions().nth(entries_offset).map(|p| p.prev_term);
    assert!(
        prev_term_of_new == Some(prev_term),
        "{:?} {:?}",
        prev_term,
        prev_term_of_new
    );

    let length = now.entries.len();
    for i in 0..new.entries.len() {
        if offset + i < length {
            // 上書き
            now.entries[offset + i] = new.entries[i].clone();
        } else {
            // 追記
            now.entries.push(new.entries[i].clone());
        }
    }
}

/*
 * Test用のIO
 */
pub struct TestIo {
    node_id: NodeId,
    recv: Receiver<Message>,
    send: Sender<Message>,
    channels: BTreeMap<NodeId, Sender<Message>>,
    ballots: Vec<Ballot>,
    snapshotted: Option<LogPrefix>,
    rawlogs: Option<LogSuffix>,
    invoker: Option<Sender<()>>,
    send_ban_list: BTreeSet<String>,
    recv_ban_list: BTreeSet<String>,
    pub was_io_op: bool,
}

impl TestIo {
    pub fn add_send_ban_list(&mut self, name: &str) {
        self.send_ban_list.insert(name.to_string());
    }
    pub fn del_send_ban_list(&mut self, name: &str) {
        self.send_ban_list.remove(name);
    }
    pub fn add_recv_ban_list(&mut self, name: &str) {
        self.recv_ban_list.insert(name.to_string());
    }
    pub fn del_recv_ban_list(&mut self, name: &str) {
        self.recv_ban_list.remove(name);
    }

    pub fn snapshot(&self) -> &Option<LogPrefix> {
        &self.snapshotted
    }
    pub fn rawlog(&self) -> &Option<LogSuffix> {
        &self.rawlogs
    }

    pub fn new(node_id: NodeId) -> Self {
        let (send, recv) = channel();
        Self {
            node_id,
            recv,
            send,
            channels: Default::default(),
            ballots: Vec::new(),
            snapshotted: None,
            rawlogs: None,
            invoker: None,

            send_ban_list: BTreeSet::new(),
            recv_ban_list: BTreeSet::new(),

            was_io_op: false,
        }
    }

    pub fn copy_sender(&self) -> Sender<Message> {
        self.send.clone()
    }

    pub fn set_channel(&mut self, node: NodeId, chan: Sender<Message>) {
        self.channels.insert(node, chan);
    }

    pub fn invoke_timer(&self) {
        if let Some(invoker) = &self.invoker {
            invoker.send(()).unwrap();
        }
    }
}

impl Io for TestIo {
    fn try_recv_message(&mut self) -> Result<Option<Message>> {
        let r = self.recv.try_recv();
        if let Ok(v) = r {
            self.was_io_op = true;

            let who = &v.header().sender;
            println!(
                "\n[try_recv] {:?} <---- {:?}\n{}",
                self.node_id,
                who,
                message_to_string(&v)
            );
            if self.recv_ban_list.contains(&who.clone().into_string()) {
                return Ok(None);
            }
            Ok(Some(v))
        } else {
            let err = r.err().unwrap();
            if err == TryRecvError::Empty {
                Ok(None)
            } else {
                panic!("disconnected");
            }
        }
    }

    fn send_message(&mut self, message: Message) {
        self.was_io_op = true;

        let dest: NodeId = message.header().destination.clone();

        println!(
            "\n[send] {:?} ----> {:?}\n{}",
            self.node_id,
            dest,
            message_to_string(&message)
        );

        if self.send_ban_list.contains(&dest.clone().into_string()) {
            return;
        }

        let channel = self.channels.get(&dest).unwrap();
        channel.send(message).unwrap();
    }

    type SaveBallot = BallotSaver;
    fn save_ballot(&mut self, ballot: Ballot) -> Self::SaveBallot {
        self.was_io_op = true;

        self.ballots.push(ballot.clone());
        BallotSaver(self.node_id.clone(), ballot)
    }

    type LoadBallot = BallotLoader;
    fn load_ballot(&mut self) -> Self::LoadBallot {
        self.was_io_op = true;

        if self.ballots.is_empty() {
            BallotLoader(self.node_id.clone(), None)
        } else {
            let last_pos = self.ballots.len() - 1;
            let ballot = self.ballots[last_pos].clone();
            BallotLoader(self.node_id.clone(), Some(ballot))
        }
    }

    type SaveLog = LogSaver;
    fn save_log_prefix(&mut self, prefix: LogPrefix) -> Self::SaveLog {
        self.was_io_op = true;

        println!("SAVE SNAPSHOT: {:?}", &prefix);

        if let Some(snap) = &self.snapshotted {
            assert!(prefix.tail.is_newer_or_equal_than(snap.tail));
        }
        self.snapshotted = Some(prefix);
        LogSaver(SaveMode::SnapshotSave, self.node_id.clone())
    }
    fn save_log_suffix(&mut self, suffix: &LogSuffix) -> Self::SaveLog {
        self.was_io_op = true;

        println!("SAVE RAWLOGS: {:?}", &suffix);

        if let Some(rawlogs) = &mut self.rawlogs {
            assert!(suffix.head.is_newer_or_equal_than(rawlogs.head));
            over_write(rawlogs, suffix);
        } else {
            // 空のdiskへの書き込みに対応する
            self.rawlogs = Some(suffix.clone());
        }

        LogSaver(SaveMode::RawLogSave, self.node_id.clone())
    }

    type LoadLog = LogLoader;
    fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog {
        self.was_io_op = true;

        dbg!("load log", &start, &end);

        if self.snapshotted.is_none() && self.rawlogs.is_none() {
            if start == LogIndex::new(0) && end == None {
                // 無に対するロードなら無を作る
                let log = LogSuffix::default(); // 何もできないのでdefaultを使う
                return LogLoader(Some(Log::from(log)));
            }
            panic!("Try load from None: start = {:?}, end = {:?}", start, end);
        }

        if end == None {
            // Case: startから全て [start..) 読み込む

            if let Some(snap) = &self.snapshotted {
                // snapshotが存在し……
                if start == LogIndex::new(0) {
                    // なおかつ、startが0からであれば、まずsnapshotを読み込むことにする。
                    // 0でなければならないのは、IO traitのload logのrequirementがそうなっているから。
                    //
                    // 直後でreturnしているfutureの実行が終わり次第、
                    // 自動的にこの load_log が呼ばれ、
                    // 必要であればsnapshot以降が読み込まれていく。
                    //
                    // https://github.com/frugalos/raftlog/blob/087d8019b42a05dbc5b463db076041d45ad2db7f/src/node_state/loader.rs#L36
                    // ^--- この実装により、snapshot読み込みの後で、必要があればrawlogの読み込みが行われる
                    return LogLoader(Some(Log::from(snap.clone())));
                }
            }
            if let Some(rawlogs) = &self.rawlogs {
                // snapshot以降が存在する場合は、
                // mut methodであるskip_toを使ってshrinkし、その結果を返す。
                //
                // startが不適切であれば、unwrapに失敗する
                let mut rawlogs = rawlogs.clone();
                rawlogs.skip_to(start).unwrap();
                return LogLoader(Some(Log::from(rawlogs)));
            }

            // [start, None]できない
            panic!("[Load log failure] start = {:?}, end = {:?}", start, end);
        }

        // 明示的に終了位置の指定されている [start, end) の読み込みケース
        let end = end.unwrap();

        assert!(start <= end);

        if let Some(snap) = &self.snapshotted {
            // snapshotが存在する場合

            // 次のif文は
            // https://github.com/frugalos/frugalos/blob/bdb58d47ef50e5f7038bdce4b6cd31736260d6e8/frugalos_raft/src/storage/mod.rs#L120
            // を参考にしている
            if start == LogIndex::new(0) && end <= snap.tail.index {
                dbg!(&snap);
                // [0, snap.tail] を表す snapshotを全て取得したい場合
                return LogLoader(Some(Log::from(snap.clone())));
            }
        }
        if let Some(rawlogs) = &self.rawlogs {
            // rawlogsが存在し
            if let Ok(sliced) = rawlogs.slice(start, end) {
                dbg!(&sliced);
                // 要求する部分がrawlogをshrinkして得られる場合
                return LogLoader(Some(Log::from(sliced)));
            }
        }

        panic!("[Load log failure] start = {:?}, end = {:?}", start, end);
    }

    type Timeout = Invoker;
    fn create_timeout(&mut self, _role: Role) -> Self::Timeout {
        let (invoker, timer) = channel();
        self.invoker = Some(invoker);
        Invoker(timer)
    }
}

pub struct Invoker(Receiver<()>);

impl Future for Invoker {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let r = self.0.try_recv();
        if r.is_ok() {
            Ok(Async::Ready(()))
        } else {
            let err = r.err().unwrap();
            if err == TryRecvError::Empty {
                Ok(Async::NotReady)
            } else {
                panic!("disconnected");
            }
        }
    }
}

pub struct LogLoader(Option<Log>);
impl Future for LogLoader {
    type Item = Log;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if let Some(log) = self.0.take() {
            Ok(Async::Ready(log))
        } else {
            panic!("")
        }
    }
}

enum SaveMode {
    SnapshotSave,
    RawLogSave,
}
pub struct LogSaver(SaveMode, pub NodeId);
impl Future for LogSaver {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.0 {
            SaveMode::SnapshotSave => {
                println!("[Node {}] Save Snapshot", self.1.as_str())
            }
            SaveMode::RawLogSave => {
                println!("[Node {}] Save Raw Logs", self.1.as_str())
            }
        }
        Ok(Async::Ready(()))
    }
}

pub struct BallotSaver(pub NodeId, pub Ballot);
pub struct BallotLoader(pub NodeId, pub Option<Ballot>);

impl Future for BallotSaver {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!(
            "[Node {}] Save Ballot({})",
            self.0.as_str(),
            ballot_to_str(&self.1),
        );
        Ok(Async::Ready(()))
    }
}

impl Future for BallotLoader {
    type Item = Option<Ballot>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("[Node {}] Load Ballot({:?})", self.0.as_str(), self.1);
        Ok(Async::Ready(self.1.clone()))
    }
}
