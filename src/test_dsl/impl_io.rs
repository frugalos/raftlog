//! テスト用DSLで用いる `Io`-traitの実装 を提供するモジュール
//! 以下の点に留意されたい:
//! * これ以外の`Io`-traitの実装を妨げるものではない

use crate::election::{Ballot, Role};
use crate::log::{Log, LogIndex, LogPrefix, LogSuffix};
use crate::message::Message;
use crate::node::NodeId;
use crate::{Error, ErrorKind, Io, Result};
use futures::{Async, Future, Poll};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};

extern crate prometrics;

#[allow(dead_code)]
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

/// IOイベントを保存するための列挙体
pub enum IOEvent {
    /// データを受信した
    Recv,
    /// データを送信した
    Send,
    /// 表を保存した
    SaveBallot,
    /// 表を読み込んだ
    LoadBallot,
    /// スナップショットを保存した
    SaveSnapshot,
    /// ログのSuffixを保存した
    SaveSuffix,
    /// ログを読み込んだ
    LoadLog,
}

/// `Io`-traitを実装する構造体
pub struct TestIo {
    /// このIoを保持するnode
    node_id: NodeId,

    /// 他のノードからメッセージを受信口
    recv: Receiver<Message>,

    /// このノードにメッセージを送る際に、複製して使うことになる送信口
    send: Sender<Message>,

    /// raftcluster中の他のノードの送信口を管理するmap
    channels: BTreeMap<NodeId, Sender<Message>>,

    /// これまでに保存したballot全体
    ballots: Vec<Ballot>,

    snapshot: Option<LogPrefix>,
    rawlog: Option<LogSuffix>,

    /// このIoを保持するnodeをタイムアウトさせたい時に
    /// 利用するタイムアウトイベントの送信口
    invoker: Option<Sender<()>>,

    /// データ受信を拒否するために用いる識別リスト
    recv_ban_list: BTreeSet<String>,

    /// これまでに行ったIoイベントの履歴
    io_events: Vec<IOEvent>,

    /// デバッグログの出力有無
    debug: bool,
}

impl TestIo {
    /// `name`をノード名とするノードを受信拒否リストに追加
    pub fn add_recv_ban_list(&mut self, name: &str) {
        self.recv_ban_list.insert(name.to_string());
    }

    /// `name`をノード名とするノードを受信拒否リストから削除
    pub fn del_recv_ban_list(&mut self, name: &str) {
        self.recv_ban_list.remove(name);
    }

    /// 永続化したスナップショット(log prefix)の取得
    pub fn snapshot(&self) -> &Option<LogPrefix> {
        &self.snapshot
    }

    /// 永続化した生のログ（log suffix）の取得
    pub fn rawlog(&self) -> &Option<LogSuffix> {
        &self.rawlog
    }

    /// `node_id`をオーナーとして持つようにIo実体を作成
    pub fn new(node_id: NodeId, debug: bool) -> Self {
        let (send, recv) = channel();
        Self {
            node_id,
            recv,
            send,
            channels: Default::default(),
            ballots: Vec::new(),
            snapshot: None,
            rawlog: None,
            invoker: None,
            recv_ban_list: BTreeSet::new(),
            io_events: Vec::new(),
            debug,
        }
    }

    /// このIo実体にデータを送るためのチャネルを複製する
    pub fn copy_sender(&self) -> Sender<Message> {
        self.send.clone()
    }

    /// `chan`を受信口として持つ`node`にデータを送れるように準備する
    pub fn set_channel(&mut self, node: NodeId, chan: Sender<Message>) {
        self.channels.insert(node, chan);
    }

    /// このIoを持つノードを、raftlogのレイヤでタイムアウトさせる
    pub fn invoke_timer(&self) {
        if let Some(invoker) = &self.invoker {
            invoker
                .send(())
                .expect("Testではタイムアウトの発生は失敗しない");
        }
    }

    /// これまでに発生したIoイベントの履歴を返す
    pub fn io_events(&self) -> &[IOEvent] {
        &self.io_events
    }
}

impl Io for TestIo {
    fn try_recv_message(&mut self) -> Result<Option<Message>> {
        let r = self.recv.try_recv();
        match r {
            Ok(v) => {
                self.io_events.push(IOEvent::Recv);

                let who = &v.header().sender;

                if self.debug {
                    println!(
                        "\n[try_recv] {:?} <---- {:?}\n{}",
                        self.node_id,
                        who,
                        message_to_string(&v)
                    );
                }

                if self.recv_ban_list.contains(&who.clone().into_string()) {
                    return Ok(None);
                }
                Ok(Some(v))
            }
            Err(err) => {
                if err == TryRecvError::Empty {
                    Ok(None)
                } else {
                    panic!("disconnected");
                }
            }
        }
    }

    fn send_message(&mut self, message: Message) {
        self.io_events.push(IOEvent::Send);

        let dest: NodeId = message.header().destination.clone();

        if self.debug {
            println!(
                "\n[send] {:?} ----> {:?}\n{}",
                self.node_id,
                dest,
                message_to_string(&message)
            );
        }

        let channel = self
            .channels
            .get(&dest)
            .expect("Testでは既知の相手に送信するので失敗しない");
        channel
            .send(message)
            .expect("Testではレシーバはdropしないので成功しなくてはらない");
    }

    type SaveBallot = BallotSaver;
    fn save_ballot(&mut self, ballot: Ballot) -> Self::SaveBallot {
        self.io_events.push(IOEvent::SaveBallot);

        self.ballots.push(ballot.clone());
        BallotSaver(self.node_id.clone(), ballot)
    }

    type LoadBallot = BallotLoader;
    fn load_ballot(&mut self) -> Self::LoadBallot {
        self.io_events.push(IOEvent::LoadBallot);

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
        self.io_events.push(IOEvent::SaveSnapshot);

        if self.debug {
            println!("SAVE SNAPSHOT: {:?}", &prefix);
        }

        // 新しいsnapshotに含まれる領域はsuffixから削除する
        // この挙動はfrugalos_raftを参照にしている
        // https://github.com/frugalos/frugalos/blob/bdb58d47ef50e5f7038bdce4b6cd31736260d6e8/frugalos_raft/src/storage/log_prefix/save.rs#L98
        //
        // ただし、IO traitの制約
        // https://github.com/frugalos/raftlog/blob/087d8019b42a05dbc5b463db076041d45ad2db7f/src/io.rs#L64
        // としては、削除要請はない。
        if let Some(rawlog) = &mut self.rawlog {
            // prefixは手持ちのrawlogの先頭部分を被覆している
            assert!(rawlog.head.index <= prefix.tail.index);
            // 被覆されている部分をskipする
            assert!(rawlog.skip_to(prefix.tail.index).is_ok());
        } else {
            unreachable!("the argument `prefix` contains invalid entries");
        }

        // 既にsnapshotがある場合は `prefix` はそれを拡張するものである
        if let Some(snap) = &self.snapshot {
            assert!(prefix.tail.is_newer_or_equal_than(snap.tail));
        }
        self.snapshot = Some(prefix);

        LogSaver(SaveMode::SnapshotSave, self.node_id.clone(), 2)
    }
    fn save_log_suffix(&mut self, suffix: &LogSuffix) -> Self::SaveLog {
        self.io_events.push(IOEvent::SaveSuffix);

        if self.debug {
            println!("SAVE RAWLOG: {:?}", &suffix);
        }

        if let Some(rawlog) = &mut self.rawlog {
            assert!(suffix.head.is_newer_or_equal_than(rawlog.head));
            over_write(rawlog, suffix);
        } else {
            // 空のdiskへの書き込みに対応する
            self.rawlog = Some(suffix.clone());
        }

        LogSaver(SaveMode::RawLogSave, self.node_id.clone(), 2)
    }

    type LoadLog = LogLoader;
    fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog {
        self.io_events.push(IOEvent::LoadLog);

        if self.debug {
            println!("LOAD LOG {:?} {:?}", &start, &end);
        }

        LogLoader {
            start,
            end,
            snapshot: self.snapshot.clone(),
            rawlog: self.rawlog.clone(),
            debug: self.debug,
        }
    }

    type Timeout = Invoker;
    fn create_timeout(&mut self, _role: Role) -> Self::Timeout {
        let (invoker, timer) = channel();
        self.invoker = Some(invoker);
        Invoker(timer)
    }
}

/// タイムアウトを表現するためのfuture（の実体）
pub struct Invoker(Receiver<()>);

impl Future for Invoker {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let r = self.0.try_recv();
        match r {
            Ok(_) => Ok(Async::Ready(())),
            Err(err) => {
                if err == TryRecvError::Empty {
                    Ok(Async::NotReady)
                } else {
                    panic!("disconnected");
                }
            }
        }
    }
}

/// ログ読み込みを表現するためのfuture（の実体）
pub struct LogLoader {
    start: LogIndex,
    end: Option<LogIndex>,
    snapshot: Option<LogPrefix>,
    rawlog: Option<LogSuffix>,
    debug: bool,
}
impl Future for LogLoader {
    type Item = Log;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let start = self.start;
        let end = self.end;

        if self.snapshot.is_none() && self.rawlog.is_none() {
            if start == LogIndex::new(0) && end == None {
                // 無に対するロードなら無を作る
                let log = LogSuffix::default(); // 何もできないのでdefaultを使う
                return Ok(Async::Ready(Log::from(log)));
            }
            track_panic!(
                ErrorKind::InvalidInput,
                "Try load from None: start = {:?}, end = {:?}",
                start,
                end
            );
        }

        if end == None {
            // Case: startから全て [start..) 読み込む

            if let Some(snap) = &self.snapshot {
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
                    return Ok(Async::Ready(Log::from(snap.clone())));
                }
            }
            if let Some(rawlog) = &self.rawlog {
                // snapshot以降が存在する場合は、
                // mut methodであるskip_toを使ってshrinkし、その結果を返す。
                //
                // startが不適切であればskip_toでエラーになる
                let mut rawlog = rawlog.clone();
                rawlog.skip_to(start)?;
                return Ok(Async::Ready(Log::from(rawlog)));
            }

            track_panic!(
                ErrorKind::InvalidInput,
                "[Load log failure] start = {:?}, end = {:?}",
                start,
                end
            );
        }

        // 明示的に終了位置の指定されている [start, end) の読み込みケース
        let end = end.expect("this never fails because end != None");

        track_assert!(start <= end, ErrorKind::InvalidInput);

        if let Some(snap) = &self.snapshot {
            // snapshotが存在する場合

            // 次のif文は
            // https://github.com/frugalos/frugalos/blob/bdb58d47ef50e5f7038bdce4b6cd31736260d6e8/frugalos_raft/src/storage/mod.rs#L120
            // を参考にしている
            if end <= snap.tail.index {
                if self.debug {
                    println!("load snapshot {:?}", &snap);
                }
                // [0, snap.tail] を表す snapshotを全て取得したい場合
                return Ok(Async::Ready(Log::from(snap.clone())));
            }
        }
        if let Some(rawlog) = &self.rawlog {
            // rawlogが存在し
            if let Ok(sliced) = rawlog.slice(start, end) {
                if self.debug {
                    println!("load rawlog {:?}", &sliced);
                }
                // 要求する部分がrawlogをshrinkして得られる場合
                return Ok(Async::Ready(Log::from(sliced)));
            }
        }

        track_panic!(
            ErrorKind::InvalidInput,
            "[Load log failure] start = {:?}, end = {:?}",
            start,
            end
        );
    }
}

enum SaveMode {
    SnapshotSave,
    RawLogSave,
}

/// ログ保存を表現するためのfuture（の実体）
pub struct LogSaver(SaveMode, pub NodeId, u8);
impl Future for LogSaver {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.2 == 0 {
            Ok(Async::Ready(()))
        } else {
            self.2 -= 1;
            Ok(Async::NotReady)
        }
    }
}

/// 票の保存を表現するためのfuture（の実体）
pub struct BallotSaver(pub NodeId, pub Ballot);

/// 票の読み込みを表現するためのfuture（の実体）
pub struct BallotLoader(pub NodeId, pub Option<Ballot>);

impl Future for BallotSaver {
    type Item = ();
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(()))
    }
}

impl Future for BallotLoader {
    type Item = Option<Ballot>;
    type Error = Error;
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        Ok(Async::Ready(self.1.clone()))
    }
}
