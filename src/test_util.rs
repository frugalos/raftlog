//! テスト用のユーティリティ群。
#[cfg(test)]
pub mod tests {
    use fibers::time::timer;
    use futures::{Async, Future, Poll};
    use std::collections::{BTreeSet, HashMap};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use trackable::error::ErrorKindExt;

    use crate::cluster::{ClusterConfig, ClusterMembers};
    use crate::election::{Ballot, Role};
    use crate::io::Io;
    use crate::log::{Log, LogIndex, LogPrefix, LogSuffix};
    use crate::message::Message;
    use crate::node::NodeId;
    use crate::{Error, ErrorKind, Result};

    type Logs = Arc<Mutex<HashMap<(LogIndex, Option<LogIndex>), Log>>>;

    /// `TestIo`を生成する。主にクラスタ構成をするために存在する。
    /// `Log` や `Ballot` の設定は直接 `TestIo` に対して行えばよい。
    #[derive(Debug)]
    pub struct TestIoBuilder {
        members: ClusterMembers,
    }

    impl TestIoBuilder {
        pub fn new() -> Self {
            Self {
                members: BTreeSet::new(),
            }
        }

        pub fn add_member(mut self, node_id: NodeId) -> Self {
            self.members.insert(node_id);
            self
        }

        pub fn finish(&self) -> TestIo {
            TestIo {
                leader_timeout: Duration::from_millis(5),
                follower_timeout: Duration::from_millis(10),
                candidate_timeout: Duration::from_millis(15),
                cluster: ClusterConfig::new(self.members.clone()),
                ballots: Arc::new(Mutex::new(Vec::new())),
                logs: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    /// `TestIo` を操作するためのハンドル。
    #[derive(Clone)]
    pub struct TestIoHandle {
        pub cluster: ClusterConfig,
        logs: Logs,
    }

    impl TestIoHandle {
        /// 最初にロードされる `LogPrefix` をセットする。
        pub fn set_initial_log_prefix(&mut self, prefix: LogPrefix) {
            let mut logs = self.logs.lock().expect("Never fails");
            logs.insert((LogIndex::new(0), None), Log::Prefix(prefix));
        }

        /// 最初にロードされる `LogSuffix` をセットする。
        ///  `set_initial_log_prefix` を使った場合は suffix もセットしないと整合性が崩れるので注意。
        pub fn set_initial_log_suffix(&mut self, start: LogIndex, suffix: LogSuffix) {
            let mut logs = self.logs.lock().expect("Never fails");
            logs.insert((start, None), Log::Suffix(suffix));
        }

        /// ログを追加する。2回目以降のログ読み込みを想定しているため end は常に指定する。
        #[allow(dead_code)]
        pub fn append_log(&mut self, start: LogIndex, end: LogIndex, log: Log) {
            let mut logs = self.logs.lock().expect("Never fails");
            logs.insert((start, Some(end)), log);
        }
    }

    /// テスト用の `Io` 実装。
    /// テストシナリオが多岐に渡ることが想定されるため、この struct では最小限の機能のみ提供する。
    /// 各 field の整合性は担保しないため、テストコード側で担保すること。
    #[derive(Debug)]
    pub struct TestIo {
        pub leader_timeout: Duration,
        pub follower_timeout: Duration,
        pub candidate_timeout: Duration,
        /// クラスタ構成。
        pub cluster: ClusterConfig,
        /// `LoadBallot` でロードされる。
        pub ballots: Arc<Mutex<Vec<Ballot>>>,
        /// `LoadLog` でロードされる。
        pub logs: Logs,
    }

    impl TestIo {
        pub fn handle(&self) -> TestIoHandle {
            TestIoHandle {
                cluster: self.cluster.clone(),
                logs: self.logs.clone(),
            }
        }
    }

    impl Io for TestIo {
        type SaveBallot = NoopSaveBallot;
        type LoadBallot = LoadBallotImpl;
        type SaveLog = NoopSaveLog;
        type LoadLog = LoadLogImpl;
        type Timeout = FibersTimeout;

        fn try_recv_message(&mut self) -> Result<Option<Message>> {
            Ok(None)
        }

        fn send_message(&mut self, _message: Message) {}

        fn save_ballot(&mut self, _ballot: Ballot) -> Self::SaveBallot {
            NoopSaveBallot
        }

        fn load_ballot(&mut self) -> Self::LoadBallot {
            let mut ballots = self.ballots.lock().expect("Never fails");
            LoadBallotImpl(ballots.pop())
        }

        fn save_log_prefix(&mut self, _prefix: LogPrefix) -> Self::SaveLog {
            NoopSaveLog
        }

        fn save_log_suffix(&mut self, _suffix: &LogSuffix) -> Self::SaveLog {
            NoopSaveLog
        }

        fn load_log(&mut self, start: LogIndex, end: Option<LogIndex>) -> Self::LoadLog {
            let mut logs = self.logs.lock().expect("Never fails");
            if let Some(log) = logs.remove(&(start, end)) {
                match log {
                    Log::Prefix(prefix) => {
                        return LoadLogImpl {
                            prefix: Some(prefix),
                            suffix: None,
                        };
                    }
                    Log::Suffix(suffix) => {
                        return LoadLogImpl {
                            prefix: None,
                            suffix: Some(suffix),
                        };
                    }
                }
            }
            LoadLogImpl {
                prefix: None,
                suffix: Some(LogSuffix::default()),
            }
        }

        fn create_timeout(&mut self, role: Role) -> Self::Timeout {
            match role {
                Role::Leader => FibersTimeout(timer::timeout(self.leader_timeout)),
                Role::Follower => FibersTimeout(timer::timeout(self.follower_timeout)),
                Role::Candidate => FibersTimeout(timer::timeout(self.candidate_timeout)),
            }
        }
    }

    /// 現時点では必要ないので何もしない。
    #[derive(Debug)]
    pub struct NoopSaveBallot;
    impl Future for NoopSaveBallot {
        type Item = ();
        type Error = Error;
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            Ok(Async::Ready(()))
        }
    }

    /// 引数で与えられた `Ballot` を返す `LoadBallot` 実装。
    #[derive(Debug)]
    pub struct LoadBallotImpl(Option<Ballot>);
    impl Future for LoadBallotImpl {
        type Item = Option<Ballot>;
        type Error = Error;
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            Ok(Async::Ready(self.0.clone()))
        }
    }

    /// 現時点では必要ないので何もしない。
    #[derive(Debug)]
    pub struct NoopSaveLog;
    impl Future for NoopSaveLog {
        type Item = ();
        type Error = Error;
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            Ok(Async::Ready(()))
        }
    }

    /// `LogPrefix` か `LogSuffix` のどちらかをロードする `LoadLog` 実装。
    #[derive(Debug)]
    pub struct LoadLogImpl {
        prefix: Option<LogPrefix>,
        suffix: Option<LogSuffix>,
    }
    impl Future for LoadLogImpl {
        type Item = Log;
        type Error = Error;
        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            if let Some(prefix) = self.prefix.clone() {
                return Ok(Async::Ready(Log::Prefix(prefix)));
            }
            if let Some(suffix) = self.suffix.clone() {
                return Ok(Async::Ready(Log::Suffix(suffix)));
            }

            Err(ErrorKind::InconsistentState
                .cause("Neither prefix or suffix is not given")
                .into())
        }
    }

    /// fibers を使ったタイムアウトの実装。
    #[derive(Debug)]
    pub struct FibersTimeout(timer::Timeout);
    impl Future for FibersTimeout {
        type Item = ();
        type Error = Error;

        fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
            self.0
                .poll()
                .map_err(|_| ErrorKind::Other.cause("Broken timer").into())
        }
    }
}
