use raftlog::election::Role;
use rand::Rng;

use crate::io::configs::TimerConfig;
use crate::io::futures::{DelayedResult, Timeout};
use crate::types::SharedRng;

/// シミュレータ用のタイマー実装.
///
/// 生成時に渡された乱数生成器と構成をもとに、
/// 決定論的にタイムアウト時間(列)を生成可能.
#[derive(Clone)]
pub struct Timer {
    config: TimerConfig,
    rng: SharedRng,
}
impl Timer {
    /// 新しい`Timer`インスタンスを生成する.
    pub fn new(config: TimerConfig, rng: SharedRng) -> Self {
        Timer { config, rng }
    }

    /// 指定されたロール用のタイムアウトオブジェクトを生成して返す.
    pub fn create_timeout(&mut self, role: Role) -> Timeout {
        let timeout = match role {
            Role::Follower => self.config.election_timeout,
            Role::Candidate => self
                .rng
                .gen_range(self.config.heartbeat_interval, self.config.election_timeout),
            Role::Leader => self.config.heartbeat_interval,
        };
        DelayedResult::ok((), timeout)
    }
}
