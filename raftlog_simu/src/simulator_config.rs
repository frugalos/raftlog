use raftlog::cluster::ClusterMembers;
use raftlog::node::NodeId;
use rand::{Rng, SeedableRng, StdRng};
use std::collections::BTreeSet;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::io::IoConfig;
use crate::types::{LogicalDuration, Probability, Range, SharedRng};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulatorConfig {
    /// シミュレータで使われる乱数のシード.
    ///
    /// この値によって、シミュレータ上で発生するイベントが全て決定される.
    #[serde(default = "SimulatorConfig::default_seed")]
    pub seed: u64,

    #[serde(default = "SimulatorConfig::default_loop_count")]
    pub loop_count: usize,

    #[serde(default = "SimulatorConfig::default_nodes")]
    pub nodes: BTreeSet<String>,

    #[serde(default = "SimulatorConfig::default_propose_command")]
    pub propose_command: Probability,

    #[serde(default = "SimulatorConfig::default_manual_heartbeat")]
    pub manual_heartbeat: Probability,

    #[serde(default = "SimulatorConfig::default_take_snapshot")]
    pub take_snapshot: Probability,

    #[serde(default = "SimulatorConfig::default_node_down")]
    pub node_down: Probability,

    #[serde(default = "SimulatorConfig::default_node_restart_interval")]
    pub node_restart_interval: Range<LogicalDuration>,

    #[serde(default = "SimulatorConfig::default_change_cluster")]
    pub change_cluster: Probability,

    #[serde(default = "SimulatorConfig::default_cluster_size")]
    pub cluster_size: Range<usize>,

    #[serde(default)]
    pub io: IoConfig,
}
impl SimulatorConfig {
    /// 現在のUNIXタイムスタンプをマイクロ秒単位で返す.
    pub fn default_seed() -> u64 {
        let unixtime = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Never fails");
        unixtime.as_secs() * 1_000_000 + u64::from(unixtime.subsec_nanos()) / 1000
    }

    /// `10000`
    pub fn default_loop_count() -> usize {
        10000
    }

    /// `0.001`
    pub fn default_propose_command() -> Probability {
        Probability { prob: 0.001 }
    }

    /// `0.0001`
    pub fn default_manual_heartbeat() -> Probability {
        Probability { prob: 0.0001 }
    }

    /// `0.0001`
    pub fn default_take_snapshot() -> Probability {
        Probability { prob: 0.0001 }
    }

    /// `0.00001`
    pub fn default_node_down() -> Probability {
        Probability { prob: 0.00001 }
    }

    /// `500...50000`
    pub fn default_node_restart_interval() -> Range<LogicalDuration> {
        Range {
            min: 500,
            max: 50000,
        }
    }

    /// `1...8`
    pub fn default_cluster_size() -> Range<usize> {
        Range { min: 1, max: 8 }
    }

    /// `0.00001`
    pub fn default_change_cluster() -> Probability {
        Probability { prob: 0.00001 }
    }

    /// `["foo", "bar", "baz", "qux", "quux", "corge", "grault"]`
    pub fn default_nodes() -> BTreeSet<String> {
        ["foo", "bar", "baz", "qux", "quux", "corge", "grault"]
            .iter()
            .map(|n| n.to_string())
            .collect()
    }

    pub fn make_rng(&self) -> SharedRng {
        let mut seed = [0; 32];
        for (i, v) in seed.iter_mut().enumerate().take(8) {
            *v = (self.seed >> (i * 8)) as u8;
        }
        let inner = StdRng::from_seed(seed);
        SharedRng::new(inner)
    }
    pub fn choose_members<R: Rng>(&self, rng: &mut R) -> ClusterMembers {
        let mut candidates = self
            .nodes
            .iter()
            .cloned()
            .map(NodeId::new)
            .collect::<Vec<_>>();
        rng.shuffle(&mut candidates[..]);
        candidates.truncate(self.cluster_size.choose(rng));
        candidates.into_iter().collect()
    }
}
impl Default for SimulatorConfig {
    fn default() -> Self {
        SimulatorConfig {
            seed: SimulatorConfig::default_seed(),
            loop_count: SimulatorConfig::default_loop_count(),
            propose_command: SimulatorConfig::default_propose_command(),
            manual_heartbeat: SimulatorConfig::default_manual_heartbeat(),
            take_snapshot: SimulatorConfig::default_take_snapshot(),
            node_down: SimulatorConfig::default_node_down(),
            node_restart_interval: SimulatorConfig::default_node_restart_interval(),
            cluster_size: SimulatorConfig::default_cluster_size(),
            change_cluster: SimulatorConfig::default_change_cluster(),
            io: IoConfig::default(),
            nodes: SimulatorConfig::default_nodes(),
        }
    }
}
