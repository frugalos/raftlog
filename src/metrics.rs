//! raftlog のメトリクス。

use prometrics::metrics::{Counter, Gauge, Histogram, HistogramBuilder, MetricBuilder};

use crate::{Error, Result};

/// `raftlog` 全体に関するメトリクス。
#[derive(Clone)]
pub struct RaftlogMetrics {
    pub(crate) node_state: NodeStateMetrics,
}
impl RaftlogMetrics {
    pub(crate) fn new(builder: &MetricBuilder) -> Result<Self> {
        let node_state = track!(NodeStateMetrics::new(builder))?;
        Ok(Self { node_state })
    }
}

/// ノード状態に関するメトリクス。
#[derive(Clone)]
pub struct NodeStateMetrics {
    pub(crate) transit_to_candidate_total: Counter,
    pub(crate) transit_to_follower_total: Counter,
    pub(crate) transit_to_leader_total: Counter,
    pub(crate) event_queue_len: Gauge,
    pub(crate) poll_timeout_total: Counter,
    pub(crate) candidate_to_leader_duration_seconds: Histogram,
    pub(crate) candidate_to_follower_duration_seconds: Histogram,
    pub(crate) loader_to_candidate_duration_seconds: Histogram,
}
impl NodeStateMetrics {
    pub(crate) fn new(builder: &MetricBuilder) -> Result<Self> {
        let mut builder: MetricBuilder = builder.clone();
        builder.subsystem("node_state");
        let transit_to_candidate_total = track!(builder
            .counter("transit_to_candidate_total")
            .help("Number of transitions to candidate role")
            .finish())?;
        let transit_to_follower_total = track!(builder
            .counter("transit_to_follower_total")
            .help("Number of transitions to follower role")
            .finish())?;
        let transit_to_leader_total = track!(builder
            .counter("transit_to_leader_total")
            .help("Number of transitions to leader role")
            .finish())?;
        let event_queue_len = track!(builder
            .gauge("event_queue_len")
            .help("Length of a raft event queue")
            .finish())?;
        let poll_timeout_total = track!(builder
            .counter("poll_timeout_total")
            .help("Number of timeout")
            .finish())?;
        let candidate_to_leader_duration_seconds = track!(make_role_change_histogram(
            builder
                .histogram("candidate_to_leader_duration_seconds")
                .help("Elapsed time moving from candidate to leader")
        ))?;
        let candidate_to_follower_duration_seconds = track!(make_role_change_histogram(
            builder
                .histogram("candidate_to_follower_duration_seconds")
                .help("Elapsed time moving from candidate to follower")
        ))?;
        let loader_to_candidate_duration_seconds = track!(make_role_change_histogram(
            builder
                .histogram("loader_to_candidate_duration_seconds")
                .help("Elapsed time moving from loader to candidate")
        ))?;
        Ok(Self {
            transit_to_candidate_total,
            transit_to_follower_total,
            transit_to_leader_total,
            event_queue_len,
            poll_timeout_total,
            candidate_to_leader_duration_seconds,
            candidate_to_follower_duration_seconds,
            loader_to_candidate_duration_seconds,
        })
    }
}

fn make_role_change_histogram(builder: &mut HistogramBuilder) -> Result<Histogram> {
    builder
        .bucket(0.001)
        .bucket(0.005)
        .bucket(0.01)
        .bucket(0.05)
        .bucket(0.1)
        .bucket(0.2)
        .bucket(0.4)
        .bucket(0.6)
        .bucket(0.8)
        .bucket(1.0)
        .bucket(2.0)
        .bucket(4.0)
        .bucket(6.0)
        .bucket(8.0)
        .bucket(10.0)
        .bucket(20.0)
        .bucket(50.0)
        .bucket(80.0)
        .bucket(320.0)
        .bucket(640.0)
        .finish()
        .map_err(|e| track!(Error::from(e)))
}
