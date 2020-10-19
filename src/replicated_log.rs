use futures::{Poll, Stream};
use prometrics::metrics::MetricBuilder;
use std::sync::Arc;
use trackable::error::ErrorKindExt;

use crate::cluster::{ClusterConfig, ClusterMembers};
use crate::election::{Ballot, Role};
use crate::io::Io;
use crate::log::{LogEntry, LogHistory, LogIndex, LogPosition, LogPrefix, ProposalId};
use crate::message::SequenceNumber;
use crate::metrics::RaftlogMetrics;
use crate::node::{Node, NodeId};
use crate::node_state::{NodeState, RoleState};
use crate::{Error, ErrorKind, Result};

/// Raftアルゴリズムに基づく分散複製ログ.
///
/// 利用者は`propose_command`メソッドを使って、コマンドをログに複製保存し、
/// 発生する`Event`をハンドリングすることで、
/// 整合性のある複製状態機械を実現することが可能となる.
///
/// `ReplicatedLog`は`Stream`トレイトを実装しているが、
/// これは無限ストリームであり、エラー時を除いて終了することはない.
///
/// ただし、構成変更によりノードがクラスタから切り離された場合は、
/// 最終的には、イベントが生成されることは無くなる.
/// `this.local_history().config().is_known_node()`メソッドを使うことで、
/// クラスタ内に属しているかどうかは判定可能なので、利用者側が明示的に確認して、
/// 不要になった`ReplicatedLog`インスタンスを回収することは可能.
pub struct ReplicatedLog<IO: Io> {
    node: NodeState<IO>,
    metrics: Arc<RaftlogMetrics>,
}
impl<IO: Io> ReplicatedLog<IO> {
    /// `members`で指定されたクラスタに属する`ReplicatedLog`のローカルインスタンス(ノード)を生成する.
    ///
    /// ローカルノードのIDは`node_id`で指定するが、これが`members`の含まれている必要は必ずしもない.
    /// 例えば、クラスタの構成変更に伴い、新規ノードを追加したい場合には、
    /// `members`に現行構成を指定することが望ましいが、このケースでは、
    /// `node_id`は`members`の中には含まれないことになる.
    ///
    /// なお、ノードの再起動時を除いて、`node_id`には対象クラスタの歴史の中でユニークなIDを
    /// 割り当てるのが望ましい.
    /// (レアケースではあるが、新規追加ノードを、以前に存在したノードと誤認識されてしまうと、
    /// 分散ログの整合性が壊れてしまう危険性があるため)
    ///
    /// また、以前のノードを再起動したい場合でも、もし永続ストレージが壊れている等の理由で、
    /// 前回の状態を正確に復元できないのであれば、
    /// ノード名を変更して、新規ノード追加扱いにした方が安全である.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        node_id: NodeId,
        members: ClusterMembers,
        io: IO,
        metric_builder: &MetricBuilder,
    ) -> Result<Self> {
        let config = ClusterConfig::new(members);
        let mut metric_builder = metric_builder.clone();
        metric_builder.namespace("raftlog");
        let metrics = track!(RaftlogMetrics::new(&metric_builder))?;
        let node = NodeState::load(node_id, config, io, metrics.node_state.clone());
        Ok(ReplicatedLog {
            node,
            metrics: Arc::new(metrics),
        })
    }

    /// `raftlog` のメトリクスを返す。
    pub fn metrics(&self) -> &Arc<RaftlogMetrics> {
        &self.metrics
    }

    /// 新しいコマンドを提案する.
    ///
    /// 提案が承認(コミット)された場合には、返り値の`LogPosition`を含む
    /// `Event::Committed`イベントが返される.
    ///
    /// もし返り値の`LogPosition`とは分岐した`Event::Committed`が返された場合には、
    /// この提案が棄却されたことを示している.
    ///
    /// # Errors
    ///
    /// 非リーダノードに対して、このメソッドが実行された場合には、
    /// `ErrorKind::NotLeader`を理由としたエラーが返される.
    pub fn propose_command(&mut self, command: Vec<u8>) -> Result<ProposalId> {
        if let RoleState::Leader(ref mut leader) = self.node.role {
            let term = self.node.common.term();
            let entry = LogEntry::Command { term, command };
            let proposal_id = leader.propose(&mut self.node.common, entry);
            Ok(proposal_id)
        } else {
            track_panic!(ErrorKind::NotLeader)
        }
    }

    /// 新しいクラスタ構成(新メンバ群)を提案する.
    ///
    /// 提案が承認(コミット)された場合には、返り値の`LogPosition`を含む
    /// `Event::Committed`イベントが返される.
    /// ただし、承認された場合であっても、それは新旧混合状態の構成が承認されただけであり、
    /// 新メンバのみの構成への移行完了を把握したい場合には、後続のコミットイベントの
    /// 追跡を行う必要がある.
    ///
    /// もし返り値の`LogPosition`とは分岐した`Event::Committed`が返された場合には、
    /// この提案が棄却されたことを示している.
    ///
    /// 複数の構成変更を並行して実施することは可能だが、
    /// その場合は、最後に提案されたものが最終的な構成として採用される.
    ///
    /// # Errors
    ///
    /// 非リーダノードに対して、このメソッドが実行された場合には、
    /// `ErrorKind::NotLeader`を理由としたエラーが返される.
    pub fn propose_config(&mut self, new_members: ClusterMembers) -> Result<ProposalId> {
        if let RoleState::Leader(ref mut leader) = self.node.role {
            let config = self.node.common.config().start_config_change(new_members);
            let term = self.node.common.term();
            let entry = LogEntry::Config { term, config };
            let proposal_id = leader.propose(&mut self.node.common, entry);
            Ok(proposal_id)
        } else {
            track_panic!(ErrorKind::NotLeader)
        }
    }

    /// 強制的にハートビートメッセージ(i.e., AppendEntriesCall)をブロードキャストする.
    ///
    /// 返り値は、送信メッセージのシーケンス番号.
    ///
    /// `last_heartbeat_ack`メソッドを用いることで、
    /// このハートビートに対して、過半数以上の応答を得られた
    /// タイミングを把握することが可能.
    ///
    /// また、リーダのコミットを即座にフォロワーに伝えたい場合にも、
    /// このメソッドが活用可能。
    /// (`Event::Committed`をリーダが生成した直後に`heartbeat`メソッドを呼び出せば良い)
    ///
    /// なおノードの役割が非リーダに変わった場合には、
    /// 応答待機中のハートビートは全て破棄されるので注意が必要.
    ///
    /// # Errors
    ///
    /// 非リーダノードに対して、このメソッドが実行された場合には、
    /// `ErrorKind::NotLeader`を理由としたエラーが返される.
    pub fn heartbeat(&mut self) -> Result<SequenceNumber> {
        if let RoleState::Leader(ref mut leader) = self.node.role {
            let seq_no = leader.heartbeat_syn(&mut self.node.common);
            Ok(seq_no)
        } else {
            track_panic!(ErrorKind::NotLeader);
        }
    }

    /// ローカルログにスナップショットをインストールする.
    ///
    /// `new_head`が新しいローカルログの先頭位置となり、
    /// `snapshot`はその地点までのコマンド群が適用済みの状態機械のスナップショット、となる.
    ///
    /// # Errors
    ///
    /// 既にローカルログに対するスナップショットのインストールが進行中の場合には、
    /// `ErrorKind::Busy`を理由としてエラーが返される.
    ///
    /// また現在のログの先頭よりも前の地点のスナップショットをインストールしようとした場合には、
    /// `ErrorKind::InvalidInput`を理由としたエラーが返される.
    pub fn install_snapshot(&mut self, new_head: LogIndex, snapshot: Vec<u8>) -> Result<()> {
        track_assert!(
            !self.node.is_loading(),
            ErrorKind::Busy,
            "Loading node state"
        );

        let (prev_term, config) = {
            let record = track!(
                self.node
                    .common
                    .log()
                    .get_record(new_head)
                    .ok_or_else(|| ErrorKind::InvalidInput.error()),
                "Too old log position: new_head={:?}, current_head={:?}, node={:?}",
                new_head,
                self.local_history().head(),
                self.local_node()
            )?;
            (record.head.prev_term, record.config.clone())
        };
        let prefix = LogPrefix {
            tail: LogPosition {
                prev_term,
                index: new_head,
            },
            config,
            snapshot,
        };
        track!(self.node.common.install_snapshot(prefix))?;
        Ok(())
    }

    /// 新しい選挙を開始する.
    ///
    /// 何らかの手段で現在のリーダのダウンを検知した場合に呼び出される.
    pub fn start_election(&mut self) {
        self.node.start_election();
    }

    /// ローカルノードの情報を返す.
    pub fn local_node(&self) -> &Node {
        self.node.common.local_node()
    }

    /// ローカルログの履歴を返す.
    pub fn local_history(&self) -> &LogHistory {
        self.node.common.log()
    }

    /// ローカルログへの書き込み待ちの状態の提案群の数を返す.
    ///
    /// この値は、ローカルストレージの詰まり具合を把握するために有用である.
    ///
    /// 「ローカルログへは追記完了 and コミット待ち」の個数は
    /// 知りたい場合には`local_history`メソッド経由で取得可能.
    ///
    /// ローカルノードが非リーダである場合には、常に`0`が返される.
    pub fn proposal_queue_len(&self) -> usize {
        if let RoleState::Leader(ref leader) = self.node.role {
            leader.proposal_queue_len(&self.node.common)
        } else {
            0
        }
    }

    /// スナップショットをインストール中の場合には`true`を返す.
    ///
    /// このメソッドが`true`を返している間は、
    /// 新しいスナップショットのインストールを行うことはできない.
    pub fn is_snapshot_installing(&self) -> bool {
        // 起動後のロードフェーズの間もスナップショットのインストールは行えないので、
        // その場合も`true`を返しておく.
        self.node.is_loading() || self.node.common.is_snapshot_installing()
    }

    /// 過半数以上の応答を得られた最新のハートビート(i.e., AppendEntriesCall) のシーケンス番号を返す.
    ///
    /// この値は、同じ選挙期間に関しては減少することはないことが保証されている.
    ///
    /// # 注意
    ///
    /// ハートビートを行うのはリーダノードのみなので、それ以外のノードに関しては、
    /// このメソッドが返す値は意味を持たない.
    pub fn last_heartbeat_ack(&self) -> SequenceNumber {
        if let RoleState::Leader(ref leader) = self.node.role {
            leader.last_heartbeat_ack()
        } else {
            SequenceNumber::new(0)
        }
    }

    /// 現在のクラスタ構成を返す.
    pub fn cluster_config(&self) -> &ClusterConfig {
        self.node.common.config()
    }

    /// I/O実装に対する参照を返す.
    pub fn io(&self) -> &IO {
        self.node.common.io()
    }

    /// I/O実装に対する破壊的な参照を返す.
    ///
    /// # Safety
    /// 破壊的な操作は、Raftの管理外の挙動となり、
    /// 整合性を崩してしまう可能性もあるので、
    /// 注意を喚起する意味で`unsafe`と設定されている.
    pub unsafe fn io_mut(&mut self) -> &mut IO {
        self.node.common.io_mut()
    }
}
impl<IO: Io> Stream for ReplicatedLog<IO> {
    type Item = Event;
    type Error = Error;
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        track!(self.node.poll(), "node={:?}", self.local_node())
    }
}

/// `ReplicatedLog`から発生するイベント一覧.
#[derive(Debug, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum Event {
    /// ローカルノードの役割が変わった.
    RoleChanged { new_role: Role },

    /// 新しい選挙期間に移った.
    TermChanged { new_ballot: Ballot },

    /// 新しいリーダーが選出された.
    NewLeaderElected,

    /// 新しいログエントリがコミットされた.
    ///
    /// エントリの内容がコマンドの場合には、
    /// `ReplicatedLog`の利用者は、自身が管理する状態機械に、
    /// `command`を適用する必要がある.
    ///
    /// ログエントリはインデックスの昇順でコミットされ,
    /// インデックスは常に一ずつ増加する.
    Committed { index: LogIndex, entry: LogEntry },

    /// スナップショットがロードされた.
    ///
    /// `ReplicatedLog`の利用者は、自身が管理する状態機械を、
    /// `snapshot`の状態にリセットする必要がある.
    SnapshotLoaded {
        new_head: LogPosition,
        snapshot: Vec<u8>,
    },

    /// スナップショットのインストールが行われた.
    ///
    /// もし`new_head`の位置が、最新のコミット済み地点よりも
    /// 新しい場合には、これとは別に`SnapshotLoaded`イベントが発行される.
    SnapshotInstalled { new_head: LogPosition },
}
