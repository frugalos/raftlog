//! クラスタ構成関連.
//!
//! なお、クラスタ構成の動的変更に関する詳細は、
//! [Raftの論文](https://raft.github.io/raft.pdf)の「6 Cluster membership changes」を参照のこと.
use std::cmp;
use std::collections::BTreeSet;

use crate::node::NodeId;

/// クラスタに属するメンバ群.
pub type ClusterMembers = BTreeSet<NodeId>;

/// クラスタの状態.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// 構成変更中ではなく安定している状態.
    Stable,

    /// 構成変更中で、新メンバ群のログを同期している状態.
    ///
    /// この状態では、リーダ選出やログのコミットにおいて、投票権を有するのは旧メンバのみである.
    CatchUp,

    /// 構成変更中で、新旧メンバ群の両方に合意が必要な状態.
    Joint,
}
impl ClusterState {
    /// 安定状態かどうかを判定する.
    pub fn is_stable(self) -> bool {
        self == ClusterState::Stable
    }

    /// 新旧混合状態かどうかを判定する.
    pub fn is_joint(self) -> bool {
        self == ClusterState::Joint
    }
}

/// クラスタ構成.
///
/// クラスタに属するメンバの集合に加えて、
/// 動的構成変更用の状態を管理する.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    new: ClusterMembers,
    old: ClusterMembers,
    state: ClusterState,
}
impl ClusterConfig {
    /// 現在のクラスタ状態を返す.
    pub fn state(&self) -> ClusterState {
        self.state
    }

    /// 構成変更後のメンバ集合が返される.
    ///
    /// 安定状態では、現在のメンバ群が返される
    /// (i.e, `members`メソッドが返すメンバ群と等しい).
    pub fn new_members(&self) -> &ClusterMembers {
        &self.new
    }

    /// 構成変更前のメンバ集合が返される.
    ///
    /// 安定状態では、空集合が返される.
    pub fn old_members(&self) -> &ClusterMembers {
        &self.old
    }

    /// プライマリなメンバ集合が返される.
    ///
    /// "プライマリな集合"とは、それに属するメンバーの過半数以上の合意が得られれば、
    /// クラスタ全体の分散ログの整合性が崩れるようなことがないような
    /// 集合をことを指す.
    ///
    /// これは、安定状態では「クラスタのメンバ群」と一致し、
    /// 構成変更時には「旧構成に属するメンバ群」となる.
    pub fn primary_members(&self) -> &ClusterMembers {
        match self.state {
            ClusterState::Stable => &self.new,
            ClusterState::CatchUp => &self.old,
            ClusterState::Joint => &self.old,
        }
    }

    /// クラスタに属するメンバ群を返す.
    ///
    /// 構成変更中の場合には、新旧両方のメンバの和集合が返される.
    pub fn members(&self) -> impl Iterator<Item = &NodeId> {
        self.new.union(&self.old)
    }

    /// このクラスタ構成に含まれるノードかどうかを判定する.
    pub fn is_known_node(&self, node: &NodeId) -> bool {
        self.new.contains(node) || self.old.contains(node)
    }

    /// 新しい安定状態の`ClusterConfig`インスタンスを生成する.
    pub fn new(members: ClusterMembers) -> Self {
        ClusterConfig {
            new: members,
            old: ClusterMembers::default(),
            state: ClusterState::Stable,
        }
    }

    /// 構成変更中の`ClusterConfig`インスタンスを生成する.
    pub fn with_state(
        new_members: ClusterMembers,
        old_members: ClusterMembers,
        state: ClusterState,
    ) -> Self {
        ClusterConfig {
            new: new_members,
            old: old_members,
            state,
        }
    }

    /// 構成変更を開始するために、`new`を構成変更後のメンバ群として設定し、
    /// `CatchUp`状態に遷移した`ClusterConfig`インスタンスを返す.
    pub(crate) fn start_config_change(&self, new: ClusterMembers) -> Self {
        ClusterConfig {
            new,
            old: self.primary_members().clone(),
            state: ClusterState::CatchUp,
        }
    }

    /// 次の状態に遷移する.
    ///
    /// # 状態遷移表
    ///
    /// - `Stable` => `Stable`
    /// - `CatchUp` => `Joint`
    /// - `Joint` => `Stable`
    pub(crate) fn to_next_state(&self) -> Self {
        match self.state {
            ClusterState::Stable => self.clone(),
            ClusterState::CatchUp => {
                let mut next = self.clone();
                next.state = ClusterState::Joint;
                next
            }
            ClusterState::Joint => {
                let mut next = self.clone();
                next.old = ClusterMembers::new();
                next.state = ClusterState::Stable;
                next
            }
        }
    }

    /// 現在の構成での最新の合意値を返す.
    //
    /// `f`は、各メンバの現在の承認値を返す関数.
    /// あるメンバが、仮にXという値を返したとして場合、
    /// それよりも小さな任意の値yに関しても、
    /// 承認済みのものとして扱われる.
    ///
    /// 最終的な合意値は「メンバの過半数が承認した値集合の中で
    /// 最も大きな値」となる.
    pub(crate) fn consensus_value<F, T>(&self, f: F) -> T
    where
        F: Fn(&NodeId) -> T,
        T: Ord + Copy + Default,
    {
        match self.state {
            ClusterState::Stable => median(&self.new, &f),
            ClusterState::CatchUp => median(&self.old, &f),
            ClusterState::Joint => {
                // joint consensus
                cmp::min(median(&self.new, &f), median(&self.old, &f))
            }
        }
    }

    /// 基本的には`consensus_value`メソッドと同様.
    ///
    /// ただし構成変更中には、常に新旧メンバ群の両方から、
    /// 過半数の承認を要求するところが異なる.
    pub(crate) fn full_consensus_value<F, T>(&self, f: F) -> T
    where
        F: Fn(&NodeId) -> T,
        T: Ord + Copy + Default,
    {
        if self.state.is_stable() {
            median(&self.new, &f)
        } else {
            // joint consensus
            cmp::min(median(&self.new, &f), median(&self.old, &f))
        }
    }
}

fn median<F, T>(members: &ClusterMembers, f: F) -> T
where
    F: Fn(&NodeId) -> T,
    T: Ord + Copy + Default,
{
    let mut values = members.iter().map(|n| f(n)).collect::<Vec<_>>();
    values.sort();
    values.reverse();
    if values.is_empty() {
        T::default()
    } else {
        values[members.len() / 2]
    }
}
