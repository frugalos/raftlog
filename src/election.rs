//! リーダ選出関連の構成要素群.
use crate::node::NodeId;

/// ある選挙ないしリーダの任期期間を識別するための番号.
///
/// 番号の値は`0`から始まり「新しい選挙に立候補する」ないし「新しいリーダが選出される」タイミングで、
/// 増加している.
/// なお、この番号は一つのクラスタにおいて常に増加していき、減少することはない.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Term(u64);
impl Term {
    /// 値が`number`となる`Term`インスタンスを生成する.
    pub fn new(number: u64) -> Self {
        Term(number)
    }

    /// このインスタンスの期間番号の値を返す.
    pub fn as_u64(self) -> u64 {
        self.0
    }
}
impl From<u64> for Term {
    fn from(f: u64) -> Self {
        Term::new(f)
    }
}

/// 選挙でのノードの投票内容.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ballot {
    /// どの選挙での投票かを識別するための期間番号.
    pub term: Term,

    /// 投票先.
    pub voted_for: NodeId,
}

/// 選挙におけるノードの役割.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// 他の候補者(or リード)に投票済み.
    Follower,

    /// 選挙の立候補者.
    Candidate,

    /// 過半数以上の投票を集めて選出されたリーダ.
    Leader,
}
