//! リーダ選出関連の構成要素群.
use node::NodeId;

/// ある選挙ないしリーダの任期期間を識別するための番号.
///
/// 以下の性質を持つ：
/// - Term値は`0`始まりである
/// - 単調増加である。値の増加は次の二種類のみ:
///     - 「新しい選挙に立候補する」（Follower -> Candidateへ遷移する）ときにインクリメント(+1)する
///     - RPCによって相手から受け取ったTermが、自分のTermを上回っている場合は、相手のTermへ更新する
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
/// 「選挙における」の意味が良く分かっていないが、各エントリはもう少し細かく書きたい。
/// 例えばFollowerの「投票済み」というのは、requestが来て投票済みなのかどうかとか
/// そもそもserver stateではなくroleという別の用語が割り当てられている理由が説明されるべき。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// 他の候補者(or リーダ)に投票済み.
    Follower,

    /// 選挙の立候補者.
    Candidate,

    /// 過半数以上の投票を集めて選出されたリーダ.
    Leader,
}
