//! ノード関連の構成要素.
use crate::election::{Ballot, Role};

/// ノードのID.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(String);
impl NodeId {
    /// 新しい`NodeId`インスタンスを生成する.
    pub fn new<T: Into<String>>(id: T) -> Self {
        NodeId(id.into())
    }

    /// IDに対応する文字列を返す.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// IDの所有権を放棄して、対応する文字列を返す.
    pub fn into_string(self) -> String {
        self.0
    }
}
impl From<String> for NodeId {
    fn from(f: String) -> Self {
        NodeId::new(f)
    }
}
impl<'a> From<&'a str> for NodeId {
    fn from(f: &'a str) -> Self {
        NodeId::new(f)
    }
}
impl From<NodeId> for String {
    fn from(f: NodeId) -> Self {
        f.into_string()
    }
}

/// ノードの各種情報を保持するためのデータ構造.
#[derive(Debug, Clone)]
pub struct Node {
    /// ノードのID.
    pub id: NodeId,

    /// 現在の選挙におけるノードの役割.
    pub role: Role,

    /// 現在の選挙における投票状況.
    pub ballot: Ballot,
}
impl Node {
    // 初期状態の`Node`インスタンスを生成する.
    //
    // # Examples
    //
    // ```
    // use raftlog::election::Role;
    // use raftlog::node::Node;
    //
    // let n = Node::new("foo".into());
    // assert_eq!(n.id.as_str(), "foo");
    // assert_eq!(n.role, Role::Follower);
    // assert_eq!(n.ballot.term.as_u64(), 0);
    // assert_eq!(n.ballot.voted_for.as_str(), "foo");
    // ```
    pub(crate) fn new(node_id: NodeId) -> Self {
        Node {
            id: node_id.clone(),
            role: Role::Follower,
            ballot: Ballot {
                term: 0.into(),
                voted_for: node_id,
            },
        }
    }
}
