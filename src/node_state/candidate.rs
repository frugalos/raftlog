use futures::{Async, Future};
use std::collections::HashSet;

use super::{Common, NextState};
use crate::election::Role;
use crate::message::{Message, RequestVoteReply};
use crate::node::NodeId;
use crate::{Io, Result};

/// 選挙の立候補者.
///
/// 以下を行う:
///
/// - 1. 自分に投票 (投票状況はストレージに保存)
/// - 2. 投票依頼をブロードキャスト
/// - 3-a. 過半数から投票を得られたら、リーダに遷移
/// - 3-b. タイムアウトに達したら、次の選挙を開始して再び立候補
pub struct Candidate<IO: Io> {
    followers: HashSet<NodeId>,
    init: Option<IO::SaveBallot>,
}
impl<IO: Io> Candidate<IO> {
    pub fn new(common: &mut Common<IO>) -> Self {
        common.set_timeout(Role::Candidate);
        let future = common.save_ballot();
        Candidate {
            init: Some(future),
            followers: HashSet::new(),
        }
    }
    pub fn handle_timeout(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        Ok(Some(common.transit_to_candidate()))
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: &Message,
    ) -> Result<NextState<IO>> {
        if let Message::RequestVoteReply(RequestVoteReply { voted: true, .. }) = message {
            self.followers.insert(message.header().sender.clone());
            let is_elected = common
                .config()
                .consensus_value(|n| self.followers.contains(n));
            if is_elected {
                return Ok(Some(common.transit_to_leader()));
            }
        }
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        if let Async::Ready(Some(())) = track!(self.init.poll())? {
            self.init = None;
            common.rpc_caller().broadcast_request_vote();
        }
        Ok(None)
    }
}
