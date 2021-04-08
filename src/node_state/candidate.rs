use futures::future::OptionFuture;
use futures::Future;
use std::collections::HashSet;
use std::pin::Pin;
use std::task::{Context, Poll};

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
    init: Option<Pin<Box<IO::SaveBallot>>>,
}
impl<IO: Io> Candidate<IO> {
    pub fn new(common: &mut Common<IO>, cx: &mut Context) -> Self {
        common.set_timeout(Role::Candidate);
        let future = Box::pin(common.save_ballot(cx));
        Candidate {
            init: Some(future),
            followers: HashSet::new(),
        }
    }
    pub fn handle_timeout(
        &mut self,
        common: &mut Common<IO>,
        cx: &mut Context,
    ) -> Result<NextState<IO>> {
        Ok(Some(common.transit_to_candidate(cx)))
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: &Message,
        cx: &mut Context,
    ) -> Result<NextState<IO>> {
        if let Message::RequestVoteReply(RequestVoteReply { voted: true, .. }) = message {
            self.followers.insert(message.header().sender.clone());
            let is_elected = common
                .config()
                .consensus_value(|n| self.followers.contains(n));
            if is_elected {
                return Ok(Some(common.transit_to_leader(cx)));
            }
        }
        Ok(None)
    }
    pub fn run_once(
        &mut self,
        common: &mut Common<IO>,
        cx: &mut Context<'_>,
    ) -> Result<NextState<IO>> {
        let mut init: OptionFuture<_> = self.init.as_mut().into();
        if let Poll::Ready(Some(result)) = track!(Pin::new(&mut init).poll(cx)) {
            let _ = track!(result)?;
            self.init = None;
            common.rpc_caller().broadcast_request_vote();
        }
        Ok(None)
    }
}
