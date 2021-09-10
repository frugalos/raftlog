use futures::{Async, Future};

use super::super::{Common, NextState, RoleState};
use super::{Follower, FollowerIdle};
use crate::log::LogPosition;
use crate::message::{AppendEntriesCall, Message};
use crate::{Io, Result};

/// ローカルログの削除を行うサブ状態
pub struct FollowerDelete<IO: Io> {
    future: IO::DeleteLog,
    from: LogPosition,
    message: AppendEntriesCall,

    // 削除処理中にtimeoutしたかどうかを記録するフラグ。
    // trueの場合は、削除処理後にcandidateに遷移する。
    // falseの場合は、FollowerIdleに遷移する。
    timeouted: bool,
}

impl<IO: Io> FollowerDelete<IO> {
    pub fn new(common: &mut Common<IO>, from: LogPosition, message: AppendEntriesCall) -> Self {
        let future = common.delete_suffix_from(from.index);
        FollowerDelete {
            future,
            from,
            message,
            timeouted: false,
        }
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        if let Message::AppendEntriesCall(m) = message {
            common.rpc_callee(&m.header).reply_busy();
        }
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        // logに対する削除が進行中であることを
        // commonに通知（フラグをセット）する。
        common.set_if_log_is_being_deleted(true);

        if let Async::Ready(_) = track!(self.future.poll())? {
            track!(common.handle_log_rollbacked(self.from))?;

            // logに対する削除が完了し
            // common.historyとlogが一致したので
            // commonに通知する。
            common.set_if_log_is_being_deleted(false);

            common
                .rpc_callee(&self.message.header)
                .reply_append_entries(self.from);

            if self.timeouted {
                Ok(Some(common.transit_to_candidate()))
            } else {
                let next = Follower::Idle(FollowerIdle::new());
                Ok(Some(RoleState::Follower(next)))
            }
        } else {
            Ok(None)
        }
    }
    /// 削除処理中にtimeoutが発生した場合にそれを記録するためのメソッド
    pub fn set_timeout(&mut self) {
        self.timeouted = true;
    }
}

#[cfg(test)]
mod test {
    use crate::test_dsl::dsl::*;

    #[test]
    #[rustfmt::skip]
    fn delete_test_scenario1() {
        use Command::*;
        use LogEntry::*;

        let a = NodeName(0);
        let b = NodeName(1);
        let c = NodeName(2);
        let (mut service, _cluster) = build_complete_graph(&[a, b, c]);

        interpret(
            &vec![
                RunAllUntilStabilize,
                Timeout(a),
                RunAllUntilStabilize,
                // ここまでで a がリーダーになっている

                // 実際にリーダーになっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsFollower),
                Check(c, Pred::IsFollower),

                RecvBan(a, b), RecvBan(a, c), // aはbからもcからも受け取らない
                RecvBan(b, a), // bはaからは受け取らない
                RecvBan(c, a), // cはaからは受け取らない

                // aが孤立している状況で
                // データをproposeすることで
                // aにデータをためる
                Propose(a), Propose(a), Propose(a),

                // bとcは新しいTermへ移る準備(aのfollowを外す)
                Timeout(b), Timeout(c), RunAllUntilStabilize,

                // bを新しいリーダーにする
                Timeout(b),

                // bとcだけ適当な回数計算を進める
                Step(b), Step(c),
                Step(b), Step(c),
                Step(b), Step(c),
                Step(b), Step(c),
                Step(b), Step(c),
                Step(b), Step(c),

                // cを独立させる
                RecvBan(c, b),
                RecvBan(c, a),
                RunAllUntilStabilize,

                // 想定している状況になっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsLeader),
                Check(c, Pred::IsFollower),
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2), Com(2), Com(2), Com(2)])),
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Noop(4)])),
                Check(c, Pred::RawLogIs(0, 0, vec![Noop(2)])), // Noop(4)がないことに注意

                // a<->b は通信可能にする
                RecvAllow(b, a),
                RecvAllow(a, b),

                // bからハートビートを送る
                Heartbeat(b),
                // メモリ上のlogとdisk上のlogにgapが生じるところまで進める
                Step(b), Step(a),
                Step(b), Step(a),
                Step(b), Step(a),
                Step(b), Step(a),

                // これは a のメモリ上のlogの終端位置が
                // 4にあって、その直前のtermが2であることを意味している
                Check(a, Pred::HistoryTail(2, 4)),

                // 一方で、既にDiskからは削除済みである
                Check(a, Pred::RawLogIs(0, 0, vec![Noop(2)])),

                // この状態のまま、aがタイムアウトしてしまうと
                // 上の食い違った状態でleaderになろうとする。
                //
                // follower/mod.rs において handle_timeout で
                // Delete中のタイムアウトは禁止している場合は
                // このような場合を防ぐためであり、
                // 以下のコードで問題は起きない。
                //
                // 一方で、タイムアウトを許す場合は
                // 以下のコード（最後のStepAll）で問題が起こる。

                // まず a<->c 間だけ通信を可能にする
                RecvAllow(a, c),
                RecvAllow(c, a),
                RecvBan(b, a),
                RecvBan(b, c),
                RecvBan(c, b),
                RecvBan(a, b),

                // aとcをタイムアウトさせて十分に実行させることで
                // 両者をcandidateにする
                Timeout(a), Timeout(c), StepAll(100),

                // そのあと、aがLeaderになれるようにTermを増やす手助け
                Timeout(a),

                // Delete中のタイムアウトを許していない場合 => aはfollowerのまま, cはcandidate
                // Delete中のタイムアウトを許している場合 => a も c も candidate になる
                //
                // Delete中の「タイムアウトを許可している場合」は、
                // aがleaderとなった後で、
                // メモリ上のlogと実体との差に起因するエラーが生じる。
                //
                // 発生するエラーについて:
                // 今回は `impl_io::over_write` で
                // Disk上で「連続しないlog」を作成しようとしてエラーとなる。
                //
                // RaftlogではDisk上のlogは
                // 論理上連続していることが仮定されているが
                // (IO traitのload_log methodの引数を参照せよ）
                // 仮にエラーとなる部分のassertionを外したとしても、
                // 存在しない領域へのloadが発生し、どのみちエラーになる。
                StepAll(100),
            ],
            &mut service
        );
    }
}
