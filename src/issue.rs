#[cfg(test)]
mod test {
    use crate::test_dsl::*;
    use crate::test_dsl::dsl::*;

    #[test]
    #[rustfmt::skip]
    fn issue_append_cancel() {
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

                // aがリーダーになっていることを確認する
                Check(a, Pred::IsLeader),
                Check(b, Pred::IsFollower),
                Check(c, Pred::IsFollower),

                // a-x->c
                RecvBan(c, a),

                Propose(a), Propose(a), Propose(a),
                Heartbeat(a),

                Step(a), Step(a), Step(a), Step(b),
                Step(a), Step(a), Step(a), Step(b),

                // bで
                // history: 22
                // log    : 2222
                // の形にする。
                Dump(b),

                // aを孤立させて
                RecvBan(b, a),

                // bとcをタイムアウトさせ
                Timeout(b), Timeout(c),
                RunAllUntilStabilize,
                Timeout(b),
                RunAllUntilStabilize,

                // bをリーダーにする
                Check(b, Pred::IsLeader),
                Check(c, Pred::IsFollower),

                // bは以下のようになっている
                // history: 224
                // log    : 2242
                Dump(b),

                // すなわち、bはInconsistentである。
                // この状態で再起動するとクラッシュする。
                Check(b, Pred::RawLogIs(0, 0, vec![Noop(2), Com(2), Noop(4), Com(2)])),
                Check(b, Pred::Not(Box::new(Pred::LogTermConsistency))),
            ],
            &mut service,
        );
    }
}
