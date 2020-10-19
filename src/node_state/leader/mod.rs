use self::appender::LogAppender;
use self::follower::FollowersManager;
use super::{Common, NextState};
use crate::election::Role;
use crate::log::{LogEntry, LogIndex, LogSuffix, ProposalId};
use crate::message::{Message, SequenceNumber};
use crate::{ErrorKind, Io, Result};

mod appender;
mod follower;

/// 選挙で選ばれたリーダ.
///
/// 主に、以下のようなことを行う:
///
/// - フォロワーとのログ同期:
///   - フォロワーのログが遅れている場合には、適宜`AppendEntriesCall`を使って差分を送信
///   - リーダのログ先頭よりも遅れている場合には、`InstallSnapshotCast`を送信
/// - 新規ログエントリの処理:
///   - ローカルログへの追記およびフォロワーへのブロードキャスト、を行う
///   - 過半数からの承認(ログ保存)を得られた時点で、コミット済みとなる
/// - クラスタ構成変更対応:
///   - 整合性を維持しながらの動的クラスタ構成変更用の処理諸々
///   - e.g., join-consensusを間に挟んだ段階的な構成移行
/// - 定期的なハートビートメッセージのブロードキャストによるリーダ維持
pub struct Leader<IO: Io> {
    followers: FollowersManager<IO>,
    appender: LogAppender<IO>,
    commit_lower_bound: LogIndex,
}
impl<IO: Io> Leader<IO> {
    pub fn new(common: &mut Common<IO>) -> Self {
        common.set_timeout(Role::Leader);
        let term_start_index = common.log().tail().index;
        let followers = FollowersManager::new(common.config().clone());
        let mut appender = LogAppender::new();

        // 新しいリーダ選出直後に追加されるログエントリ.
        // 詳細は、論文の「8 Client interaction」参照.
        let noop = LogEntry::Noop {
            term: common.term(),
        };
        appender.append(common, vec![noop]);

        Leader {
            followers,
            appender,
            commit_lower_bound: term_start_index,
        }
    }
    pub fn handle_timeout(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        self.broadcast_empty_entries(common);
        Ok(None)
    }
    pub fn handle_message(
        &mut self,
        common: &mut Common<IO>,
        message: Message,
    ) -> Result<NextState<IO>> {
        if let Message::AppendEntriesReply(reply) = message {
            let updated = self.followers.handle_append_entries_reply(&common, &reply);

            track!(self.followers.log_sync(common, &reply))?;

            if updated {
                track!(self.handle_committed_log(common))?;
            }
        }
        Ok(None)
    }
    pub fn run_once(&mut self, common: &mut Common<IO>) -> Result<NextState<IO>> {
        while let Some(appended) = track!(self.appender.run_once(common))? {
            for e in &appended.entries {
                if let LogEntry::Config { ref config, .. } = *e {
                    self.followers.handle_config_updated(config);

                    // 構成変更のタイミングによっては、
                    // 一時的にコミット済み領域が巻き戻る可能性があるので、
                    // それを防ぐために、下限を更新する.
                    //
                    // e.g., `Joint構成への遷移時に巻き戻りが発生するケース
                    //
                    // - 1. 新旧のコミット地点が一致したので`Joint`に遷移
                    // - 2. `Joint`用の構成をリーダのローカルログに追記開始
                    // - 3. 追記が完了する前に、旧構成のコミット地点のみが進む
                    // - 4. `Joint`構成の追記が完了 => 以後はJoint合意が使用される
                    // - 5. この時点で新構成のコミット地点の方が遅れているので、
                    //      Joint合意を取ると最悪で、1の地点までコミットが巻き戻る
                    //
                    // 上のケース以外にも、複数の構成変更が並行して実行された場合にも、
                    // 一時的な巻き戻りが発生する可能性がある.
                    if self.commit_lower_bound < common.log().committed_tail().index {
                        // NOTE:
                        // `commit_lower_bound`は、`Term`の開始地点を記録するためにも使っているため、
                        // 上の条件式が必要.
                        self.commit_lower_bound = common.log().committed_tail().index;
                    }
                }
            }
            self.broadcast_slice(common, appended);
        }
        track!(self.handle_change_config(common))?;
        track!(self.followers.run_once(common))?;
        Ok(None)
    }
    pub fn propose(&mut self, common: &mut Common<IO>, entry: LogEntry) -> ProposalId {
        let proposal_id = self.next_proposal_id(common);
        self.appender.append(common, vec![entry]);
        proposal_id
    }
    pub fn heartbeat_syn(&mut self, common: &mut Common<IO>) -> SequenceNumber {
        let seq_no = common.next_seq_no();
        self.broadcast_empty_entries(common);
        seq_no
    }
    pub fn proposal_queue_len(&self, common: &Common<IO>) -> usize {
        self.appender.unappended_log_tail(common) - common.log().tail().index
    }
    pub fn last_heartbeat_ack(&self) -> SequenceNumber {
        self.followers.latest_hearbeat_ack()
    }

    fn handle_change_config(&mut self, common: &mut Common<IO>) -> Result<()> {
        if common.config().state().is_stable() {
            return Ok(());
        }

        if self.appender.is_busy() {
            // 前回の構成変更用に追記したログエントリがまだ処理されていない可能性がある
            return Ok(());
        }

        let committed = self.followers.committed_log_tail();
        if committed < common.log().last_record().head.index {
            // まだ新構成がコミットされていない可能性がある
            return Ok(());
        }

        let joint_committed = self.followers.joint_committed_log_tail();
        if joint_committed == committed {
            // 新構成のメンバのローカルログが、旧構成のものに追い付いた
            // => 構成変更の次のフェーズに遷移
            let term = common.term();
            let config = common.config().to_next_state();
            let entry = LogEntry::Config { term, config };
            self.propose(common, entry);
        }
        Ok(())
    }
    fn next_proposal_id(&self, common: &Common<IO>) -> ProposalId {
        let term = common.term();
        let index = self.appender.unappended_log_tail(common);
        ProposalId { term, index }
    }
    fn broadcast_slice(&mut self, common: &mut Common<IO>, slice: LogSuffix) {
        self.followers
            .set_last_broadcast_seq_no(common.next_seq_no());
        common.set_timeout(Role::Leader);
        common.rpc_caller().broadcast_append_entries(slice);
    }
    fn broadcast_empty_entries(&mut self, common: &mut Common<IO>) {
        let head = common.log().tail();
        let entries = Vec::new();
        let slice = LogSuffix { head, entries };
        self.broadcast_slice(common, slice);
    }
    fn handle_committed_log(&mut self, common: &mut Common<IO>) -> Result<()> {
        let committed = self.followers.committed_log_tail();
        if committed < self.commit_lower_bound {
            // コミット済みのログ領域でも、現在のtermよりも前に追加されたものはまだコミットできない.
            // 詳細は論文の「5.4.2 Committing entries from previous terms」を参照のこと.
            return Ok(());
        }

        let old = common.log().committed_tail();
        if old.index == committed {
            // 未処理していないコミット済みログ領域は無い.
            return Ok(());
        }
        track_assert!(
            old.index < committed,
            ErrorKind::InconsistentState,
            "old={:?}, committed={:?}",
            old,
            committed
        );

        // 履歴に新しいコミット済み領域を記録する.
        // 新規コミット済み領域の処理は`Common::run_once`関数の中で行われる.
        track!(common.handle_log_committed(committed))?;
        Ok(())
    }
}
