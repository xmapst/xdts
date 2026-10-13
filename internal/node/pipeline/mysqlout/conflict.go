package mysqlout

import (
	"github.com/xmapst/xdts/internal/iproto"
)

// 本文件只判定与构造，绝不改变任何成功/失败语义：`ApplyBatch` 在检出冲突时
// 仍然返回 nil，DELETE 命中 0 行仍然视为成功。
//
// 这条边界必须写清楚，因为 buildDeleteSourceOverride 的文档里有一段相反方向的警告：
// 「谁将来在 applyRowChange 里加一道"受影响行数必须 > 0"的校验…会让每一次重试
// 都失败，pipeline 原地卡住」。那段警告针对的是校验，本文件做的是记录，
// 两者的失败模式不同：校验会让正常重放失败，记录只多发一条事件。
//
// go-sql-driver 的 `clientFoundRows` 必须是关闭的（默认值）：打开时
// "匹配到但值未变"的 UPDATE 会返回 1 而不是 0，下面 UPDATE 行的判读会把它误读成
// "目标行不存在"。本包的 DSN 构造在 internal/node/pipeline/mysqlout/writer.go，形如 `user:password@tcp(host:port)/`，
// 不带任何参数，所以该前提成立。
//
// 期望 0 命中（本包从不设置该参数）。这个 0 是盲区口径：0 时"行"与"出现"两种
// 读法同时为 0，口径错误在这一格不可观测——所以它不能被拿去支撑别的格。
type conflictKind int

const (
	// conflictNone：这一行变更按预期落地，没有冲突。
	conflictNone conflictKind = iota
	// conflictDuplicateKey：INSERT 撞主键/唯一键（目标行已经存在）。
	conflictDuplicateKey
	// conflictTargetRowMissing：UPDATE/DELETE 的目标行不存在。
	conflictTargetRowMissing
	// conflictUnexpectedRows：受影响行数超出该语句形态的可能取值。
	conflictUnexpectedRows
)

func (k conflictKind) String() string {
	switch k {
	case conflictNone:
		return "none"
	case conflictDuplicateKey:
		return "duplicate_key"
	case conflictTargetRowMissing:
		return "target_row_missing"
	case conflictUnexpectedRows:
		return "unexpected_rows_affected"
	default:
		return "unknown"
	}
}

// classifyConflict derives conflict meaning from operation and affected-row counts.
// Replacement writes use upsert row-count semantics; do not interpret every count
// as a plain INSERT or UPDATE result.
func classifyConflict(t iproto.ChangeType, rowsAffected int64) conflictKind {
	switch t {
	case iproto.ChangeType_CHANGE_TYPE_INSERT:
		switch rowsAffected {
		case 1:
			return conflictNone
		case 0, 2:
			return conflictDuplicateKey
		}
	case iproto.ChangeType_CHANGE_TYPE_UPDATE:
		switch rowsAffected {
		case 0, 2:
			return conflictNone
		case 1:
			return conflictTargetRowMissing
		}
	case iproto.ChangeType_CHANGE_TYPE_DELETE:
		switch rowsAffected {
		case 1:
			return conflictNone
		case 0:
			return conflictTargetRowMissing
		}
	}
	return conflictUnexpectedRows
}

// buildConflictDetail 构造一条 ConflictEventDetail（LOOP-15：表、主键、冲突类型、
// 处理结果、前后镜像）。
//
// 三个字段的取值都由源覆盖策略唯一确定：
//   - Change 原样带上这条 RowChange——schema/table/primary_key_columns/before/after
//     全在里面，LOOP-15 要的"表、主键、前后镜像"不需要另外拼；
//   - Policy here is SOURCE_WINS. applyLWWRow replaces it for configured tables.
//   - Resolution 恒为 CONFLICT_RESOLUTION_APPLIED：源覆盖是"记录后继续"，到达的变更
//     已经落地。DISCARDED 是 LWW 判负时才有的结局，TXN_ROLLED_BACK 是整事务回滚，
//     两者在本策略下都不会出现。
//
// Writer.emitConflict creates independent report ownership after the policy
// decision. Configured mask_rules replace selected before/after/target values,
// including positioning keys, with "***" in appended redacted image fields.
// Business values used by DML and policy comparison remain unchanged.
func buildConflictDetail(rc *iproto.RowChange) *iproto.ConflictEventDetail {
	return &iproto.ConflictEventDetail{
		Change:     rc,
		Policy:     iproto.ConflictMode_CONFLICT_MODE_SOURCE_WINS,
		Resolution: iproto.ConflictResolution_CONFLICT_RESOLUTION_APPLIED,
	}
}

// effTypeForUpdate 给出源覆盖 UPDATE 那条 ODKU 语句实际扮演的变更类型，
// 它决定受影响行数按哪张表判读。
func effTypeForUpdate(primaryKeyChanged bool) iproto.ChangeType {
	if primaryKeyChanged {
		// 前镜像那一行刚被 DELETE 掉，这条 ODKU 是一次插入。
		return iproto.ChangeType_CHANGE_TYPE_INSERT
	}
	// ODKU 落在原来那一行上，语义就是 UPDATE。
	return iproto.ChangeType_CHANGE_TYPE_UPDATE
}

// conflictSink 收一条已构造好的冲突详情。由 Writer.SetConflictSink 注入，nil 表示
// 不上报（此时检测仍然发生，只是产物被丢弃——见 maybeReportConflict）。
type conflictSink func(*iproto.ConflictEventDetail)

// effType 是这条语句实际扮演的变更类型，不一定等于 rc.GetType()：源覆盖的
// "主键变化 UPDATE" 会先按前镜像主键 DELETE、再按后镜像 INSERT（见 applyRowChange
// 那一支），两条语句的受影响行数必须分别按 DELETE 与 INSERT 的表判读。用 rc.GetType()
// 去判那两条语句会得出相反的结论，这是本参数单独存在的全部理由。
func maybeReportConflict(sink conflictSink, effType iproto.ChangeType, rowsAffected int64, rc *iproto.RowChange) conflictKind {
	k := classifyConflict(effType, rowsAffected)
	if k != conflictDuplicateKey && k != conflictTargetRowMissing {
		return k
	}
	if sink != nil {
		detail := buildConflictDetail(rc)
		switch k {
		case conflictDuplicateKey:
			detail.Reason = "insert_duplicate"
		case conflictTargetRowMissing:
			if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE {
				detail.Reason = "delete_missing"
			} else {
				detail.Reason = "update_missing"
			}
		}
		sink(detail)
	}
	return k
}
