package mysqlout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// errUnsupportedChangeType marks row operations absent from the published
// write matrix. Policy selection belongs to Writer.applyTxnResult; controller
// and Writer.New reject unknown policy modes before creating connections.
var errUnsupportedChangeType = errors.New("mysqlout: 不支持的行变更类型")

// quoteIdent 给一个 MySQL 标识符（库名/表名/列名）加反引号，内部的反引号按 MySQL
// 规则转义成两个连续反引号。RowChange.schema/table/column_names 来自源端
// information_schema，理论上已经是合法标识符，这里仍然做转义而不是假设"上游
// 一定干净"（NODE-OUT-07 doc 注释同一条原则："目标端必须对任意上游…保持健壮"）。
func quoteIdent(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}

// buildInsertSourceOverride 按 NODE-OUT-03"源覆盖"策略为一次 INSERT 行变更生成
// 参数化的 INSERT ... ON DUPLICATE KEY UPDATE 语句：目标行不存在则插入，存在则
// 用源端后镜像整行覆盖。
//
// 如果 column_names 里只有主键列、没有任何非主键列（纯主键表），
// ON DUPLICATE KEY UPDATE 后面不能是空列表（MySQL 语法错误），退化成对第一列
// 做"自己赋值给自己"的空操作更新，效果等价于"已存在则什么都不改"，仍然满足
// "源覆盖"在这种退化情形下的唯一合理语义（没有非主键列可覆盖）。
func buildInsertSourceOverride(schema, table string, columnNames []string, after *iproto.Row, primaryKeyColumns []string) (string, []any, error) {
	if len(columnNames) == 0 {
		return "", nil, fmt.Errorf("mysqlout: INSERT 缺少 column_names（%s.%s）", schema, table)
	}
	values := after.GetValues()
	if len(values) != len(columnNames) {
		return "", nil, fmt.Errorf("mysqlout: INSERT 的 after 列值数量(%d)与 column_names 数量(%d)"+
			"不一致（%s.%s）", len(values), len(columnNames), schema, table)
	}

	args := make([]any, 0, len(columnNames)*2)

	quotedCols := make([]string, len(columnNames))
	placeholders := make([]string, len(columnNames))
	for i, col := range columnNames {
		arg, err := columnValueToArg(values[i])
		if err != nil {
			return "", nil, fmt.Errorf("mysqlout: 列 %s.%s.%s: %w", schema, table, col, err)
		}
		quotedCols[i] = quoteIdent(col)
		placeholders[i] = "?"
		args = append(args, arg)
	}

	pkSet := make(map[string]bool, len(primaryKeyColumns))
	for _, pk := range primaryKeyColumns {
		pkSet[pk] = true
	}

	updateAssignments := make([]string, 0, len(columnNames))
	for _, col := range columnNames {
		if pkSet[col] {
			continue
		}
		q := quoteIdent(col)
		updateAssignments = append(updateAssignments, q+" = VALUES("+q+")")
	}
	if len(updateAssignments) == 0 {
		// 纯主键表：没有非主键列可覆盖，用第一列的空操作赋值占位，语义等价于
		// "已存在则不改"。
		q := quotedCols[0]
		updateAssignments = append(updateAssignments, q+" = "+q)
	}

	query := fmt.Sprintf(
		"INSERT INTO %s.%s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s",
		quoteIdent(schema), quoteIdent(table),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
		strings.Join(updateAssignments, ", "),
	)
	return query, args, nil
}

// 这句不成立会在哪坏：谁将来在 applyRowChange 里加一道"受影响行数必须 > 0"的校验，
// 坏的不是这一次 DELETE，是重放——NODE-CKP-04 去重会重放已经删成功过的 DELETE，
// 那一行此时已经不存在，校验会让每一次重试都失败，pipeline 原地卡住。它会报错
// （不是静默），但报的是"0 行受影响"，指向的是数据不一致，而真正的原因是这条语义被
// 改掉了——排查方向会被带偏。要改这里得先改  写入方式矩阵"源覆盖"
// 行的 DELETE 列。
//
// primaryKeyColumns 为空时拒绝生成语句——没有 WHERE 条件的 DELETE 会删掉整张表，
// 这是灾难性的，不能因为上游数据缺失就静默退化成"删全表"。NODE-OUT-04 预检查
// 本该挡住无主键表进入同步范围，这里是第二道防线（同 NODE-OUT-07 doc 注释的
// 原则："目标端必须对任意上游…保持健壮"，各自负责，不假设上游一定干净）。
func buildDeleteSourceOverride(schema, table string, columnNames []string, before *iproto.Row, primaryKeyColumns []string) (string, []any, error) {
	if len(primaryKeyColumns) == 0 {
		return "", nil, fmt.Errorf("mysqlout: DELETE 缺少 primary_key_columns（%s.%s），"+
			"拒绝生成没有 WHERE 条件的语句", schema, table)
	}
	values := before.GetValues()
	if len(values) != len(columnNames) {
		return "", nil, fmt.Errorf("mysqlout: DELETE 的 before 列值数量(%d)与 column_names 数量(%d)"+
			"不一致（%s.%s）", len(values), len(columnNames), schema, table)
	}

	colIndex := make(map[string]int, len(columnNames))
	for i, col := range columnNames {
		colIndex[col] = i
	}

	conditions := make([]string, 0, len(primaryKeyColumns))
	args := make([]any, 0, len(primaryKeyColumns))
	for _, pk := range primaryKeyColumns {
		idx, ok := colIndex[pk]
		if !ok {
			return "", nil, fmt.Errorf("mysqlout: 主键列 %s 不在 column_names 里（%s.%s）", pk, schema, table)
		}
		arg, err := columnValueToArg(values[idx])
		if err != nil {
			return "", nil, fmt.Errorf("mysqlout: 主键列 %s.%s.%s: %w", schema, table, pk, err)
		}
		conditions = append(conditions, quoteIdent(pk)+" = ?")
		args = append(args, arg)
	}

	query := fmt.Sprintf("DELETE FROM %s.%s WHERE %s",
		quoteIdent(schema), quoteIdent(table), strings.Join(conditions, " AND "))
	return query, args, nil
}

// primaryKeyChanged 判断一次 UPDATE 的主键值是否发生了变化：逐列比较
// primary_key_columns 在 before/after 的取值是否相等。用 proto.Equal 而不是
// 手写等值比较——ColumnValue 是带 oneof 的 protobuf 消息，proto.Equal 正确处理
// "选中了哪个分支 + 分支内的值"两部分的结构性相等，手写比较（比如只比某个
// 具体字段）容易在新增分支时漏掉。
//
// primaryKeyColumns 为空时直接返回"未变化"：没有主键可比较，且
// buildDeleteSourceOverride 本身也会拒绝在空主键下生成 DELETE，这里提前短路
// 避免调用方误以为"变化"而走进那条必然出错的分支。
func primaryKeyChanged(columnNames, primaryKeyColumns []string, before, after *iproto.Row) (bool, error) {
	if len(primaryKeyColumns) == 0 {
		return false, nil
	}
	beforeValues := before.GetValues()
	afterValues := after.GetValues()
	if len(beforeValues) != len(columnNames) {
		return false, fmt.Errorf("mysqlout: UPDATE 的 before 列值数量(%d)与 column_names 数量(%d)不一致",
			len(beforeValues), len(columnNames))
	}
	if len(afterValues) != len(columnNames) {
		return false, fmt.Errorf("mysqlout: UPDATE 的 after 列值数量(%d)与 column_names 数量(%d)不一致",
			len(afterValues), len(columnNames))
	}

	colIndex := make(map[string]int, len(columnNames))
	for i, col := range columnNames {
		colIndex[col] = i
	}
	for _, pk := range primaryKeyColumns {
		idx, ok := colIndex[pk]
		if !ok {
			return false, fmt.Errorf("mysqlout: 主键列 %s 不在 column_names 里", pk)
		}
		if !proto.Equal(beforeValues[idx], afterValues[idx]) {
			return true, nil
		}
	}
	return false, nil
}

// applyInsertOrUpdate 是 INSERT 与"主键未变化"的 UPDATE 共用的写入路径：
//
//	写入方式矩阵"源覆盖"行的 UPDATE 列是"目标行不存在则按后
//
// 镜像插入"——这正是 INSERT...ON DUPLICATE KEY UPDATE 的语义，不需要为 UPDATE
// 单独构造不同的语句。
func applyInsertOrUpdate(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange, sink conflictSink, effType iproto.ChangeType) error {
	query, args, err := buildInsertSourceOverride(
		rc.GetSchema(), rc.GetTable(), rc.GetColumnNames(), rc.GetAfter(), rc.GetPrimaryKeyColumns())
	if err != nil {
		return err
	}
	res, err := tx.execDML(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("mysqlout: 执行 %s 失败(%s.%s): %w", rc.GetType(), rc.GetSchema(), rc.GetTable(), err)
	}
	inspectRowsAffected(res, sink, effType, rc)
	return nil
}

// 它永不返回 error、永不改变调用方的成功/失败语义：检测失败（驱动不支持
// RowsAffected、或行数超出该语句形态的可能取值）只记一条 Warn，写入本身照旧算成功。
// 这条约束是本功能能够安全落地的前提——buildDeleteSourceOverride 的文档警告过
// "谁在这里加一道『受影响行数必须 > 0』的校验会让重放每次都失败、pipeline 原地
// 卡住"，那段警告针对校验，本函数做的是记录，两者的失败模式不同。
func inspectRowsAffected(res sql.Result, sink conflictSink, effType iproto.ChangeType, rc *iproto.RowChange) {
	n, err := res.RowsAffected()
	if err != nil {
		// 驱动不支持（go-sql-driver 支持，所以这里实际走不到）——记一条就够，
		// **不要**把它当成冲突，也不要让写入失败。
		slog.Warn("rows affected unavailable, conflict detection skipped",
			slog.String("schema", rc.GetSchema()), slog.String("table", rc.GetTable()),
			slog.Any("change_type", effType), slog.Any("error", err))
		return
	}
	if k := maybeReportConflict(sink, effType, n, rc); k == conflictUnexpectedRows {
		slog.Warn("unexpected rows affected, conflict undetermined",
			slog.String("schema", rc.GetSchema()), slog.String("table", rc.GetTable()),
			slog.Any("change_type", effType), slog.Int64("rows_affected", n))
	}
}

var errForeignKeyChecksUnsafeForPrimaryKeyChange = errors.New(
	"mysqlout: foreign_key_checks=1 时不允许执行主键变化的 UPDATE")

// applyRowChange executes the source-wins write matrix. Writer.applyTxnResult
// selects it for SOURCE_WINS, and applyLWWRow calls it only after a winning
// comparison. STOP uses ordinary statements through applyStopRow.
//
// foreignKeyChecksEnabled 是调用方当前会话的 foreign_key_checks 取值（见
// Options.ForeignKeyChecks），只在 UPDATE 的主键变化分支里使用——见该分支内的
// 详细说明：该配置项被打开时，主键变化分支会造成目标端与源端静默分叉的真实
// 风险，不是假想场景。
func applyRowChange(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange, foreignKeyChecksEnabled bool, sink conflictSink) error {
	if rc == nil {
		return fmt.Errorf("mysqlout: RowChange 为 nil")
	}
	if err := iproto.ValidateRowChange(rc); err != nil {
		return fmt.Errorf("mysqlout: RowChange 未通过结构性校验(%s.%s): %w", rc.GetSchema(), rc.GetTable(), err)
	}

	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE || rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE {
		var err error
		sink, err = tx.inspectBeforeImage(ctx, rc, sink)
		if err != nil {
			return err
		}
	}
	switch rc.GetType() {
	case iproto.ChangeType_CHANGE_TYPE_INSERT:
		return applyInsertOrUpdate(ctx, tx, rc, sink, iproto.ChangeType_CHANGE_TYPE_INSERT)
	case iproto.ChangeType_CHANGE_TYPE_UPDATE:
		// 处理方式：先按 before 的主键值删除旧行，再按 after 写入新行，两条
		// 语句在同一个 gtid_next 事务内依次执行（不影响 NODE-OUT-13：两者都是
		// 普通参数化 DML，都通过 tx.execDML，不是隐式提交语句）。
		changed, err := primaryKeyChanged(rc.GetColumnNames(), rc.GetPrimaryKeyColumns(), rc.GetBefore(), rc.GetAfter())
		if err != nil {
			return fmt.Errorf("mysqlout: 判断主键是否变化失败(%s.%s): %w", rc.GetSchema(), rc.GetTable(), err)
		}
		// effType 决定下面那条 ODKU 的受影响行数按哪张表判读，**它不等于 rc.GetType()**：
		//
		//   - 主键**没变**：ODKU 落在原来那一行上，语义就是 UPDATE ⇒ 按 UPDATE 判读
		//     （2 行 = 正常更新；1 行 = 目标行本不存在）。
		//   - 主键**变了**：前镜像那一行刚被上面的 DELETE 删掉，这条 ODKU 是一次**插入**
		//     ⇒ 必须按 INSERT 判读（1 行 = 正常插入；2 行 = 后镜像主键已被别人占用，撞键）。
		//
		// **写成常量 INSERT 会让每一条普通 UPDATE 都被报成撞键**（UPDATE 正常时是 2 行，
		// 而 INSERT 表里 2 行 = 撞键）——那是"失败产出一个合法答案"，冲突记录会被污染到
		// 不可用，且没有任何东西会报错。
		effType := effTypeForUpdate(changed)
		if changed {
			if foreignKeyChecksEnabled {
				return fmt.Errorf("mysqlout: 表 %s.%s 的 UPDATE 主键从(%v)变为(%v)，但当前会话"+
					"foreign_key_checks=1：若目标表存在外键约束，删除前镜像主键行可能触发"+
					"ON DELETE CASCADE 级联删除源端并未删除的子行，或因 RESTRICT/NO ACTION"+
					"导致这条语句直接失败，两者都会让目标库与源端静默分叉，拒绝执行。"+
					"请将 Options.ForeignKeyChecks 改回默认值（关闭/nil），或确认目标表"+
					"确实没有外键约束后再评估是否需要放开这条限制: %w",
					rc.GetSchema(), rc.GetTable(), rc.GetBefore().GetValues(), rc.GetAfter().GetValues(),
					errForeignKeyChecksUnsafeForPrimaryKeyChange)
			}
			deleteQuery, deleteArgs, err := buildDeleteSourceOverride(
				rc.GetSchema(), rc.GetTable(), rc.GetColumnNames(), rc.GetBefore(), rc.GetPrimaryKeyColumns())
			if err != nil {
				return fmt.Errorf("mysqlout: 主键变化时构造删除前镜像键语句失败(%s.%s): %w",
					rc.GetSchema(), rc.GetTable(), err)
			}
			res, err := tx.execDML(ctx, deleteQuery, deleteArgs...)
			if err != nil {
				return fmt.Errorf("mysqlout: 主键变化时删除前镜像键失败(%s.%s): %w",
					rc.GetSchema(), rc.GetTable(), err)
			}
			// 这条语句实际是一次 DELETE，必须按 DELETE 的表判读：0 行 ⇒ 前镜像行
			// 不存在。用 rc.GetType()（UPDATE）判会得出相反结论，见 maybeReportConflict
			// 文档 effType 那一段。
			inspectRowsAffected(res, sink, iproto.ChangeType_CHANGE_TYPE_DELETE, rc)
		}
		return applyInsertOrUpdate(ctx, tx, rc, sink, effType)
	case iproto.ChangeType_CHANGE_TYPE_DELETE:
		query, args, err := buildDeleteSourceOverride(
			rc.GetSchema(), rc.GetTable(), rc.GetColumnNames(), rc.GetBefore(), rc.GetPrimaryKeyColumns())
		if err != nil {
			return err
		}
		res, err := tx.execDML(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("mysqlout: 执行 DELETE 失败(%s.%s): %w", rc.GetSchema(), rc.GetTable(), err)
		}
		inspectRowsAffected(res, sink, iproto.ChangeType_CHANGE_TYPE_DELETE, rc)
		return nil
	default:
		return fmt.Errorf("mysqlout: 行变更类型 %s（%s.%s）: %w",
			rc.GetType(), rc.GetSchema(), rc.GetTable(), errUnsupportedChangeType)
	}
}
