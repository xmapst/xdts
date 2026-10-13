package iproto

import (
	"errors"
	"fmt"
)

// ErrInvalidColumnValue 是 ColumnValue 未通过结构性校验时的哨兵错误。
var ErrInvalidColumnValue = errors.New("iproto: 非法的 ColumnValue")

// ValidateColumnValue 校验 ColumnValue 的结构性约束。is_null 分支只允许 true；
// false 不能被解释成 NULL 或其它列值，必须显式拒绝。
func ValidateColumnValue(v *ColumnValue) error {
	if v == nil {
		// nil 表示"该列值缺失"，与"业务上就是 NULL"是不同的状态（例如调用方按下标对齐
		// column_names 时数组越界），不在本函数的校验范围内，由调用方按各自场景处理。
		return nil
	}
	if isNull, ok := v.GetValue().(*ColumnValue_IsNull); ok && !isNull.IsNull {
		return fmt.Errorf("%w: is_null 分支的唯一合法取值是 true，收到 false", ErrInvalidColumnValue)
	}
	return nil
}

// ValidateRow 对 Row 内的每个 ColumnValue 做 ValidateColumnValue 校验，
// 出错时用列下标定位，便于调用方（transform/mysql_out 等）在日志/事件里指出具体列。
func ValidateRow(row *Row) error {
	if row == nil {
		return nil
	}
	for i, v := range row.GetValues() {
		if err := ValidateColumnValue(v); err != nil {
			return fmt.Errorf("列下标 %d: %w", i, err)
		}
	}
	return nil
}

// ValidateRowChange 校验一次行变更的前镜像与后镜像。
func ValidateRowChange(rc *RowChange) error {
	if rc == nil {
		return nil
	}
	if err := ValidateRow(rc.GetBefore()); err != nil {
		return fmt.Errorf("before: %w", err)
	}
	if err := ValidateRow(rc.GetAfter()); err != nil {
		return fmt.Errorf("after: %w", err)
	}
	return nil
}

// ValidateTxn 校验一个事务内的全部行变更（DDL 事务没有 rows，天然跳过）。
func ValidateTxn(txn *Txn) error {
	if txn == nil {
		return nil
	}
	if err := ValidateTxnLineage(txn); err != nil {
		return err
	}
	if err := ValidateDDLColumnPolicy(txn.GetDdl().GetColumnPolicy()); err != nil {
		return err
	}
	if err := ValidateDDLSourceDependencies(txn.GetDdl()); err != nil {
		return err
	}
	if err := ValidateShadowTablePolicy(txn.GetShadowTablePolicy()); err != nil {
		return err
	}
	for i, rc := range txn.GetRows() {
		if err := ValidateRowChange(rc); err != nil {
			return fmt.Errorf("gtid=%s 第 %d 条行变更: %w", txn.GetGtid(), i, err)
		}
	}
	return nil
}

// ErrDataBatchEmpty 是 ValidateDataBatch 拒绝零事务批次时的哨兵，供 amqp_in 用
// errors.Is 精确识别"这是空批次"，不是笼统的结构校验失败（文档见 ValidateDataBatch）。
var ErrDataBatchEmpty = errors.New("iproto: DataBatch 不含任何事务")

// ValidateDataBatch rejects malformed transactions and empty batches before application.
func ValidateDataBatch(b *DataBatch) error {
	if b == nil {
		return nil
	}
	if err := ValidatePublicationWindowMetadata(b); err != nil {
		return err
	}
	if len(b.GetTxns()) == 0 {
		return fmt.Errorf("%w: pipeline_id=%s run_id=%d", ErrDataBatchEmpty, b.GetPipelineId(), b.GetRunId())
	}
	for i, txn := range b.GetTxns() {
		if err := ValidateTxn(txn); err != nil {
			return fmt.Errorf("批次第 %d 个事务: %w", i, err)
		}
	}
	return nil
}

// ErrSnapshotChunkMissingTarget 是 ValidateSnapshotChunk 拒绝"缺少目标表标识"的
// 分块时的哨兵，供调用方用 errors.Is 区分（文档见 ValidateSnapshotChunk）。
var ErrSnapshotChunkMissingTarget = errors.New("iproto: SnapshotChunk 缺少 schema/table")

// ValidateSnapshotChunk 校验一个全量分块内的全部行（分块没有 RowChange 包装，
// 直接是 Row 列表，因此不复用 ValidateRowChange）。
func ValidateSnapshotChunk(c *SnapshotChunk) error {
	if c == nil {
		return nil
	}
	if c.Recovery != nil {
		if err := ValidateSnapshotRecoveryIdentity(c.Recovery); err != nil {
			return err
		}
		if c.ChunkNo == 0 || c.ChunkNo >= 1<<63-4002 || c.TableCreation != nil {
			return fmt.Errorf("iproto: invalid recovery chunk range or creation")
		}
	}
	if err := ValidateShadowTablePolicy(c.GetShadowTablePolicy()); err != nil {
		return err
	}
	if c.GetSchema() == "" || c.GetTable() == "" {
		return fmt.Errorf("%w: pipeline_id=%s chunk_no=%d schema=%q table=%q",
			ErrSnapshotChunkMissingTarget, c.GetPipelineId(), c.GetChunkNo(), c.GetSchema(), c.GetTable())
	}
	if err := ValidateSnapshotCreation(c.GetTableCreation(), c.GetSchema(), c.GetTable()); err != nil {
		return err
	}
	for i, row := range c.GetRows() {
		if err := ValidateRow(row); err != nil {
			return fmt.Errorf("分块 chunk_no=%d 第 %d 行: %w", c.GetChunkNo(), i, err)
		}
	}
	return nil
}
