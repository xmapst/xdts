package mysqlout

import (
	"context"
	"errors"
	"fmt"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

// errDDLNotSupported marks a DDL that cannot be safely parsed or rewritten.
var errDDLNotSupported = errors.New("mysqlout: DDL cannot be safely rewritten")

// ApplyBatch 实现 IBatchApplier：把 batch 里的事务按源端提交顺序（NODE-OUT-02）
// 依次写入目标库。单个事务写入失败会中止本次 ApplyBatch 调用（不跳过、不继续
// 处理后续事务）——调用方 amqpin据此决定不 ack，让该批次按原样重投。
func (w *Writer) ApplyBatch(ctx context.Context, batch *iproto.DataBatch) error {
	_, err := w.ApplyBatchResult(ctx, batch)
	return err
}

func (w *Writer) ApplyBatchResult(ctx context.Context, batch *iproto.DataBatch) (result iproto.BatchApplyResult, resultErr error) {
	if batch == nil {
		return result, fmt.Errorf("mysqlout: DataBatch 为 nil")
	}
	// 见 iproto.ValidateDataBatch 的文档注释："供 amqp_in 在 Decode 之后…调用"，
	// 即设计上这层校验本该由 amqp_in 做过一次。这里重复调用一次是防御性的第二道
	// 边界检查，不信任调用方一定已经校验过（NODE-OUT-07 doc 注释同一条纪律：
	// 各自负责、不假设上游一定干净），成本是一次遍历，可以接受。
	if err := iproto.ValidateDataBatch(batch); err != nil {
		return result, fmt.Errorf("mysqlout: DataBatch 未通过结构性校验(pipeline_id=%s): %w", batch.GetPipelineId(), err)
	}
	if len(w.workers) != 0 && !w.catchingUp && len(batch.GetTxns()) > 1 {
		return w.applyParallelBatch(ctx, batch)
	}
	if err := w.ensureConnection(ctx); err != nil {
		return result, err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	if w.restoreForeignKeys {
		if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks = ?", w.opts.foreignKeyChecksValue()); err != nil {
			return result, err
		}
		w.restoreForeignKeys = false
	}

	for i, txn := range batch.GetTxns() {
		if deferred, err := w.deferRecoveryTxn(ctx, txn); err != nil {
			return result, &BatchTxnError{Index: i, Err: err}
		} else if deferred {
			result.DeferredGTIDs = append(result.DeferredGTIDs, txn.Gtid)
			continue
		}
		applied, err := w.applyTxnResult(ctx, txn)
		if err != nil {
			return result, &BatchTxnError{Index: i, Err: err}
		}
		if applied {
			result.AppliedGTIDs = append(result.AppliedGTIDs, txn.GetGtid())
		} else {
			result.RejectedGTIDs = append(result.RejectedGTIDs, txn.GetGtid())
		}
	}
	return result, nil
}

// applyTxn commits DML and DDL in source order through separate transaction paths.
func (w *Writer) applyTxn(ctx context.Context, txn *iproto.Txn) error {
	_, err := w.applyTxnResult(ctx, txn)
	return err
}

func (w *Writer) applyTxnResult(ctx context.Context, txn *iproto.Txn) (bool, error) {
	return w.applyTxnCommit(ctx, txn, nil)
}

func (w *Writer) applyTxnCommit(ctx context.Context, txn *iproto.Txn, beforeCommit func(context.Context) error) (bool, error) {
	if txn == nil {
		return false, fmt.Errorf("mysqlout: Txn 为 nil")
	}
	if txn.GetGtid() == "" {
		return false, fmt.Errorf("mysqlout: Txn.gtid 为空")
	}
	if w.opts.ShadowTablePolicy != nil && !proto.Equal(w.opts.ShadowTablePolicy, txn.GetShadowTablePolicy()) {
		return false, fmt.Errorf("mysqlout: transaction shadow table policy differs from target configuration")
	}

	hasDDL := txn.GetDdl() != nil
	hasRows := len(txn.GetRows()) > 0
	switch {
	case hasDDL && hasRows:
		// proto/data.proto Txn 注释："DDL 事务时为空；DML 事务时非空。两者互斥"——
		// 两者都非空说明上游违反了协议不变量，不是本包该静默容忍或猜测的情形。
		return false, fmt.Errorf("mysqlout: Txn 同时携带 DDL 与 %d 条行变更，违反 DDL/DML 互斥协议不变量",
			len(txn.GetRows()))
	case hasDDL:
		return w.applyDDL(ctx, txn)
	case !hasRows:
		return false, fmt.Errorf("mysqlout: Txn 既没有 DDL 也没有行变更，无法判断如何写入")
	}

	// foreignKeyChecksEnabled 传给 applyRowChange 供 UPDATE 的主键变化分支判断
	// 是否需要拒绝执行，见 internal/node/pipeline/mysqlout/insert.go 该分支的详细说明。
	foreignKeyChecksEnabled := w.opts.foreignKeyChecksValue() == 1
	txn = w.mapTxn(txn)
	for _, row := range txn.GetRows() {
		if dblineage.Reserved(row.GetSchema()) {
			return false, fmt.Errorf("mysqlout: reserved metadata cannot be a business destination")
		}
		if err := validateMappedColumns(row.GetColumnNames()); err != nil {
			return false, err
		}
	}
	if w.catchingUp {
		return true, w.applyCatchupTxn(ctx, txn)
	}
	var conflicts []*iproto.ConflictEventDetail
	accepted := 0
	err := withGTIDTxnCommit(ctx, w.conn, txn.GetGtid(), func(ctx context.Context, tx *dmlTxn) error {
		if err := w.stageTxnLineage(tx, txn); err != nil {
			return err
		}
		var err error
		txn, err = w.projectTargetTxn(ctx, tx, txn)
		if err != nil {
			return err
		}
		txn, err = w.prepareTransformTxn(ctx, tx.tx, txn)
		if err != nil {
			return err
		}
		mergeTables := make(map[string]mergeTable)
		for i := 0; i < len(txn.Rows); {
			rc := txn.Rows[i]
			sink := func(detail *iproto.ConflictEventDetail) {
				detail.Gtid = txn.GetGtid()
				conflicts = append(conflicts, detail)
			}
			merged, err := w.mergeRows(ctx, tx, txn.Rows[i:], mergeTables, sink)
			if err != nil {
				return fmt.Errorf("row change %d: %w", i, err)
			}
			if merged != 0 {
				accepted += merged
				i += merged
				continue
			}
			applied := true
			if w.opts.ConflictPolicy.GetMode() == iproto.ConflictMode_CONFLICT_MODE_STOP {
				err = w.applyStopRow(ctx, tx, rc, sink)
			} else if column := w.lwwTimestampColumn(rc); column != "" {
				applied, err = w.applyLWWRow(ctx, tx, rc, column, sink)
			} else {
				err = applyRowChange(ctx, tx, rc, foreignKeyChecksEnabled, sink)
			}
			if err != nil {
				return fmt.Errorf("row change %d: %w", i, err)
			}
			if applied {
				accepted++
			}
			i++
		}
		if accepted == 0 {
			return errLWWTransactionRejected
		}
		return nil
	}, beforeCommit)
	if errors.Is(err, ErrConflictStopped) {
		if w.conflictSink != nil {
			for _, detail := range conflicts {
				w.emitConflict(detail)
			}
		}
		return false, err
	}
	if errors.Is(err, errLWWTransactionRejected) {
		if isConnectionError(err) {
			return false, err
		}
		for _, detail := range conflicts {
			detail.Resolution = iproto.ConflictResolution_CONFLICT_RESOLUTION_TXN_ROLLED_BACK
		}
		if w.conflictSink != nil {
			for _, detail := range conflicts {
				w.emitConflict(detail)
			}
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if w.conflictSink != nil {
		for _, detail := range conflicts {
			w.emitConflict(detail)
		}
	}
	return true, nil
}
