package mysqlout

import (
	"context"
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
)

func (w *Writer) applyParallelBatch(ctx context.Context, batch *iproto.DataBatch) (iproto.BatchApplyResult, error) {
	workers := append([]*Writer{w}, w.workers...)
	reports := make([][]*iproto.ConflictEventDetail, len(batch.Txns))
	indices := make(map[*iproto.Txn]int, len(batch.Txns))
	for i, txn := range batch.Txns {
		if _, exists := indices[txn]; exists {
			return iproto.BatchApplyResult{}, errors.New("mysqlout: repeated transaction object in replay batch")
		}
		indices[txn] = i
	}
	sink := w.conflictSink
	result, err := scheduleReplay(ctx, batch.Txns, len(workers), w.opts.WritePolicy.GetOrderedCommit(), func(jobCtx context.Context, index int, txn *iproto.Txn, permit func(context.Context) error) (applied bool, applyErr error) {
		worker := workers[index]
		previousSink := worker.conflictSink
		worker.conflictSink = func(detail *iproto.ConflictEventDetail) {
			reports[indices[txn]] = append(reports[indices[txn]], detail)
		}
		defer func() {
			worker.conflictSink = previousSink
			if recover() != nil {
				worker.broken = true
				applyErr = errors.New("mysqlout: target replay writer panicked")
			}
			worker.observeConnectionError(applyErr)
		}()
		if err := worker.ensureConnection(jobCtx); err != nil {
			return false, err
		}
		if worker.restoreForeignKeys {
			if _, err := worker.conn.ExecContext(jobCtx, "SET SESSION foreign_key_checks = ?", worker.opts.foreignKeyChecksValue()); err != nil {
				return false, err
			}
			worker.restoreForeignKeys = false
		}
		return worker.applyTxnCommit(jobCtx, txn, permit)
	})
	// Workers mask their independent diagnostic copies. Only the coordinator
	// invokes the downstream sink, after every worker returned ownership.
	if sink != nil {
		for _, group := range reports {
			for _, detail := range group {
				sink(detail)
			}
		}
	}
	return result, err
}
