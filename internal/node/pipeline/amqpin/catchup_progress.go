package amqpin

import (
	"math"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

// pendingCatchupTransactions counts H minus consumed, preserving gaps and
// tagged UUID intervals. Work scales with intervals, never with GTID numbers.
func pendingCatchupTransactions(high, consumed gmysql.GTIDSet) uint64 {
	if high == nil {
		return 0
	}
	remaining := withoutRejected(high, consumed).(*gmysql.MysqlGTIDSet)
	var total uint64
	for _, tags := range *remaining {
		for _, intervals := range tags {
			for _, span := range intervals {
				count := uint64(span.Stop - span.Start)
				if count > math.MaxUint64-total {
					return math.MaxUint64
				}
				total += count
			}
		}
	}
	return total
}

// Called by startup before the consumer runs, then only by its handler. The
// report owns a fresh value; the final report retains totals with zero pending.
func (m *Module) pipelineProgress() *iproto.PipelineProgress {
	progress := buildPipelineProgress(m.opts.PipelineID, m.appliedGTIDSetString(), m.now().UnixMilli())
	progress.RunId = m.opts.RunID
	if m.catchupTotalChunks > 0 {
		progress.Snapshot = &iproto.SnapshotProgress{
			TotalChunks:          m.catchupTotalChunks,
			DoneChunks:           m.catchupTotalChunks,
			CatchupRemainingTxns: pendingCatchupTransactions(m.catchupHigh, m.consumedSet),
		}
		if m.recoveryPending {
			// A consumed watermark is not recovery completion. Keep Manager's
			// convergence observation pending until the replacement is complete.
			progress.Snapshot.CatchupRemainingTxns = max(1, progress.Snapshot.CatchupRemainingTxns)
		}
		if m.catchupExtensionSequence > 0 {
			progress.Snapshot.TotalChunks = m.catchupInitialTotal
			progress.Snapshot.DoneChunks = m.catchupInitialTotal
			progress.Snapshot.Extension = &iproto.SnapshotPlanExtensionProgress{Sequence: m.catchupExtensionSequence, TotalChunks: m.catchupTotalChunks, DoneChunks: m.catchupTotalChunks}
		}
	}
	return progress
}
