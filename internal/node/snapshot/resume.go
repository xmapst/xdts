package snapshot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type IResumePlanReader interface {
	ReadPendingPlan(context.Context, Plan, []Table, func(context.Context, Chunk) (uint64, bool, error), func(context.Context, ChunkData) error) error
}

// Each acknowledgement gets an immutable record. A timed-out older writer
// cannot replace progress made by a subsequent resume workflow.
type publishedChunk struct {
	PipelineID string `bson:"pipeline_id"`
	RunID      uint64 `bson:"run_id"`
	UUID       string `bson:"uuid"`
	Number     uint64 `bson:"number"`
	Schema     string `bson:"schema"`
	Table      string `bson:"table"`
	Rows       uint64 `bson:"rows"`
}

func (r *Runner) chunkRecordID(number uint64) string {
	if r.opts.Recovery != nil {
		return r.recordID(fmt.Sprintf("published.%d", number))
	}
	return fmt.Sprintf("%d:%s:%d.published.%d", len(r.opts.PipelineID), r.opts.PipelineID, r.opts.RunID, number)
}
func (r *Runner) published(ctx context.Context, chunk Chunk) (uint64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	var saved publishedChunk
	err := r.store.Get(ctx, "snapshot_chunks", r.chunkRecordID(chunk.Number), &saved)
	if errors.Is(err, xdoc.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if saved.PipelineID != r.opts.PipelineID || saved.RunID != r.opts.RunID || saved.UUID != r.opts.SnapshotUUID || saved.Number != chunk.Number || saved.Schema != chunk.Schema || saved.Table != chunk.Table || saved.Rows > 1<<63-1 {
		return 0, false, fmt.Errorf("snapshot: invalid published chunk record")
	}
	return saved.Rows, true, nil
}

func (r *Runner) recordProgress(chunk Chunk, rows uint64) {
	r.done++
	key := fmt.Sprintf("%d:%s%s", len(chunk.Schema), chunk.Schema, chunk.Table)
	r.tableDone[key]++
	for _, table := range r.progress {
		if table.GetSchema() != chunk.Schema || table.GetTable() != chunk.Table {
			continue
		}
		if rows > uint64(1<<63-1-table.DoneRows) {
			table.DoneRows = 1<<63 - 1
		} else {
			table.DoneRows += int64(rows)
		}
		done := r.tableDone[key]
		table.DoneChunks = &done
		if elapsed := time.Since(r.rateAt).Seconds(); elapsed > 0 {
			if newRows, known := r.tableReadRows[key]; known {
				rate := float64(newRows) / elapsed
				table.RowsPerSec = &rate
			}
		}
		table.Status = iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_READING
		for _, planned := range r.plan.Tables {
			if planned.Schema == chunk.Schema && planned.Table == chunk.Table && r.tableDone[key] == planned.ChunkCount {
				table.Status = iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_DONE
				break
			}
		}
		break
	}
	if r.opts.Report == nil {
		return
	}
	if r.done != r.plan.Total && !r.lastReport.IsZero() && time.Since(r.lastReport) < time.Second {
		return
	}
	r.lastReport = time.Now()
	if r.done == r.plan.Total {
		for _, table := range r.progress {
			table.Status = iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_DONE
		}
	}
	progress := &iproto.SnapshotProgress{TotalChunks: r.planner.initialTotal, DoneChunks: min(r.done, r.planner.initialTotal), Tables: r.progress}
	if r.planner.extensions > 0 {
		progress.Extension = &iproto.SnapshotPlanExtensionProgress{Sequence: r.planner.extensions, TotalChunks: r.plan.Total, DoneChunks: r.done}
	}
	r.opts.Report(proto.Clone(progress).(*iproto.SnapshotProgress))
	if r.opts.RateReport != nil {
		elapsed := time.Since(r.rateAt).Seconds()
		if elapsed > 0 {
			r.opts.RateReport(float64(r.rateRows)/elapsed, float64(r.rateBytes)/elapsed)
		}
	}
}
