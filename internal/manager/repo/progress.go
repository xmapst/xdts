package repo

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const ProgressSampleInterval = 5 * time.Second

var ErrProgressOwnership = errors.New("repo: pipeline progress ownership or run mismatch")

// ValidateProgressOwnershipTx locks the current pipeline before any progress,
// snapshot or history write. Authentic stale runs are ignored; wrong nodes are
// rejected even when their run is old. Use within a caller-owned transaction.
func ValidateProgressOwnershipTx(ctx context.Context, tx *gorm.DB, r model.PipelineProgressRecord) (bool, error) {
	if err := r.Validate(); err != nil {
		return false, err
	}
	var p model.Pipeline
	if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id = ?", r.PipelineID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, ErrPipelineNotFound
		}
		return false, err
	}
	if (r.Side == model.SideSource && p.SourceNodeID != r.NodeID) || (r.Side == model.SideTarget && p.TargetNodeID != r.NodeID) || r.RunID > p.RunID {
		return false, ErrProgressOwnership
	}
	return r.RunID == p.RunID, nil
}

// RecordPipelineProgress retains every newer observation, and appends the first
// observation at least five seconds after the last history sample. Both writes
// commit together; the durable cursor survives restarts and concurrent consumers.
// It is safe to call inside the snapshot handler's existing transaction.
func RecordPipelineProgress(ctx context.Context, db *gorm.DB, r model.PipelineProgressRecord) error {
	r.ObservedAt = r.ObservedAt.UTC().Truncate(time.Millisecond)
	raw, err := r.EncodeValues()
	if err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := ValidateProgressOwnershipTx(ctx, tx, r)
		if err != nil || !current {
			return err
		}
		var previous model.PipelineProgressLatest
		err = tx.Where("pipeline_id = ? AND side = ?", r.PipelineID, r.Side).First(&previous).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		sameRun := err == nil && previous.RunID == r.RunID
		if sameRun && !r.ObservedAt.After(previous.ObservedAt) {
			return nil
		}
		latest := model.PipelineProgressLatest{PipelineID: r.PipelineID, Side: r.Side, NodeID: r.NodeID, RunID: r.RunID, ObservedAt: r.ObservedAt, Values: raw}
		if sameRun {
			latest.LastSampleAt = previous.LastSampleAt
		}
		if latest.LastSampleAt == nil || r.ObservedAt.Sub(*latest.LastSampleAt) >= ProgressSampleInterval {
			v := r.Values
			conflicts := int64(v.ConflictCount)
			row := model.PipelineMetric{Ts: r.ObservedAt, PipelineID: r.PipelineID, DelayMs: int64(math.Round(v.LagMS)), RPS: v.RowsPerSec, BPS: v.BytesPerSec, QueueDepth: int64(v.DataQueueBacklog), Errors: int64(v.ErrorCount), Side: &r.Side, RunID: &r.RunID, NodeID: &r.NodeID, TPS: &v.TxnsPerSec, Conflicts: &conflicts, Values: raw}
			if err := InsertPipelineMetrics(ctx, tx, []model.PipelineMetric{row}); err != nil {
				return err
			}
			sampled := r.ObservedAt
			latest.LastSampleAt = &sampled
		}
		return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "pipeline_id"}, {Name: "side"}}, DoUpdates: clause.AssignmentColumns([]string{"node_id", "run_id", "observed_at", "last_sample_at", "values_json"})}).Create(&latest).Error
	})
}

func ListPipelineProgress(ctx context.Context, db *gorm.DB, id uuidx.UUID, runID uint64) ([]model.PipelineProgressLatest, error) {
	rows := []model.PipelineProgressLatest{}
	err := db.WithContext(ctx).Where("pipeline_id = ? AND run_id = ?", id, runID).Order("side ASC").Find(&rows).Error
	return rows, err
}

// ListPipelineMetricHistory only returns observations assigned to this run.
func ListPipelineMetricHistory(ctx context.Context, db *gorm.DB, id uuidx.UUID, runID uint64, from, to time.Time, page, size int, activeOnly bool) ([]model.PipelineMetric, int64, error) {
	if page < 1 || size < 1 || size > 200 || page-1 > int(^uint(0)>>1)/size || from.IsZero() || !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		return nil, 0, errors.New("repo: invalid progress history range or page")
	}
	q := db.WithContext(ctx).Model(&model.PipelineMetric{}).Where("pipeline_id = ? AND run_id = ? AND ts >= ? AND ts < ?", id, runID, from, to)
	if activeOnly {
		q = q.Where("rps > 0 OR tps > 0")
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	rows := []model.PipelineMetric{}
	err := q.Order("ts DESC, id DESC").Offset((page - 1) * size).Limit(size).Find(&rows).Error
	return rows, total, err
}
