package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// ProgressValues retains every diagnostic in a progress report. A nil snapshot
// or absent future optional observation stays unknown rather than becoming zero.
type ProgressValues struct {
	ReadGTIDSet         string          `json:"readGtidSet"`
	PublishedGTIDSet    string          `json:"publishedGtidSet"`
	AppliedGTIDSet      string          `json:"appliedGtidSet"`
	SourceEventTSMS     int64           `json:"sourceEventTsMs"`
	LagMS               float64         `json:"lagMs"`
	RowsPerSec          float64         `json:"rowsPerSec"`
	TxnsPerSec          float64         `json:"txnsPerSec"`
	BytesPerSec         float64         `json:"bytesPerSec"`
	InflightBatches     uint32          `json:"inflightBatches"`
	InflightBytes       uint64          `json:"inflightBytes"`
	ModuleQueues        []ProgressQueue `json:"moduleQueues"`
	DataQueueBacklog    uint64          `json:"dataQueueBacklog"`
	ErrorCount          uint64          `json:"errorCount"`
	ConflictCount       uint64          `json:"conflictCount"`
	Snapshot            JSON            `json:"snapshot"`
	MetricsObservedTSMS *int64          `json:"metricsObservedTsMs"`
	QueueObservedTSMS   *int64          `json:"queueObservedTsMs"`
}

type ProgressQueue struct {
	Module string `json:"module"`
	Len    uint32 `json:"len"`
	Cap    uint32 `json:"cap"`
}

type PipelineProgressRecord struct {
	PipelineID uuidx.UUID
	NodeID     uuidx.UUID
	Side       Side
	RunID      uint64
	ObservedAt time.Time
	Values     ProgressValues
}

func (r PipelineProgressRecord) Validate() error {
	if r.PipelineID.IsZero() || r.NodeID.IsZero() || (r.Side != SideSource && r.Side != SideTarget) || r.ObservedAt.IsZero() || r.ObservedAt.UnixMilli() <= 0 || r.ObservedAt.Year() > 9999 {
		return errors.New("model: invalid progress identity or observation time")
	}
	v := r.Values
	for _, observed := range []*int64{v.MetricsObservedTSMS, v.QueueObservedTSMS} {
		if observed != nil && (*observed <= 0 || *observed > r.ObservedAt.UnixMilli()) {
			return errors.New("model: invalid metric observation time")
		}
	}
	for _, n := range []float64{v.LagMS, v.RowsPerSec, v.TxnsPerSec, v.BytesPerSec} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
			return errors.New("model: progress rates and delay must be finite and nonnegative")
		}
	}
	if v.LagMS >= math.Exp2(63) || v.DataQueueBacklog > math.MaxInt64 || v.ErrorCount > math.MaxInt64 || v.ConflictCount > math.MaxInt64 || v.SourceEventTSMS < 0 {
		return errors.New("model: progress count or delay exceeds storage range")
	}
	for _, position := range []string{v.ReadGTIDSet, v.PublishedGTIDSet, v.AppliedGTIDSet} {
		if len(position) > 1024*1024 {
			return errors.New("model: progress position exceeds size limit")
		}
		if position != "" {
			if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, position); err != nil {
				return errors.New("model: invalid progress GTID set")
			}
		}
	}
	if len(v.ModuleQueues) > 128 {
		return errors.New("model: too many module queue observations")
	}
	names := make(map[string]bool, len(v.ModuleQueues))
	for _, q := range v.ModuleQueues {
		if q.Module == "" || len(q.Module) > 255 || q.Len > q.Cap || names[q.Module] {
			return errors.New("model: invalid or duplicate module queue observation")
		}
		names[q.Module] = true
	}
	if len(v.Snapshot) > 4*1024*1024 || (len(v.Snapshot) != 0 && !json.Valid(v.Snapshot)) {
		return errors.New("model: invalid snapshot progress JSON")
	}
	return nil
}

// EncodeValues allocates an independent JSON value for database ownership.
func (r PipelineProgressRecord) EncodeValues() (JSON, error) {
	if err := r.Validate(); err != nil {
		return nil, err
	}
	v := r.Values
	if v.ModuleQueues == nil {
		v.ModuleQueues = []ProgressQueue{}
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("model: encode pipeline progress: %w", err)
	}
	return JSON(raw), nil
}

// The primary key separates source/target, including a node hosting both sides.
type PipelineProgressLatest struct {
	PipelineID   uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);primaryKey"`
	Side         Side       `gorm:"column:side;primaryKey"`
	NodeID       uuidx.UUID `gorm:"column:node_id;type:binary(16);not null"`
	RunID        uint64     `gorm:"column:run_id;not null"`
	ObservedAt   time.Time  `gorm:"column:observed_at;not null"`
	LastSampleAt *time.Time `gorm:"column:last_sample_at"`
	Values       JSON       `gorm:"column:values_json;type:json;not null"`
}

func (PipelineProgressLatest) TableName() string { return "pipeline_progress_latest" }
