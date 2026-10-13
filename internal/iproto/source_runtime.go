package iproto

import (
	"context"

	"github.com/xmapst/xdts/internal/rule"
)

// These plans are compiled before delivery and remain immutable. Each message
// transfers an independently constructed plan; no actor-owned maps are shared.
type ISourceTablePlan interface{ MatchTable(string, string) bool }
type ISourceColumnPlan interface {
	Allow(string, string, string) bool
}
type ISourceRowPlan interface {
	HasRowFilters(string, string) bool
	RowFilterColumns(string, string) []string
	MatchRow(string, string, []string, []any) (bool, error)
}
type ISourceOriginPlan interface {
	Resolve(string) (string, []string, uint32, bool)
}
type ISourceOriginFilter interface {
	AllowOrigin(string, uint32, []string) bool
}

type ISourceEndpointPlan interface {
	ResolveEndpoint(string) (string, bool)
}

// SourceRuntimeConfig is owned by the serial binlog session after acceptance.
type SourceRuntimeConfig struct {
	ConfigVersion     uint64
	TableFilter       ISourceTablePlan
	ColumnFilter      ISourceColumnPlan
	RowFilter         ISourceRowPlan
	Transforms        *rule.TransformPlan
	DDLColumnPolicy   *DDLColumnPolicy
	OriginResolver    ISourceOriginPlan
	OriginFilter      ISourceOriginFilter
	TargetCenter      string
	DatabaseRoute     DatabaseLineageRoute
	MaxOriginHops     uint32
	DropUnknownOrigin bool
	RowsPerSec        uint64
	BytesPerSec       uint64
	PublicationWindow *IncrementalWindowPolicy
	ShadowTablePolicy *ShadowTablePolicy
}

// SourceRuntimeUpdate is a local, expiring request for one existing instance.
// The response is delayed until all fragments of the previous transaction have
// been published and the new configuration has actually become active.
type SourceRuntimeUpdate struct {
	Context    context.Context
	PipelineID string
	RunID      uint64
	InstanceID string
	Config     SourceRuntimeConfig
	Barrier    *SourceRuntimeBarrier
}

type SourceRuntimeBarrier struct {
	Ready  chan<- error
	Commit <-chan struct{}
	Done   chan<- error
}

type SnapshotSourceRuntimeUpdate struct {
	Update                  SourceRuntimeUpdate
	ReadConcurrency         uint32
	MaxRows, MaxBytes       uint64
	RowsPerSec, BytesPerSec uint64
}
