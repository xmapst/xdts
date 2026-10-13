package iproto

import (
	"context"

	"github.com/xmapst/xdts/internal/rule"
)

type TargetSourceTable struct{ Schema, Table string }

// Mapping dictionaries have independent ownership and are never modified by
// the sender after delivery. They use source names before remapping.
type TargetRuntimeConfig struct {
	ConfigVersion       uint64
	Schemas             map[string]string
	Tables              map[TargetSourceTable]string
	Columns             map[TargetSourceTable]map[string]string
	MaxOriginHops       uint32
	DatabaseRoute       DatabaseLineageRoute
	ErrorPolicy         TargetErrorPolicy
	Transforms          []rule.TransformRule
	SourceCaseSensitive bool
	TargetCaseSensitive bool
	WritePolicy         *WritePolicy
	PublicationWindow   *IncrementalWindowPolicy
	ShadowTables        []*ShadowTableRule
}

// The response is delayed until the previous complete batch is applied and
// acknowledged. A partial transaction retains its original mapping.
type TargetRuntimeUpdate struct {
	Context    context.Context
	PipelineID string
	RunID      uint64
	InstanceID string
	Config     TargetRuntimeConfig
	Resource   *PreparedTargetResource
	// Barrier is used by the snapshot coordinator. Ready means the old batch
	// has finished; Commit authorizes the change. Done releases the coordinator.
	Barrier *TargetRuntimeBarrier
}

type TargetRuntimeBarrier struct {
	Ready  chan<- error
	Commit <-chan struct{}
	Done   chan<- error
}

type SnapshotTargetRuntimeUpdate struct {
	Update            TargetRuntimeUpdate
	MaxRows, MaxBytes uint64
	WriteConcurrency  uint32
}
