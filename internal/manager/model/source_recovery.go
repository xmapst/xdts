package model

import "github.com/xmapst/xdts/pkg/uuidx"

// SourceRecoveryState owns the durable epoch ceiling independently of run and
// position changes. Checkpoint is either an authenticated source publication or
// its explicit initial baseline saved with the first durable epoch lease.
type SourceRecoveryState struct {
	PipelineID   uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);primaryKey"`
	EpochCeiling uint64     `gorm:"column:epoch_ceiling;not null"`
	Checkpoint   JSON       `gorm:"column:checkpoint_json;type:json"`
}

func (SourceRecoveryState) TableName() string { return "source_recovery_states" }

// Every range request has a durable result, making a lost reply retry harmless.
type SourceEpochLease struct {
	RequestID     uuidx.UUID `gorm:"column:request_id;type:binary(16);primaryKey"`
	PipelineID    uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null"`
	RequestDigest string     `gorm:"column:request_digest;not null"`
	Reply         JSON       `gorm:"column:reply_json;type:json;not null"`
}

func (SourceEpochLease) TableName() string { return "source_epoch_leases" }
