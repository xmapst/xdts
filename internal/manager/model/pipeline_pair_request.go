package model

import "github.com/xmapst/xdts/pkg/uuidx"

// PipelinePairRequest permanently identifies an atomic bidirectional creation.
// It stores references and a digest; credentials and full rules stay elsewhere.
type PipelinePairRequest struct {
	Base
	RequestedBy uuidx.UUID `gorm:"column:requested_by;type:binary(16);not null"`
	Digest      string     `gorm:"column:digest;type:char(64);not null"`
	ForwardID   uuidx.UUID `gorm:"column:forward_id;type:binary(16);not null"`
	ReverseID   uuidx.UUID `gorm:"column:reverse_id;type:binary(16);not null"`
}

func (PipelinePairRequest) TableName() string { return "pipeline_pair_requests" }
