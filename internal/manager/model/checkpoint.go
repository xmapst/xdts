package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// PipelineCheckpoint 保存各流水线侧的最新位点。按 (pipeline_id, side) upsert，
// 比较 EventTime 拒绝较旧观测；不使用通用行版本号。
type PipelineCheckpoint struct {
	Base

	PipelineID uuidx.UUID `gorm:"column:pipeline_id;type:binary(16);not null;uniqueIndex:uk_pipeline_checkpoints_pipeline_side,priority:1" json:"pipeline_id"`
	Side       Side       `gorm:"column:side;type:varchar(8);not null;uniqueIndex:uk_pipeline_checkpoints_pipeline_side,priority:2" json:"side"`

	// ReadGTIDSet/PublishedGTIDSet/AppliedGTIDSet 分别表示三级位点
	// （read/published/applied），GTID 集合用 text 承载。
	ReadGTIDSet      string `gorm:"column:read_gtid;type:text" json:"read_gtid"`
	PublishedGTIDSet string `gorm:"column:published_gtid;type:text" json:"published_gtid"`
	AppliedGTIDSet   string `gorm:"column:applied_gtid;type:text" json:"applied_gtid"`

	// Epoch 用于区分源端发布会话，BatchSeq 在同一 epoch 内连续递增。
	Epoch    uint64 `gorm:"column:epoch;not null;default:0" json:"epoch"`
	BatchSeq uint64 `gorm:"column:batch_seq;not null;default:0" json:"batch_seq"`

	// EventTime 是上报消息自带的时间戳，MGR-MON-08 幂等消费按
	// (pipeline_id, side, EventTime) 取较新者、丢弃更旧的重复投递。
	EventTime time.Time `gorm:"column:event_time;not null" json:"event_time"`
}

// TableName 显式指定表名。
func (PipelineCheckpoint) TableName() string { return "pipeline_checkpoints" }

// Validate 校验枚举字段。
func (c *PipelineCheckpoint) Validate() error {
	if !c.Side.Valid() {
		return fmt.Errorf("model: 未知的 side %q", c.Side)
	}
	return nil
}
