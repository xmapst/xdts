package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// InitMode 是 pipeline 的初始化方式。
//
// 除 InitModeSchemaOnly（仅结构迁移）外，其余方式均可叠加结构迁移，是否
// 叠加由策略开关控制，不体现在本枚举中。
type InitMode string

const (
	InitModeFullAndIncremental       InitMode = "full_and_incremental"        // 全量 + 增量（默认）
	InitModeIncrementalOnlyLatest    InitMode = "incremental_only_latest"     // 仅增量：当前最新
	InitModeIncrementalOnlyGTIDSet   InitMode = "incremental_only_gtid_set"   // 仅增量：指定 GTID 集合
	InitModeIncrementalOnlyBinlogPos InitMode = "incremental_only_binlog_pos" // 仅增量：指定 binlog 文件 + 偏移
	InitModeIncrementalOnlyTimestamp InitMode = "incremental_only_timestamp"  // 仅增量：指定时间点（P1）
	InitModeFullOnly                 InitMode = "full_only"                   // 仅全量：追平到 H 后停止（P1）
	InitModeSchemaOnly               InitMode = "schema_only"                 // 仅结构迁移，不同步数据（P1）
)

// Valid 校验初始化方式取值。
func (m InitMode) Valid() bool {
	switch m {
	case InitModeFullAndIncremental, InitModeIncrementalOnlyLatest, InitModeIncrementalOnlyGTIDSet,
		InitModeIncrementalOnlyBinlogPos, InitModeIncrementalOnlyTimestamp, InitModeFullOnly, InitModeSchemaOnly:
		return true
	default:
		return false
	}
}

// DesiredState 是用户可设置的期望状态（期望状态只有
// Stopped/Running/Paused）。
type DesiredState string

const (
	DesiredStateRunning DesiredState = "running"
	DesiredStatePaused  DesiredState = "paused"
	DesiredStateStopped DesiredState = "stopped"
)

// Valid 校验期望状态取值。
func (s DesiredState) Valid() bool {
	switch s {
	case DesiredStateRunning, DesiredStatePaused, DesiredStateStopped:
		return true
	default:
		return false
	}
}

// ActualState 是 pipeline 状态机的实际状态。
//
// ActualStateUnknown 对应「节点离线时实际状态记为未知，不改写」
// （需求 MGR-PL-16），是状态机图之外补充的取值。
type ActualState string

const (
	ActualStateDraft        ActualState = "draft"
	ActualStatePrechecking  ActualState = "prechecking"
	ActualStateDeploying    ActualState = "deploying"
	ActualStateMigrating    ActualState = "migrating"
	ActualStateSnapshotting ActualState = "snapshotting"
	ActualStateCatchingUp   ActualState = "catching_up"
	ActualStateRunning      ActualState = "running"
	ActualStatePaused       ActualState = "paused"
	ActualStateError        ActualState = "error"
	ActualStateStopped      ActualState = "stopped"
	ActualStateUnknown      ActualState = "unknown"
)

// Valid 校验实际状态取值。
func (s ActualState) Valid() bool {
	switch s {
	case ActualStateDraft, ActualStatePrechecking, ActualStateDeploying, ActualStateMigrating,
		ActualStateSnapshotting, ActualStateCatchingUp, ActualStateRunning, ActualStatePaused,
		ActualStateError, ActualStateStopped, ActualStateUnknown:
		return true
	default:
		return false
	}
}

type CleanupState string

const (
	CleanupStatePending    CleanupState = "pending"
	CleanupStateInProgress CleanupState = "in_progress" // 回收进行中
	CleanupStateDone       CleanupState = "done"        // 全部资源回收成功，记录才真正从「待清理」列表消失
	CleanupStateFailed     CleanupState = "failed"      // 上一次回收失败，等待重试
)

// Valid 校验资源回收整体状态取值。
func (s CleanupState) Valid() bool {
	switch s {
	case CleanupStatePending, CleanupStateInProgress, CleanupStateDone, CleanupStateFailed:
		return true
	default:
		return false
	}
}

// CleanupResourceState 是 PipelineCleanupDetail 中单个资源的回收状态。
type CleanupResourceState string

const (
	CleanupResourceStatePending CleanupResourceState = "pending"
	CleanupResourceStateDone    CleanupResourceState = "done"
	CleanupResourceStateFailed  CleanupResourceState = "failed"
)

// Valid 校验单个资源回收状态取值。
func (s CleanupResourceState) Valid() bool {
	switch s {
	case CleanupResourceStatePending, CleanupResourceStateDone, CleanupResourceStateFailed:
		return true
	default:
		return false
	}
}

// CleanupResourceDetail 记录单个资源的回收进度和最近错误，供节点和流水线共用。
type CleanupResourceDetail struct {
	State     CleanupResourceState `json:"state"`
	LastError string               `json:"last_error,omitempty"`
}

// CleanupDetail 按资源保存回收结果，重试时跳过已完成项。
type PipelineCleanupDetail struct {
	Queue        CleanupResourceDetail `json:"queue"`         // 数据队列（需求 MGR-PL-07）
	ServerID     CleanupResourceDetail `json:"server_id"`     // dump 用 binlog_server_id 号段（需求 MGR-PL-06）
	SnapshotUUID CleanupResourceDetail `json:"snapshot_uuid"` // 全量合成来源 UUID（ADR-03）
	SchemaUUID   CleanupResourceDetail `json:"schema_uuid"`   // 结构迁移合成来源 UUID（ADR-03）
}

// Pipeline 是一条源端到目标端的同步链路定义（需求
// MGR-PL-*）。
type Pipeline struct {
	Base

	Name       string     `gorm:"column:name;type:varchar(128);not null;uniqueIndex:uk_pipelines_name" json:"name"`
	TopologyID uuidx.UUID `gorm:"column:topology_id;type:binary(16);not null;index:idx_pipelines_topology_id" json:"topology_id"`

	// SourceEndpointID and TargetEndpointID reference independent connection endpoints.
	SourceEndpointID uuidx.UUID `gorm:"column:source_endpoint_id;type:binary(16);not null;index:idx_pipelines_source_endpoint_id" json:"source_endpoint_id"`
	SourceNodeID     uuidx.UUID `gorm:"column:source_node_id;type:binary(16);not null" json:"source_node_id"`
	TargetEndpointID uuidx.UUID `gorm:"column:target_endpoint_id;type:binary(16);not null;index:idx_pipelines_target_endpoint_id" json:"target_endpoint_id"`
	TargetNodeID     uuidx.UUID `gorm:"column:target_node_id;type:binary(16);not null" json:"target_node_id"`

	StrategyID      uuidx.UUID `gorm:"column:strategy_id;type:binary(16);not null" json:"strategy_id"`
	StrategyVersion uint32     `gorm:"column:strategy_version;not null" json:"strategy_version"`

	InitMode InitMode `gorm:"column:init_mode;type:varchar(32);not null" json:"init_mode"`
	// StartPosition 按 InitMode 承载不同结构：GTID 集合字符串、
	// {file,offset}、时间点等，具体 schema 由 service 层与节点协商。
	//
	// 已验证：上述两个函数存在且确实读这个字段、协议字段名。
	// 未验证：除这两跳之外是否还有第三方按别的 schema 解释同一份 JSON。
	StartPosition JSON `gorm:"column:start_position;type:json" json:"start_position,omitempty"`

	// RunID 仅「重新初始化」时递增，并分配新的全量与结构迁移合成 UUID
	RunID uint64 `gorm:"column:run_id;not null;default:0" json:"run_id"`

	SnapshotUUID uuidx.UUID `gorm:"column:snapshot_uuid;type:binary(16)" json:"snapshot_uuid"`
	SchemaUUID   uuidx.UUID `gorm:"column:schema_uuid;type:binary(16)" json:"schema_uuid"`
	// ReinitPending is command execution state, independent of actual lifecycle
	// states. It remains set until both assigned sides acknowledge the new run.
	ReinitPending bool `gorm:"column:reinit_pending;not null;default:false" json:"reinit_pending"`

	// BinlogServerID 是源端 dump 用的复制协议 server_id，全局唯一，从可配
	// 号段分配；仅结构迁移或非源端角色时可能未分配。
	//
	// 唯一索引在 CleanupState 变为 CleanupStateDone 之前不得释放（不得置
	// nil 后又被新 pipeline 复用同一号段）：号段复用必须等真实回收完成，
	// 否则新 pipeline 会与仍挂在源库上的旧 dump 连接撞号。
	// 本层只保证唯一索引存在。
	//
	// 实际保证者是两处，都不在「service 层」：
	//  1. 门在仓储层自己：repo.RecordPipelineCleanupAttempt 里的
	//     `if newState == model.CleanupStateDone { updates["binlog_server_id"] = nil }`
	//     ——只有回收真正完成才置 nil；
	//  2. 唯一生产驱动方是 internal/manager/reclaim 的回收循环
	//     （internal/manager/reclaim/reclaim.go 里对 RecordPipelineCleanupAttempt 的调用）。
	BinlogServerID *uint32 `gorm:"column:binlog_server_id;uniqueIndex:uk_pipelines_binlog_server_id" json:"binlog_server_id,omitempty"`

	DesiredState DesiredState `gorm:"column:desired_state;type:varchar(16);not null;index:idx_pipelines_desired_source_actual_state,priority:1;index:idx_pipelines_desired_target_actual_state,priority:1" json:"desired_state"`

	// SourceActualState and TargetActualState are independent observed role states.
	// Each last-error field belongs to its role. Unknown means no current observation;
	// display state is derived by DeriveDisplayState rather than stored independently.
	SourceActualState ActualState `gorm:"column:source_actual_state;type:varchar(16);not null;index:idx_pipelines_desired_source_actual_state,priority:2" json:"source_actual_state"`
	SourceLastError   string      `gorm:"column:source_last_error;type:text" json:"source_last_error"`
	TargetActualState ActualState `gorm:"column:target_actual_state;type:varchar(16);not null;index:idx_pipelines_desired_target_actual_state,priority:2" json:"target_actual_state"`
	TargetLastError   string      `gorm:"column:target_last_error;type:text" json:"target_last_error"`

	// PositionRevision advances only after an explicit confirmed start change.
	// It is reset by REINIT and applied once against the source checkpoint.
	PositionRevision uint64 `gorm:"column:position_revision;not null;default:0" json:"position_revision"`

	// ConfigVersion 是配置乐观锁：任何配置字段变化即递增，节点拒绝版本
	// 倒退；本表不再另设通用 lock_version 字段。
	ConfigVersion uint64 `gorm:"column:config_version;not null;default:0" json:"config_version"`

	// LastCmdSeq is the latest allocated pipeline command identity.
	// Retries reuse it; a new lifecycle intent advances it. It is independent of the
	// configuration version and each role's observed command result.
	LastCmdSeq uint64 `gorm:"column:last_cmd_seq;not null;default:0" json:"last_cmd_seq"`

	// RecoveryExhaustedCmdSeq identifies the exhausted lifecycle command.
	// Nil means no record; zero is valid. Dispatch owns writes and interpretation.
	RecoveryExhaustedCmdSeq *uint64 `gorm:"column:recovery_exhausted_cmd_seq" json:"recovery_exhausted_cmd_seq"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index;index:idx_pipelines_deleted_cleanup_state,priority:1" json:"-"`

	// CleanupState 是资源回收整体进度。「待清理」列表 = deleted_at IS NOT
	// NULL AND cleanup_state <> 'done'。默认值 'done'：
	// 一条从未被删除的 pipeline 没有待回收的资源。
	CleanupState CleanupState `gorm:"column:cleanup_state;type:varchar(16);not null;default:'done';index:idx_pipelines_deleted_cleanup_state,priority:2" json:"cleanup_state"`
	// Only authenticated UNLOAD receipts for the deletion revision write these.
	SourceCleanupConfirmedAt *time.Time `gorm:"column:source_cleanup_confirmed_at" json:"-"`
	TargetCleanupConfirmedAt *time.Time `gorm:"column:target_cleanup_confirmed_at" json:"-"`
	SourceCleanupError       string     `gorm:"column:source_cleanup_error;type:text" json:"-"`
	TargetCleanupError       string     `gorm:"column:target_cleanup_error;type:text" json:"-"`
	// CleanupDetail 逐资源记录回收状态（结构见 PipelineCleanupDetail），
	// 供重试时跳过已回收的资源，保证幂等。
	CleanupDetail JSON `gorm:"column:cleanup_detail;type:json" json:"cleanup_detail,omitempty"`
	// CleanupAttempts tracks the persistent retry budget for resource cleanup.
	CleanupAttempts int `gorm:"column:cleanup_attempts;not null;default:0" json:"cleanup_attempts"`
	// CleanupLastError 是最近一次回收失败原因。
	//
	// The dedicated cleanup API exposes a redacted diagnostic; the ordinary
	// pipeline view does not serialize internal cleanup fields.
	CleanupLastError string `gorm:"column:cleanup_last_error;type:text" json:"cleanup_last_error,omitempty"`

	CleanupLastAttemptAt *time.Time `gorm:"column:cleanup_last_attempt_at" json:"cleanup_last_attempt_at,omitempty"`
}

// TableName 显式指定表名。
func (Pipeline) TableName() string { return "pipelines" }

// Validate 校验枚举字段。
func (p *Pipeline) Validate() error {
	if !p.InitMode.Valid() {
		return fmt.Errorf("model: 未知的初始化方式 %q", p.InitMode)
	}
	if !p.DesiredState.Valid() {
		return fmt.Errorf("model: 未知的期望状态 %q", p.DesiredState)
	}

	if !p.SourceActualState.Valid() {
		return fmt.Errorf("model: 未知的源端实际状态 %q", p.SourceActualState)
	}
	if !p.TargetActualState.Valid() {
		return fmt.Errorf("model: 未知的目标端实际状态 %q", p.TargetActualState)
	}
	if !p.CleanupState.Valid() {
		return fmt.Errorf("model: 未知的资源回收状态 %q", p.CleanupState)
	}
	return nil
}

// displayStateAggregationTier 给 DeriveDisplayState 用的聚合优先级——
// 数值越小越优先被选中。只是聚合计算的内部排序，不是状态机拓扑的另一种
// 表达，不代表"进度"：中间那 6 个进行中状态（Draft/Prechecking/
// Deploying/Migrating/Snapshotting/CatchingUp）故意给同一个 tier，
// 彼此谁先谁后对聚合结果没有意义上的区别，见 DeriveDisplayState 文档。
func displayStateAggregationTier(s ActualState) int {
	switch s {
	case ActualStateError:
		return 0
	case ActualStateUnknown:
		return 1
	case ActualStateStopped:
		return 3
	case ActualStatePaused:
		return 4
	case ActualStateRunning:
		return 5
	default: // Draft/Prechecking/Deploying/Migrating/Snapshotting/CatchingUp：进行中，互相不分先后
		return 2
	}
}

// DeriveDisplayState combines current source and target observations and errors.
// Callers use this pure projection for display; lifecycle reconciliation compares
// each role's observed state with its own desired state.
func (p *Pipeline) DeriveDisplayState() (ActualState, string) {
	if displayStateAggregationTier(p.SourceActualState) <= displayStateAggregationTier(p.TargetActualState) {
		return p.SourceActualState, p.SourceLastError
	}
	return p.TargetActualState, p.TargetLastError
}
