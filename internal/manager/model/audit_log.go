package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// AuditAction identifies persisted audit operations. Published values are append-only.
type AuditAction string

const (
	AuditActionLogin AuditAction = "login" // 登录成功

	AuditActionLoginFailure        AuditAction = "login_failure"
	AuditActionLogout              AuditAction = "logout"
	AuditActionRegisterCodeCreate  AuditAction = "register_code_create"
	AuditActionRegisterCodeRevoke  AuditAction = "register_code_revoke"
	AuditActionPipelineCreate      AuditAction = "pipeline_create"
	AuditActionPipelineUpdate      AuditAction = "pipeline_update"
	AuditActionPipelineStart       AuditAction = "pipeline_start"
	AuditActionPipelineStop        AuditAction = "pipeline_stop"
	AuditActionPipelinePause       AuditAction = "pipeline_pause"
	AuditActionPipelineResume      AuditAction = "pipeline_resume"
	AuditActionPipelineDelete      AuditAction = "pipeline_delete"
	AuditActionPipelineSetPosition AuditAction = "pipeline_set_position"
	AuditActionPipelineReinit      AuditAction = "pipeline_reinit"
	AuditActionPipelineSkipError   AuditAction = "pipeline_skip_error"

	AuditActionSchemaPlanConfirm   AuditAction = "schema_plan_confirm"
	AuditActionPipelineRetry       AuditAction = "pipeline_retry"
	AuditActionPipelineNodeMigrate AuditAction = "pipeline_node_migrate"
	AuditActionConfirmLoopWarning  AuditAction = "confirm_loop_warning"
	AuditActionStrategyChange      AuditAction = "strategy_change"

	AuditActionEndpointChange AuditAction = "endpoint_change"

	// center 系列对应 ：POST/PATCH/DELETE /centers[/{id}]、
	// POST/DELETE /centers/{id}/origin-uuids、PUT /centers/{id}/inspector。
	AuditActionCenterCreate           AuditAction = "center_create"
	AuditActionCenterUpdate           AuditAction = "center_update"
	AuditActionCenterDelete           AuditAction = "center_delete"
	AuditActionCenterInspectorAssign  AuditAction = "center_inspector_assign"
	AuditActionCenterOriginUUIDAdd    AuditAction = "center_origin_uuid_add"
	AuditActionCenterOriginUUIDRemove AuditAction = "center_origin_uuid_remove"

	// node 系列对应 ：PATCH /nodes/{id}（改名/改标签走
	// NodeUpdate，禁用/启用走各自专属动作，即使三者共用同一个 HTTP
	// 端点——审计要按仓储层实际调用的是 UpdateNode 还是
	// DisableNode/EnableNode 来分类，不按 HTTP 端点分类）、
	// POST /nodes/{id}/revoke、DELETE /nodes/{id}（注销）、
	// POST /nodes/{id}/log-level（NODE-RPT-04）。没有 NodeCreate——
	// 节点只能通过注册码自注册产生，没有管理员直接创建节点的入口。
	AuditActionNodeUpdate      AuditAction = "node_update"
	AuditActionNodeDisable     AuditAction = "node_disable"
	AuditActionNodeEnable      AuditAction = "node_enable"
	AuditActionNodeRevoke      AuditAction = "node_revoke"
	AuditActionNodeDelete      AuditAction = "node_delete"
	AuditActionNodeSetLogLevel AuditAction = "node_set_log_level"

	AuditActionStrategyDelete        AuditAction = "strategy_delete"
	AuditActionStrategyVersionDelete AuditAction = "strategy_version_delete"

	AuditActionTopologyChange AuditAction = "topology_change"
	AuditActionTopologyDelete AuditAction = "topology_delete"

	// 为什么必须同批加：新端点 `POST /events/{id}/ack` 按 http 包惯例要记
	// 审计（该包写操作 handler 全部记），而 `CreateAuditLog` 在写路径上调
	// `Valid()`——常量不在白名单里时整条审计写入被拒。少这一个常量不会让
	// 端点编译失败，只会让它在运行期写审计那一步报错，所以它必须和端点同批。
	AuditActionEventAck           AuditAction = "event_ack"
	AuditActionUserCreate         AuditAction = "user_create"
	AuditActionUserUpdate         AuditAction = "user_update"
	AuditActionUserPasswordReset  AuditAction = "user_password_reset"
	AuditActionUserPasswordChange AuditAction = "user_password_change"
	AuditActionUserStatusChange   AuditAction = "user_status_change"
	AuditActionUserUnlock         AuditAction = "user_unlock"
	AuditActionUserSessionsRevoke AuditAction = "user_sessions_revoke"
	AuditActionUserDelete         AuditAction = "user_delete"
	AuditActionUserGrantChange    AuditAction = "user_grant_change"
	AuditActionPrecheckConfirm    AuditAction = "precheck_confirm"
	AuditActionAlertRuleChange    AuditAction = "alert_rule_change"
	AuditActionAlertRuleDelete    AuditAction = "alert_rule_delete"
	AuditActionSettingsChange     AuditAction = "settings_change"
)

// Valid 校验审计动作取值。
func (a AuditAction) Valid() bool {
	switch a {
	case AuditActionPrecheckConfirm, AuditActionAlertRuleChange, AuditActionAlertRuleDelete, AuditActionSettingsChange:
		return true
	case AuditActionLogin, AuditActionLoginFailure, AuditActionLogout, AuditActionRegisterCodeCreate, AuditActionRegisterCodeRevoke,
		AuditActionPipelineCreate, AuditActionPipelineUpdate,
		AuditActionPipelineStart, AuditActionPipelineStop, AuditActionPipelinePause, AuditActionPipelineResume, AuditActionPipelineDelete,
		AuditActionPipelineSetPosition, AuditActionPipelineReinit, AuditActionPipelineSkipError, AuditActionPipelineRetry, AuditActionPipelineNodeMigrate, AuditActionSchemaPlanConfirm,
		AuditActionConfirmLoopWarning, AuditActionStrategyChange, AuditActionEndpointChange,
		AuditActionCenterCreate, AuditActionCenterUpdate, AuditActionCenterDelete,
		AuditActionCenterInspectorAssign, AuditActionCenterOriginUUIDAdd, AuditActionCenterOriginUUIDRemove,
		AuditActionNodeUpdate, AuditActionNodeDisable, AuditActionNodeEnable,
		AuditActionNodeRevoke, AuditActionNodeDelete, AuditActionNodeSetLogLevel,
		AuditActionStrategyDelete, AuditActionStrategyVersionDelete,
		AuditActionTopologyChange, AuditActionTopologyDelete,
		AuditActionEventAck,
		AuditActionUserCreate, AuditActionUserUpdate, AuditActionUserPasswordReset, AuditActionUserPasswordChange,
		AuditActionUserStatusChange, AuditActionUserUnlock,
		AuditActionUserSessionsRevoke, AuditActionUserDelete, AuditActionUserGrantChange:
		return true
	default:
		return false
	}
}

// AuditTargetType 是审计动作作用的对象类型。
type AuditTargetType string

const (
	AuditTargetTypeUser    AuditTargetType = "user"
	AuditTargetTypeSession AuditTargetType = "session"
	AuditTargetTypeCenter  AuditTargetType = "center"

	AuditTargetTypeEndpoint         AuditTargetType = "endpoint"
	AuditTargetTypeNode             AuditTargetType = "node"
	AuditTargetTypeNodeRegisterCode AuditTargetType = "node_register_code"
	AuditTargetTypeStrategy         AuditTargetType = "strategy"
	AuditTargetTypeTopology         AuditTargetType = "topology"
	AuditTargetTypePipeline         AuditTargetType = "pipeline"
	AuditTargetTypeSetting          AuditTargetType = "setting"

	AuditTargetTypeEvent     AuditTargetType = "event"
	AuditTargetTypeAlertRule AuditTargetType = "alert_rule"
)

// Valid 校验目标对象类型取值。
func (t AuditTargetType) Valid() bool {
	switch t {
	case AuditTargetTypeUser, AuditTargetTypeSession, AuditTargetTypeCenter, AuditTargetTypeEndpoint,
		AuditTargetTypeNode, AuditTargetTypeNodeRegisterCode, AuditTargetTypeStrategy,
		AuditTargetTypeTopology, AuditTargetTypePipeline, AuditTargetTypeSetting,
		AuditTargetTypeEvent, AuditTargetTypeAlertRule:
		return true
	default:
		return false
	}
}

// AuditLog 是关键操作审计（需求 MGR-USR-06，P1）。
//
// 不自动清理，因此不设 updated_at 与 deleted_at；LogBase 提供
// 主键与写入时间。
type AuditLog struct {
	LogBase

	Ts     time.Time  `gorm:"column:ts;not null;index:idx_audit_logs_ts" json:"ts"`
	UserID uuidx.UUID `gorm:"column:user_id;type:binary(16);not null;index:idx_audit_logs_user_id" json:"user_id"`

	Action     AuditAction     `gorm:"column:action;type:varchar(32);not null;index:idx_audit_logs_action" json:"action"`
	TargetType AuditTargetType `gorm:"column:target_type;type:varchar(32);not null" json:"target_type"`
	TargetID   uuidx.UUID      `gorm:"column:target_id;type:binary(16)" json:"target_id"`

	Detail JSON `gorm:"column:detail;type:json" json:"detail,omitempty"`
}

// TableName 显式指定表名。
func (AuditLog) TableName() string { return "audit_logs" }

// Validate 校验枚举字段。
func (a *AuditLog) Validate() error {
	if !a.Action.Valid() {
		return fmt.Errorf("model: 未知的审计动作 %q", a.Action)
	}
	if !a.TargetType.Valid() {
		return fmt.Errorf("model: 未知的审计目标类型 %q", a.TargetType)
	}
	return nil
}
