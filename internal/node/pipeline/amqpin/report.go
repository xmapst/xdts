package amqpin

import (
	"uuid"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/safeerror"
)

// buildPipelineStatus/buildPipelineProgress 是纯函数，把本包已有的状态拼成
// reporter 要求的 payload 形状，不碰 m.Cast——这样可以直接单测内容是否正确
// （pipeline_id/role/字段映射对不对），不需要经过真实或假的 Skeleton
// （reportStatus/reportProgress 才是那一半，见下）。与 binlogin.setPipelineState
// 把两件事拧在一起不同，这里拆开是因为 binlogin 的字段来源单一（都在
// Module 自身字段上），而本包想同时单测"内容对不对"与"调用点触发对不对"两件
// 独立的事，拆开的成本很低。

// buildPipelineStatus creates a reusable status value. reportStatus supplies the
// production configuration version from its immutable module options.
func buildPipelineStatus(pipelineID string, runID uint64, state iproto.PipelineState, lastError string, nowMs int64) *iproto.PipelineStatus {
	return &iproto.PipelineStatus{
		PipelineId:    pipelineID,
		Role:          iproto.PipelineRole_PIPELINE_ROLE_TARGET,
		State:         state,
		ConfigVersion: 0,
		RunId:         runID,
		LastError:     safeerror.PublicText(lastError),
		TsMs:          nowMs,
	}
}

// buildPipelineProgress emits a partial applied checkpoint. Real cumulative
// metrics are produced independently by ctl_telemetry.go; reporter preserves
// their observation time when merging these checkpoint/catchup values.
func buildPipelineProgress(pipelineID string, appliedGTIDSet string, nowMs int64) *iproto.PipelineProgress {
	return &iproto.PipelineProgress{
		PipelineId:     pipelineID,
		Role:           iproto.PipelineRole_PIPELINE_ROLE_TARGET,
		TsMs:           nowMs,
		AppliedGtidSet: appliedGTIDSet,
	}
}

// reportStatus 在状态发生变化时调用（OnInit 成功一次 RUNNING、freeze 一次
// ERROR），与 binlogin.setPipelineState 是同一个模式：只在变化点 Cast，不周期
// 重复发送——reporter 自己按 pipeline_id 只保留最新一份、每个发布周期都会重新
// 发布（internal/node/reporter/messages.go StatusReport 的文档），"周期性上报完整状态"这半句
// 由 reporter 负责，调用方不需要自己再维护一个定时器。
func (m *Module) reportStatus(state iproto.PipelineState, lastError string) {
	status := buildPipelineStatus(m.opts.PipelineID, m.opts.RunID, state, lastError, m.now().UnixMilli())
	status.ConfigVersion = m.runtimeVersion.Load()
	status.RuntimeInstanceId = m.opts.InstanceID
	if m.opts.InstanceID != "" {
		m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: state})
	}
	if m.opts.SnapshotReady != nil {
		switch state {
		case iproto.PipelineState_PIPELINE_STATE_SNAPSHOTTING:
			status.SnapshotState = iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_READING
		case iproto.PipelineState_PIPELINE_STATE_CATCHING_UP:
			status.SnapshotState = iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_CONVERGING
		case iproto.PipelineState_PIPELINE_STATE_RUNNING:
			status.SnapshotState = iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_CONVERGED
		case iproto.PipelineState_PIPELINE_STATE_ERROR:
			status.SnapshotState = iproto.SnapshotRunState_SNAPSHOT_RUN_STATE_FAILED
		}
	}
	m.Cast(reporter.ModuleName, reporter.StatusReport{
		Status: status,
	})
}

// Progress follows checkpoint throttling during consumption. Catchup startup,
// completion and partial failure also report immediately; no extra timer owns
// the handler's mutable catchup state.
func (m *Module) reportProgress() {
	m.Cast(reporter.ModuleName, reporter.ProgressReport{
		Progress: m.pipelineProgress(),
	})
}

// EventId 用 uuid.New().String()（uuid），与 binlogin 现有先例
// 一致，不用 pkg/uuidx——uuidx 的 UUIDv7 时间有序性是为 InnoDB 聚簇索引主键
// 准备的（该包自己的文档），但 EventId 端到端从不落任何数据库列（已核实：
// internal/manager/model/event.go 的 Event 模型没有这个字段，events 表主键是
// manager 自己生成的 id BINARY(16)，与 EventId 无关），那个性质在这里没有
// 作用对象。
//
// 分工：detail（表、主键、冲突类型、处理结果、前后镜像）由 mysqlout 构造——它知道
// 受影响行数与策略；信封（EventId/Kind/Level/TsMs/PipelineId）由本包拼——它知道自己
// 是哪条 pipeline、有 now 可注入。mysqlout 不认识 Event，本包不认识受影响行数。
//
// Level 取 "WARN" 而不是 "ERROR"：源覆盖下冲突是"记录后继续"，变更已经落地、
// pipeline 照常前进（Resolution=APPLIED）；用 ERROR 会让它和 amqp_in 冻结那类
// "已经停住了"的事件混在同一档里，按级别筛告警时分不开。
//
// EventId 用 uuid.New().String()，与 buildFreezeEvent 同一先例（理由见该函数文档那段
// 关于 uuidx/去重键的说明，此处不复述）。
func buildConflictEvent(pipelineID string, detail *iproto.ConflictEventDetail, nowMs int64) *iproto.Event {
	return &iproto.Event{
		EventId:    uuid.New().String(),
		Kind:       iproto.EventKind_EVENT_KIND_CONFLICT,
		PipelineId: pipelineID,
		TsMs:       nowMs,
		Level:      "WARN",
		Message:    "conflict policy decision recorded",
		Detail:     &iproto.Event_Conflict{Conflict: detail},
	}
}

// ReportConflict 是 mysqlout 的冲突回调落点（由 internal/node/controller/loader.go 用
// Writer.SetConflictSink 接上）。
//
// Called on the handler goroutine after COMMIT or intentional policy ROLLBACK.
// Cast transfers a value report without blocking the ordered consumer.
//
// detail 为 nil 时不发：nil 不是"一条内容为空的冲突"，发出去会让下游拿到一条
// Kind=CONFLICT 而 Detail 为空的事件，比不发更难排查。
func (m *Module) ReportConflict(detail *iproto.ConflictEventDetail) {
	if detail == nil {
		return
	}
	m.stats.conflicts.Add(1)
	m.Cast(reporter.ModuleName, reporter.EventReport{
		Event: buildConflictEvent(m.opts.PipelineID, detail, m.now().UnixMilli()),
	})
}

func buildFreezeEvent(pipelineID, code, message string, nowMs int64) *iproto.Event {
	publicMessage := safeerror.PublicText(message)
	return &iproto.Event{
		EventId:    uuid.New().String(),
		Kind:       iproto.EventKind_EVENT_KIND_ERROR,
		PipelineId: pipelineID,
		TsMs:       nowMs,
		Level:      "ERROR",
		Message:    publicMessage,
		Detail: &iproto.Event_Error{
			Error: &iproto.ErrorEventDetail{
				Code:      code,
				Detail:    publicMessage,
				Retryable: false,
			},
		},
	}
}

// reportEvent 上报冻结事件；调用方在成功取得冻结状态所有权后调用一次。
// code 使用稳定的错误码，message 在编码前脱敏。
func (m *Module) reportEvent(code, message string) {
	m.Cast(reporter.ModuleName, reporter.EventReport{
		Event: buildFreezeEvent(m.opts.PipelineID, code, message, m.now().UnixMilli()),
	})
}
