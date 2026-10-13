package reporter

import (
	"errors"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
)

// ModuleName 是 reporter 模块在 xhive 里注册的名字（静态模块，Priority 20）。
const ModuleName = "reporter"

// 发送方投递后不得修改消息，Reporter 直接接管其数据所有权。

type statusKey struct {
	pipelineID string
	role       iproto.PipelineRole
}

type statusInstanceFloor struct {
	runID      uint64
	instanceID string
}

func statusKeyOf(pipelineID string, role iproto.PipelineRole) statusKey {
	return statusKey{pipelineID: pipelineID, role: role}
}

// StatusReport 对应 pipeline.status（NODE-RPT-02：状态变化即时上报，并周期性上报
// 完整状态）。reporter 按 (pipeline_id, role) 只保留最新一份，每个发布周期都会
// 重新发布（天然满足「周期性上报完整状态」，不需要额外的「是否变化」判断）。
type StatusReport struct {
	Status *iproto.PipelineStatus
}

func (r StatusReport) validate() error {
	if r.Status == nil {
		return errors.New("reporter: StatusReport.Status 不能为空")
	}
	if r.Status.PipelineId == "" {
		return errors.New("reporter: StatusReport.Status.PipelineId 不能为空")
	}
	return nil
}

type ProgressReport struct {
	Progress *iproto.PipelineProgress
}

type SnapshotRateReport struct {
	PipelineID  string
	RunID       uint64
	RowsPerSec  float64
	BytesPerSec float64
}

func (r ProgressReport) validate() error {
	if r.Progress == nil {
		return errors.New("reporter: ProgressReport.Progress 不能为空")
	}
	if r.Progress.PipelineId == "" {
		return errors.New("reporter: ProgressReport.Progress.PipelineId 不能为空")
	}
	return nil
}

// 那份状态（controller 的 m.applied）由 controller 的事件循环独占，
// 跨 goroutine 读就是数据竞争。与 StatusReport 走同一条通路、同一条纪律。
//
// Cast 在对端未就绪时走 chanrpc 的 ErrServerNil 分支，有意不打 warn（避免启动期
// 误报），队列满时也只是丢弃 + 一条 warn。节点侧 controller 的 Priority(10) 小于
// reporter 的 Priority(20)，xhive 按升序装配，所以 controller 先就绪——它在那个窗口里
// 推的镜像会被静默丢弃。只在变化时推的话，启动期丢掉那一次之后如果再没有配置
// 变化，reporter 会永久停留在"没收到过"。全量重推让一次丢弃在下一拍自愈。
//
// 每次推送由 controller 新建整个切片与其中每个 entry（pushPipelinesMirror），投递后
// 不再修改（"投递后不再修改"）。reporter 直接把它交给心跳编码，不做防御性拷贝；
// 也不得原地修改它——上一份切片可能正被某个进行中的心跳编码读取。
type PipelinesMirror struct {
	Entries []*iproto.PipelineHeartbeatEntry
	// ConfirmedConfigs bind an applied configuration to the same physical
	// runtime. They refresh cached versions without changing lifecycle facts.
	ConfirmedConfigs []iproto.PipelineRuntimeStatus
}

func (r PipelinesMirror) validate() error {
	for i, c := range r.ConfirmedConfigs {
		// Revision 0 is valid for the first explicitly authorized START. Runtime
		// identity proves this is an applied instance; a positive revision cannot.
		if c.PipelineID == "" || c.InstanceID == "" || (c.Role != iproto.PipelineRole_PIPELINE_ROLE_SOURCE && c.Role != iproto.PipelineRole_PIPELINE_ROLE_TARGET) {
			return fmt.Errorf("reporter: invalid confirmed configuration at %d", i)
		}
	}
	for i, e := range r.Entries {
		if e == nil {
			return fmt.Errorf("reporter: PipelinesMirror.Entries[%d] 不能为 nil", i)
		}
		if e.GetPipelineId() == "" {
			return fmt.Errorf("reporter: PipelinesMirror.Entries[%d].PipelineId 不能为空", i)
		}
		// role 是这份镜像的定位维度之一（自环拓扑下同一条 pipeline 两侧各一项），
		// UNSPECIFIED 会让消费方无法分辨"哪一侧"，而 pipelines[] 存在的理由之一
		// 正是分辨侧别。拒绝而不是补一个默认值，避免把无法定位的记录写进权威状态。
		if e.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_UNSPECIFIED {
			return fmt.Errorf("reporter: PipelinesMirror.Entries[%d].Role 不能为 UNSPECIFIED（pipeline_id=%s）",
				i, e.GetPipelineId())
		}
	}
	return nil
}

// EventReport 对应 event.error / event.ddl / event.conflict / event.unknown_origin
// （冲突事件复用同一个 Event 信封，用 Kind=EVENT_KIND_CONFLICT 区分，
// 不是单独的消息类型）。reporter 全部保留，受 EventTTL 与 EventMaxCount 约束
// （NODE-RPT-07）。
type EventReport struct {
	Event *iproto.Event
}

func (r EventReport) validate() error {
	if r.Event == nil {
		return errors.New("reporter: EventReport.Event 不能为空")
	}
	if r.Event.EventId == "" {
		return errors.New("reporter: EventReport.Event.EventId 不能为空（用作去重幂等键与 outbox 主键）")
	}
	if r.Event.Kind == iproto.EventKind_EVENT_KIND_UNSPECIFIED {
		return errors.New("reporter: EventReport.Event.Kind 不能为 EVENT_KIND_UNSPECIFIED")
	}
	return nil
}

var errEventKindManagerOnly = errors.New(
	"reporter: node_offline 由 Manager 依据心跳超时自行判定产生，节点侧不上报该事件（report.proto EventKind 文档）")

// eventMessageType maps Node event kinds to AMQP message types.
// Manager-only kinds are rejected before publication.
func eventMessageType(kind iproto.EventKind) (string, error) {
	switch kind {
	case iproto.EventKind_EVENT_KIND_ERROR:
		return iproto.MessageTypeEventError, nil
	case iproto.EventKind_EVENT_KIND_DDL:
		return iproto.MessageTypeEventDDL, nil
	case iproto.EventKind_EVENT_KIND_CONFLICT:
		return iproto.MessageTypeEventConflict, nil
	case iproto.EventKind_EVENT_KIND_UNKNOWN_ORIGIN:
		return iproto.MessageTypeEventUnknownOrigin, nil
	case iproto.EventKind_EVENT_KIND_BACKPRESSURE:
		return iproto.MessageTypeEventBackpressure, nil
	case iproto.EventKind_EVENT_KIND_LOCAL_STORE_REBUILD:
		return iproto.MessageTypeEventLocalStoreRebuild, nil
	case iproto.EventKind_EVENT_KIND_LINK_PROBE_FAILED:
		return iproto.MessageTypeEventLinkProbeFailed, nil
	case iproto.EventKind_EVENT_KIND_NODE_OFFLINE:
		// 显式拒绝，不是漏写：协议规定本端不产生这个取值。见本函数文档那一节。
		return "", errEventKindManagerOnly
	default:
		return "", errors.New("reporter: message_types.go 未登记该 EventKind 对应的 type 字符串")
	}
}
