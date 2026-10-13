package reporter

import (
	"log/slog"
	"math"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xhive/chanrpc"
)

// onStatus/onProgress/onEvent 是 StatusReport/ProgressReport/EventReport 的 ChanRPC
// 处理器。三者都是单向 Cast（「无」回包列），处理器本身不调用 ci.Hold()、
// 直接返回 nil 即可——Cast 没有响应通道，返回值被框架丢弃（internal/node/store/client.go 对这一点的说明
// 同样适用：本模块也不需要为 Cast 场景操心回包）。
//
// 校验失败的消息直接丢弃并记日志，不 panic、不静默吞掉（边界输入必须
// 校验）——调用方（未来的动态模块）传入非法数据是编程错误，日志是唯一能让这类错误
// 在联调阶段暴露出来的地方。

func (m *Module) onStatus(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(StatusReport)
	if !ok {
		slog.Error("received unexpected message type", slog.String("want", "reporter.StatusReport"))
		return nil
	}
	if err := req.validate(); err != nil {
		slog.Error("statusreport validation failed, discarding", slog.Any("error", err))
		return nil
	}
	key := statusKeyOf(req.Status.GetPipelineId(), req.Status.GetRole())
	if previous := m.statusByPipeline[key]; previous != nil && req.Status.GetRunId() < previous.GetRunId() {
		return nil
	}
	if previous := m.statusByPipeline[key]; previous != nil && req.Status.GetRunId() == previous.GetRunId() && req.Status.GetConfigVersion() < previous.GetConfigVersion() {
		return nil
	}
	if previous := m.statusByPipeline[key]; previous != nil && req.Status.GetRunId() == previous.GetRunId() &&
		req.Status.GetTsMs() > 0 && previous.GetTsMs() > 0 && req.Status.GetTsMs() < previous.GetTsMs() {
		return nil
	}
	if instance := req.Status.GetRuntimeInstanceId(); instance != "" {
		floor := m.statusInstanceFloors[key]
		if floor.runID == req.Status.GetRunId() && instance < floor.instanceID {
			return nil
		}
		if previous := m.statusByPipeline[key]; previous != nil && previous.GetRunId() == req.Status.GetRunId() &&
			previous.GetRuntimeInstanceId() == "" && instance == floor.instanceID &&
			(previous.GetState() == iproto.PipelineState_PIPELINE_STATE_STOPPED || previous.GetState() == iproto.PipelineState_PIPELINE_STATE_PAUSED) {
			return nil
		}
		if m.statusInstanceFloors == nil {
			m.statusInstanceFloors = make(map[statusKey]statusInstanceFloor)
		}
		m.statusInstanceFloors[key] = statusInstanceFloor{runID: req.Status.GetRunId(), instanceID: instance}
	}
	if previous := m.progressByPipeline[key]; previous != nil && previous.GetRunId() < req.Status.GetRunId() {
		delete(m.progressByPipeline, key)
		delete(m.telemetryByPipeline, key)
	}
	status := proto.Clone(req.Status).(*iproto.PipelineStatus)
	// The reporter owns publication ordering. Producers transfer an independent
	// value; periodic retries never allocate another sequence for that value.
	if m.statusSessionID != "" {
		if m.statusSequences[key] == math.MaxUint64 {
			slog.Error("status sequence exhausted", slog.String("pipeline_id", status.GetPipelineId()))
			return nil
		}
		m.statusSequences[key]++
		status.StatusSessionId = m.statusSessionID
		status.StatusSessionStartedAtMs = m.statusSessionStartedAtMS
		status.StatusSeq = m.statusSequences[key]
	}
	m.statusByPipeline[key] = status
	slog.Debug("pipeline status received",
		slog.String("pipeline_id", req.Status.GetPipelineId()),
		slog.Any("role", req.Status.GetRole()),
		slog.Any("state", req.Status.GetState()),
		slog.Uint64("config_version", req.Status.GetConfigVersion()),
		slog.Uint64("run_id", req.Status.GetRunId()))
	return nil
}

func (m *Module) onProgress(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(ProgressReport)
	if !ok {
		slog.Error("received unexpected message type", slog.String("want", "reporter.ProgressReport"))
		return nil
	}
	if err := req.validate(); err != nil {
		slog.Error("progressreport validation failed, discarding", slog.Any("error", err))
		return nil
	}
	key := statusKeyOf(req.Progress.GetPipelineId(), req.Progress.GetRole())
	progress := proto.Clone(req.Progress).(*iproto.PipelineProgress)
	if status := m.statusByPipeline[key]; status != nil && progress.GetRunId() < status.GetRunId() {
		return nil
	}
	if previous := m.progressByPipeline[key]; previous != nil {
		if progress.GetRunId() < previous.GetRunId() {
			return nil
		}
		if progress.GetRunId() != previous.GetRunId() {
			// A new run owns its own positions and snapshot totals. Only
			// reports from the same run can be merged as partial updates.
			m.progressByPipeline[key] = progress
			return nil
		}
		// Target checkpoint/catchup updates contain their applied position and
		// snapshot only. Preserve the sampled metrics and their observation time
		// between reporting windows; a partial update cannot attest new rates.
		if progress.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_TARGET && progress.MetricsObservedTsMs == nil && previous.MetricsObservedTsMs != nil {
			merged := proto.Clone(previous).(*iproto.PipelineProgress)
			merged.AppliedGtidSet = progress.AppliedGtidSet
			merged.TsMs = progress.TsMs
			if progress.Snapshot != nil {
				merged.Snapshot = progress.Snapshot
			}
			progress = merged
		}
		if progress.GetSnapshot() != nil {
			if progress.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
				// Source snapshot reports are partial, separate from binlog
				// positions. Target catchup reports include their current position.
				merged := proto.Clone(previous).(*iproto.PipelineProgress)
				merged.Snapshot = progress.Snapshot
				merged.RunId = progress.RunId
				merged.TsMs = progress.TsMs
				progress = merged
			}
		} else if previous.GetSnapshot() != nil {
			progress.Snapshot = proto.Clone(previous.Snapshot).(*iproto.SnapshotProgress)
			progress.RunId = previous.RunId
		}
	}
	m.progressByPipeline[key] = progress
	return nil
}

func (m *Module) onEvent(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(EventReport)
	if !ok {
		slog.Error("received unexpected message type", slog.String("want", "reporter.EventReport"))
		return nil
	}
	if err := req.validate(); err != nil {
		slog.Error("eventreport validation failed, discarding", slog.Any("error", err))
		return nil
	}
	if _, err := eventMessageType(req.Event.Kind); err != nil {
		slog.Error("event kind not yet supported for publishing, discarding",
			slog.String("event_id", req.Event.EventId),
			slog.String("kind", req.Event.Kind.String()),
			slog.Any("error", err))
		return nil
	}
	m.enqueueEvent(req.Event)
	return nil
}

func (m *Module) onSnapshotRate(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	message, ok := ci.Request.(SnapshotRateReport)
	if !ok || message.PipelineID == "" || message.RowsPerSec < 0 || message.BytesPerSec < 0 || math.IsNaN(message.RowsPerSec) || math.IsNaN(message.BytesPerSec) || math.IsInf(message.RowsPerSec, 0) || math.IsInf(message.BytesPerSec, 0) {
		return nil
	}
	key := statusKeyOf(message.PipelineID, iproto.PipelineRole_PIPELINE_ROLE_SOURCE)
	current := m.progressByPipeline[key]
	if current == nil || current.GetSnapshot() == nil || current.GetRunId() != message.RunID {
		return nil
	}
	copy := proto.Clone(current).(*iproto.PipelineProgress)
	copy.RowsPerSec = message.RowsPerSec
	copy.BytesPerSec = message.BytesPerSec
	m.progressByPipeline[key] = copy
	return nil
}

// 整份替换，不做合并：这是一份全量快照，"某条 pipeline 已经不在了"只能由"它不在
// 这份快照里"表达。按 pipeline_id 合并进一个 map 会让卸载掉的条目永远留在心跳里——
// 与 StatusReport 的保留语义不同，那边每条 status 是独立的事实，这里整个切片才是一条。
//
// 校验失败时保留上一份镜像并保留 received 标记：一条畸形推送不是"节点突然不知道
// 自己承载什么了"，把 received 退回 false 会让心跳从此报"不知道"，比继续报上一份
// （最多是旧的，下一拍就会被全量重推覆盖）更糟。
func (m *Module) onPipelinesMirror(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(PipelinesMirror)
	if !ok {
		slog.Error("received unexpected message type", slog.String("want", "reporter.PipelinesMirror"))
		return nil
	}
	if err := req.validate(); err != nil {
		slog.Error("pipelines mirror validation failed, keeping previous mirror",
			slog.Any("error", err), slog.Bool("had_previous", m.pipelinesMirrorReceived))
		return nil
	}
	present := make(map[statusKey]bool, len(req.Entries))
	for _, entry := range req.Entries {
		present[statusKeyOf(entry.GetPipelineId(), entry.GetRole())] = true
	}
	for _, previous := range m.pipelinesMirror {
		key := statusKeyOf(previous.GetPipelineId(), previous.GetRole())
		if !present[key] {
			// A full ownership mirror retires periodic reports for the unloaded
			// role. Keep lifecycle floors to reject late reports from its instance.
			delete(m.statusByPipeline, key)
			delete(m.progressByPipeline, key)
		}
	}
	m.pipelinesMirror = req.Entries
	m.pipelinesMirrorReceived = true
	for _, config := range req.ConfirmedConfigs {
		key := statusKeyOf(config.PipelineID, config.Role)
		previous := m.statusByPipeline[key]
		if previous == nil || previous.GetRunId() != config.RunID || previous.GetRuntimeInstanceId() != config.InstanceID || previous.GetConfigVersion() >= config.ConfigVersion {
			continue
		}
		// Clone before advancing: an async publication may still own the
		// previous value. Lifecycle and snapshot observations stay intact.
		updated := proto.Clone(previous).(*iproto.PipelineStatus)
		updated.ConfigVersion = config.ConfigVersion
		m.onStatus(&chanrpc.CallInfo{Request: StatusReport{Status: updated}})
	}
	return nil
}
