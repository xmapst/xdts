package reporter

import (
	"log/slog"
	"uuid"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/node/store"
)

// Reporter converts store recovery observations into Node events. Store supplies
// values through its existing callback and does not depend on Reporter.
func (m *Module) reportRebuildEvents() {
	if m.opts.RebuildEventsFunc == nil {
		return
	}
	for _, rec := range m.opts.RebuildEventsFunc() {
		event := buildRebuildEvent(rec, m.now().UnixMilli())
		if err := (EventReport{Event: event}).validate(); err != nil {
			// 理论不会发生：EventId 由 uuid.New().String() 保证非空，Kind 是本函数写死的
			// EVENT_KIND_LOCAL_STORE_REBUILD。保留检查是为了不让这条构造路径绕开
			// onEvent 对外部调用方要求的同一条防线，不因为"这里是本包自己拼的"就默认
			// 它总是合法。
			slog.Error("built local store rebuild event failed self validation, discarding",
				slog.String("file", rec.File), slog.Any("error", err))
			continue
		}
		if _, err := eventMessageType(event.Kind); err != nil {
			slog.Error("local store rebuild event kind not registered for publishing, discarding",
				slog.String("file", rec.File), slog.Any("error", err))
			continue
		}
		m.enqueueEvent(event)
	}
}

// buildRebuildEvent 把一条 store.RebuildRecord 转成协议 Event。
func buildRebuildEvent(rec store.RebuildRecord, nowMs int64) *iproto.Event {
	return &iproto.Event{
		EventId: uuid.New().String(),
		Kind:    iproto.EventKind_EVENT_KIND_LOCAL_STORE_REBUILD,
		TsMs:    nowMs,
		Level:   rec.Level,
		Message: rebuildEventMessage(rec.Level),
		Detail: &iproto.Event_LocalStoreRebuild{
			LocalStoreRebuild: &iproto.LocalStoreRebuildEventDetail{
				Reason:     safeerror.PublicText(rec.Reason),
				BackupPath: rec.BackupPath,
			},
		},
	}
}

// rebuildEventMessage 按 Level 给稳定描述。三个取值与 internal/node/store/selfcheck.go
// recordRebuild 全部调用点实际传入的 Level 一一对应：
//   - "INFO"：重建成功，未发现数据丢失（rebuildFile 的 else 分支）；
//   - "WARN"：重建成功，但存在部分数据丢失（rebuildFile 的 Skipped>0 分支）；
//   - "ERROR"：重建失败，已降级为空状态启动，或连移入 corrupt/ 都失败
//     （fallbackToEmpty 的两个分支）。
//
// 未识别的 Level 兜底成一句不编造严重度的通用描述，不 panic、不整条丢弃事件——字段
// 值来自本包内部（store.RebuildRecord.Level 只由 recordRebuild 写入），出现未知取值
// 意味着两边约定漂移了，但事件本身携带的 Detail.Reason 仍然是有意义的信息，没有
// 理由因为 Message 选词失配就把整条事件吞掉。
func rebuildEventMessage(level string) string {
	switch level {
	case "INFO":
		return "本地状态库自动重建成功"
	case "WARN":
		return "本地状态库自动重建成功，但存在部分数据丢失"
	case "ERROR":
		return "本地状态库自动重建失败，已降级为空状态启动"
	default:
		return "本地状态库发生自动重建"
	}
}
