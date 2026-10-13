package ingest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// onEvent 处理 event.* 消息（需求 MGR-MON-03）。report
// 队列上 event.error/ddl/conflict/unknown_origin/backpressure/
// local_store_rebuild/link_probe_failed 这 7 个 Properties.Type 都路由到
// 这里（见 internal/manager/ingest/handler.go dispatchReport）——它们共用同一个 xdts.v1.Event
// 信封（oneof detail），具体种类由消息体的 kind 字段而不是 Properties.Type
// 决定：两者理论上应该一致，但 kind 在 HMAC 签名覆盖的 body 内（internal/iproto/sign.go
// 的 SignParams.Body），Properties.Type 不在签名覆盖范围内，因此以 kind
// 为准更可信，这里不用 Properties.Type 做二次校验。
//
// 一条 AMQP 消息只携带一个 Event（不是批量），因此这里调用 InsertEvents
// 时永远只传一行——不存在"多行分块、部分成功"的问题，process()
// （internal/manager/ingest/handler.go）"失败也 Ack、靠日志+计数器兜底"的策略在这里退化成最简单
// 的情形：这一行要么整体成功要么整体失败，不会出现"这条 AMQP 消息里一半
// 落库一半没落库"的中间状态。
func (s *Service) onEvent(ctx context.Context, env *iproto.Envelope, headerNodeID uuidx.UUID) error {
	var m iproto.Event
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &m); err != nil {
		return fmt.Errorf("ingest: 解码 Event 失败: %w", err)
	}
	if err := checkBodyNodeID(m.GetNodeId(), headerNodeID); err != nil {
		return err
	}
	eventType, err := eventTypeFromKind(m.GetKind())
	if err != nil {
		return err
	}
	level, err := parseEventLevel(m.GetLevel())
	if err != nil {
		return err
	}

	var pipelineID uuidx.UUID
	if pid := m.GetPipelineId(); pid != "" {
		pipelineID, err = uuidx.Parse(pid)
		if err != nil {
			return fmt.Errorf("ingest: Event.pipeline_id 不是合法 UUID: %w", err)
		}
	}

	if migration := m.GetDdl().GetMigration(); migration != nil {
		if pipelineID.IsZero() || m.GetKind() != iproto.EventKind_EVENT_KIND_DDL {
			return fmt.Errorf("ingest: invalid migration result event")
		}
		if err := repo.RecordSchemaMigrationResult(ctx, s.opts.DB, pipelineID, headerNodeID, migration.GetRunId(), migration.GetSchemaUuid(), migration.GetSeq(), migration.GetSuccess(), redact.PotentialCredentials(migration.GetError())); err != nil {
			return err
		}
	}
	payload, err := eventPayload(&m)
	if err != nil {
		return err
	}

	row := model.Event{
		Ts:         time.UnixMilli(m.GetTsMs()),
		NodeID:     headerNodeID,
		PipelineID: pipelineID,
		Type:       eventType,
		Level:      level,
		Payload:    payload,
	}
	if m.GetKind() == iproto.EventKind_EVENT_KIND_CONFLICT {
		conflict, err := conflictRecord(&m)
		if err != nil {
			return err
		}
		row.ID = conflict.ID
		conflict.PipelineID, conflict.Ts = pipelineID, row.Ts
		return repo.RecordConflictEvent(ctx, s.opts.DB, row, conflict)
	}
	if err := s.opts.InsertEvents(ctx, s.opts.DB, []model.Event{row}); err != nil {
		return fmt.Errorf("ingest: 写入事件失败（event_id=%s）: %w", m.GetEventId(), err)
	}
	return nil
}

// eventTypeFromKind 把 proto/report.proto 的 EventKind 映射为 model.EventType。
func eventTypeFromKind(k iproto.EventKind) (model.EventType, error) {
	switch k {
	case iproto.EventKind_EVENT_KIND_ERROR:
		return model.EventTypeError, nil
	case iproto.EventKind_EVENT_KIND_DDL:
		return model.EventTypeDDL, nil
	case iproto.EventKind_EVENT_KIND_CONFLICT:
		return model.EventTypeConflict, nil
	case iproto.EventKind_EVENT_KIND_UNKNOWN_ORIGIN:
		return model.EventTypeUnknownOrigin, nil
	case iproto.EventKind_EVENT_KIND_BACKPRESSURE:
		return model.EventTypeBackpressure, nil
	case iproto.EventKind_EVENT_KIND_LOCAL_STORE_REBUILD:
		return model.EventTypeLocalStoreRebuild, nil
	case iproto.EventKind_EVENT_KIND_LINK_PROBE_FAILED:
		return model.EventTypeLinkProbeFailed, nil
	case iproto.EventKind_EVENT_KIND_NODE_OFFLINE:
		// proto/report.proto 注释：node_offline 由 Manager 自行判定产生，不经由
		// 节点上报——正常路径不会走到这里（本包自己产生该事件，见
		// internal/manager/ingest/ctl_offline.go），但协议允许的取值不因此变成非法取值，仍按已
		// 定义词表放行，不额外拒绝。
		return model.EventTypeNodeOffline, nil
	default:
		// 含 EVENT_KIND_UNSPECIFIED：明确拒绝未知枚举，不静默写入权威状态
		return "", fmt.Errorf("ingest: 未知的 EventKind 取值 %d", k)
	}
}

// parseEventLevel 把 Event.level（协议注释："INFO/WARN/ERROR，与日志级别
// 体系一致"）转换为 model.EventLevel。
//
// 注意两套词表在"警告"这一档拼写不同——协议用 WARN
// （proto/report.proto），model.EventLevel 用 warning
// （internal/manager/model/event.go）。这不是本包引入的不一致，是两处
// 已有约定各自独立产生的；这里必须显式做别名映射，不能简单
// strings.ToLower 之后直接转换成 model.EventLevel 类型——那样
// "warn" != "warning"，会导致所有 WARN 级别事件都被判定为未知级别而拒绝
// 落库（对比 parseLogLevel，日志级别的两套词表除大小写外完全一致，不需要
// 这层映射）。
func parseEventLevel(s string) (model.EventLevel, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "info":
		return model.EventLevelInfo, nil
	case "warn":
		return model.EventLevelWarning, nil
	case "error":
		return model.EventLevelError, nil
	default:
		return "", fmt.Errorf("ingest: 未知的事件级别 %q", s)
	}
}

// 这是编码方式的缺陷、不是某个字段的缺陷：同一条路径上
// ConflictEventDetail 的 policy/resolution 两个枚举字段零值时会同样消失。
// 故修在编码器上，不给 retryable 单独打补丁。
//
// 选项（UseProtoNames + EmitDefaultValues + json.Compact）集中在
// iproto.MarshalEventDetailJSON 一处，不由本函数各传各的——payload 的形状
// 是 ingest 落库、http 暴露、web 展示三方共享的契约，两个生产点用不同选项
// 会让同一张表里出现两套键名风格，而这种漂移在任何一端都不会报错
// （红线「契约改全」）。完整论证见该函数文档。
//
// v 的静态类型从 any 改为 proto.Message：七个 oneof getter 的返回类型都
// 满足它，default 分支在赋值前就 return，不存在把 nil 交给编码器的路径。
func eventPayload(m *iproto.Event) (model.JSON, error) {
	var v proto.Message
	switch {
	case m.GetError() != nil:
		v = m.GetError()
	case m.GetDdl() != nil:
		v = m.GetDdl()
	case m.GetConflict() != nil:
		v = m.GetConflict()
	case m.GetUnknownOrigin() != nil:
		v = m.GetUnknownOrigin()
	case m.GetBackpressure() != nil:
		v = m.GetBackpressure()
	case m.GetLocalStoreRebuild() != nil:
		v = m.GetLocalStoreRebuild()
	case m.GetLinkProbeFailed() != nil:
		v = m.GetLinkProbeFailed()
	default:
		return nil, nil
	}
	raw, err := iproto.MarshalEventDetailJSON(v)
	if err != nil {
		return nil, fmt.Errorf("ingest: 序列化事件详情失败: %w", err)
	}
	if raw == nil {
		return nil, nil
	}
	// 替换文本（*:*@）不含引号与反斜杠，不会破坏 JSON 结构；脱敏只在
	// 这里做，不下探到 iproto.MarshalEventDetailJSON——那是编码器，不是
	// 策略点，且为 Manager/Node 两侧共享。
	return model.JSON(redact.PotentialCredentials(string(raw))), nil
}
