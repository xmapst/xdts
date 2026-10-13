package reporter

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// flushOutcome 是一次 flush 周期在后台 goroutine（LinearContext 的 f）里执行的结果，
// 通过同一个指针传给 cb，cb 回到事件循环后据此更新 m.pendingEvents（见 internal/node/reporter/reporter.go
// 顶部关于该模式与 store 模块 commitBatch/ackBatch 一致的说明）。f 完成之后 cb 才会
// 运行是 LinearContext 的契约，因此这里不需要额外的锁或原子操作就能安全地跨 goroutine
// 传递这个结构。
type flushOutcome struct {
	// events 是本次 flush 快照时的事件顺序（与 runFlush 的入参同一个切片），cb 需要
	// 它来计算"这次快照里有哪些没发出去"，不能只靠一个计数去对当前 m.pendingEvents
	// 做下标切片——本次异步发布进行期间，新的 Cast 可能已经把 m.pendingEvents 变长，
	// 单纯的 m.pendingEvents[n:] 会用错误的下标切到别的事件头上，容量不足时甚至直接
	// panic（out of range）。
	events []*pendingEvent
	// publishedEvents 是 events 里从队首开始连续发布成功的条数。一旦遇到失败就停止
	// （不继续尝试后面的条目），确保 outbox 严格按时间顺序补发——不会出现"后面的事件
	// 先发出去、前面的还在等"的乱序。
	publishedEvents int
}

// flush publishes current observations and pending events with bounded deadlines.
func (m *Module) flush() {
	if len(m.pendingEvents) == 0 && len(m.statusByPipeline) == 0 && len(m.progressByPipeline) == 0 && len(m.pipelinesMirror) == 0 {
		return
	}

	events := m.pendingEvents
	m.pendingEvents = nil

	statuses := make([]*iproto.PipelineStatus, 0, len(m.statusByPipeline))
	for _, st := range m.statusByPipeline {
		statuses = append(statuses, st)
	}
	var progresses []*iproto.PipelineProgress
	now := m.now()
	if m.lastProgressFlush.IsZero() || now.Sub(m.lastProgressFlush) >= m.opts.ProgressInterval {
		progresses = m.progressSnapshot(now.UnixMilli())
		m.lastProgressFlush = now
	}

	out := &flushOutcome{events: events}
	m.linear.Go(
		func() { m.runFlush(events, statuses, progresses, out) },
		func() { m.applyFlushOutcome(out) },
	)
}

// runFlush 在 LinearContext 的后台 goroutine 里执行，串行发布事件、状态、进度。
// 不直接读写 Module 的事件循环私有字段（pendingEvents/statusByPipeline/...），只操作
// 传入的快照与 out——与 store 模块 commitBatch 的纪律完全一致。
func (m *Module) runFlush(events []*pendingEvent, statuses []*iproto.PipelineStatus, progresses []*iproto.PipelineProgress, out *flushOutcome) {
	ctx := context.Background()

	for i, pe := range events {
		msgType, err := eventMessageType(pe.event.Kind)
		if err != nil {
			// 理论不可达：onEvent 已经用同一个函数拒绝了未登记的 Kind。防御性处理，
			// 不让一条脏数据卡住整条 outbox 队列——跳过它（计入失败），继续尝试后面的。
			slog.Error("event type not registered at publish stage",
				slog.String("event_id", pe.event.EventId), slog.Any("error", err))
			m.stats.failed.Add(1)
			continue
		}
		headers := map[string]any{}
		if pe.event.PipelineId != "" {
			headers[iproto.HeaderPipelineID] = pe.event.PipelineId
		}
		routingKey := transport.RoutingKeyEvent(m.opts.Center, m.opts.NodeID)
		if pubErr := m.publishMessage(ctx, msgType, routingKey, pe.event, headers); pubErr != nil {
			slog.Warn("publish event failed, writing to outbox for retry next cycle",
				slog.String("event_id", pe.event.EventId), slog.Any("error", pubErr))
			m.stats.failed.Add(1)
			m.writeEventsToOutbox(events[i:])
			return
		}
		m.stats.published.Add(1)
		out.publishedEvents++
	}

	// 心跳不写 outbox：过期心跳没有重发价值，发布失败后等待下一个心跳周期。
	for _, st := range statuses {
		headers := map[string]any{iproto.HeaderPipelineID: st.PipelineId}
		routingKey := transport.RoutingKeyStatus(m.opts.Center, m.opts.NodeID)
		if pubErr := m.publishMessage(ctx, iproto.MessageTypePipelineStatus, routingKey, st, headers); pubErr != nil {
			slog.Warn("publish status failed, writing to outbox for retry next cycle",
				slog.String("pipeline_id", st.PipelineId), slog.Any("error", pubErr))
			m.stats.failed.Add(1)
			m.writeLatestToOutbox("status", st.PipelineId, st)
			continue
		}
		slog.Debug("pipeline status published",
			slog.String("pipeline_id", st.GetPipelineId()),
			slog.Any("role", st.GetRole()),
			slog.Any("state", st.GetState()),
			slog.Uint64("config_version", st.GetConfigVersion()),
			slog.Uint64("run_id", st.GetRunId()))
		m.stats.published.Add(1)
	}
	for _, pg := range progresses {
		if !m.inspectProgressQueue(ctx, pg) {
			continue
		}
		headers := map[string]any{iproto.HeaderPipelineID: pg.PipelineId}
		routingKey := transport.RoutingKeyProgress(m.opts.Center, m.opts.NodeID)
		if pubErr := m.publishMessage(ctx, iproto.MessageTypePipelineProgress, routingKey, pg, headers); pubErr != nil {
			slog.Warn("publish progress failed, writing to outbox for retry next cycle",
				slog.String("pipeline_id", pg.PipelineId), slog.Any("error", pubErr))
			m.stats.failed.Add(1)
			m.writeLatestToOutbox("progress", pg.PipelineId, pg)
			continue
		}
		m.stats.published.Add(1)
	}
}

// applyFlushOutcome 回到事件循环执行，把本次快照里没能发出去的事件（若有）合并回
// 当前的 m.pendingEvents 前面——用合并而不是按下标切片，是因为 flush() 提交异步任务
// 期间，事件循环仍在正常处理新到达的 Cast，m.pendingEvents 可能已经变化（见 flush 的
// 注释）；只对"这次快照自己的 events"做归并，天然对并发到达的新事件安全。
func (m *Module) applyFlushOutcome(out *flushOutcome) {
	if out.publishedEvents >= len(out.events) {
		return // 这次快照里的事件全部发布成功，没有需要放回的。
	}
	leftover := out.events[out.publishedEvents:]
	merged := make([]*pendingEvent, 0, len(leftover)+len(m.pendingEvents))
	merged = append(merged, leftover...)
	merged = append(merged, m.pendingEvents...)
	m.pendingEvents = merged
	// 放回之后总量可能超过上限（放回的旧事件 + 期间新到达的事件），重新走一遍
	// TTL/容量治理（NODE-RPT-07）。
	m.governEvents()
}

// publishHeartbeat 是心跳定时器的处理函数（NODE-RPT-01，默认 5s）。心跳本身不进
// outbox（的 outbox 表只列了状态/进度/事件/回执，心跳不在其中）：一条过期
// 的心跳没有补发价值，下一周期用新采样的资源数据重新发一条即可。
func (m *Module) publishHeartbeat() {
	hb := &iproto.NodeHeartbeat{
		NodeId:        m.opts.NodeID,
		TsMs:          m.now().UnixMilli(),
		Resource:      m.sampleResource(),
		NodeVersion:   m.opts.NodeVersion,
		UptimeSeconds: m.uptimeSeconds(),
	}

	// 两者同一份快照、同一处产出：count 就是这一次赋进去的那个切片的长度，不是
	// 另取一次。分两处取会让校验和在正常运行时也偶发不等，于是这个信号退化成噪声
	// （proto/report.proto pipelines_known_count"生产方纪律"；与 mem_percent / mem_rss_bytes
	// / mem_total_bytes 必须出自一次采样是同一条）。
	//
	// 承载 0 条时显式写 0（received 为真、切片为空 ⇒ count=&0）：这一半同样是纪律，
	// 漏掉就又回到"空即未知"。
	if m.pipelinesMirrorReceived {
		hb.Pipelines = m.pipelinesMirror
		known := uint32(len(m.pipelinesMirror)) //nolint:gosec // 承载条数远小于 uint32 上限
		hb.PipelinesKnownCount = &known
	}

	// NODE-XDOC-08：状态库大小、提交耗时随心跳上报。NodeHeartbeat 已
	// 包含 LocalStoreStats 字段，
	// 这里正式接线，不再只打本地日志。store_size_bytes 由 store.Stats() 自己算
	// （对 identity.xdoc/state.xdoc 做 os.Stat 求和），reporter 不需要知道 xdoc 文件
	// 命名——store 才是唯一知道自己文件布局的模块（"句柄注入"同一条边界）。
	if m.opts.StatsFunc != nil {
		st := m.opts.StatsFunc()
		hb.LocalStore = &iproto.LocalStoreStats{
			StoreSizeBytes:       st.StoreSizeBytes,
			LastCommitDurationMs: st.LastCommitDuration.Milliseconds(),
		}
	}
	// StatsFunc 为空（未接入 store 的过渡态、部分单元测试）时 hb.LocalStore 保持
	// nil——LocalStoreStats 是嵌套消息而非拍平字段，正是为了让"未上报"（nil）与
	// "上报了 0"（非 nil 但字段为零值）在协议层面可区分，这里不主动构造一个假的
	// 零值消息去冒充"已上报"。

	m.linear.Go(func() {
		routingKey := transport.RoutingKeyHeartbeat(m.opts.Center, m.opts.NodeID)
		if err := m.publishMessage(context.Background(), iproto.MessageTypeNodeHeartbeat, routingKey, hb, nil); err != nil {
			m.stats.failed.Add(1)
			slog.Warn("publish heartbeat failed, will resample and publish next cycle", slog.Any("error", err))
			return
		}
		m.stats.published.Add(1)
	}, nil)
}

// publishMessage 编码并签名上报信封，通过注入的 Publisher 发布。
// 发布确认和 return 关联由 transport 管理。
func (m *Module) publishMessage(ctx context.Context, msgType, routingKey string, msg proto.Message, extraHeaders map[string]any) error {
	body, encoding, err := iproto.Encode(msg, iproto.EncodeOptions{Compress: false})
	if err != nil {
		return fmt.Errorf("reporter: 编码消息失败: %w", err)
	}

	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		Type:            msgType,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
		HeaderExtra:     extraHeaders,
	})

	sig := iproto.Sign(m.signKey, env.SignParamsFor())
	env.WithSignature(sig)

	pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
	defer cancel()
	outcome, err := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeReport, routingKey, env)
	if err != nil {
		return fmt.Errorf("reporter: 发布消息 %s 失败: %w", env.Properties.MessageID, err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("reporter: 消息 %s 未被判定为已投递，outcome=%s", env.Properties.MessageID, outcome)
	}
	return nil
}

// writeLatestToOutbox 把状态/进度的当前值写入 store 的 outbox 集合，ID 按
// "<kind>.<pipeline_id>" 覆盖写入（Upsert 语义天然满足"只保留最新"）。m.opts.Store 为
// 空时（未接入 store 的过渡态、部分单元测试）直接跳过，不是错误。
func (m *Module) writeLatestToOutbox(kind, pipelineID string, msg proto.Message) {
	if m.opts.Store == nil {
		return
	}
	raw, err := proto.Marshal(msg)
	if err != nil {
		slog.Error("marshal outbox entry failed", slog.String("kind", kind), slog.String("pipeline_id", pipelineID), slog.Any("error", err))
		return
	}
	doc := map[string]any{
		"kind":        kind,
		"pipeline_id": pipelineID,
		"body_b64":    base64.StdEncoding.EncodeToString(raw),
		"updated_at":  m.now().Format(time.RFC3339Nano),
	}
	if err := m.opts.Store.Put(store.Op{Collection: "outbox", ID: kind + "." + pipelineID, Doc: doc}); err != nil {
		slog.Error("write outbox failed", slog.String("kind", kind), slog.String("pipeline_id", pipelineID), slog.Any("error", err))
	}
}

// writeEventsToOutbox 把一批未能发布成功的事件写入 store 的 outbox 集合，ID 按
// "event.<event_id>" 各自独立（不覆盖，全部保留，NODE-RPT-07）。
func (m *Module) writeEventsToOutbox(events []*pendingEvent) {
	if m.opts.Store == nil {
		return
	}
	for _, pe := range events {
		raw, err := proto.Marshal(pe.event)
		if err != nil {
			slog.Error("marshal event outbox entry failed", slog.String("event_id", pe.event.EventId), slog.Any("error", err))
			continue
		}
		doc := map[string]any{
			"kind":        "event",
			"event_id":    pe.event.EventId,
			"pipeline_id": pe.event.PipelineId,
			"body_b64":    base64.StdEncoding.EncodeToString(raw),
			"enqueued_at": pe.enqueued.Format(time.RFC3339Nano),
		}
		if err := m.opts.Store.Put(store.Op{Collection: "outbox", ID: "event." + pe.event.EventId, Doc: doc}); err != nil {
			slog.Error("write event outbox failed", slog.String("event_id", pe.event.EventId), slog.Any("error", err))
		}
	}
}
