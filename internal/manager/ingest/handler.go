package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// iDelivery 是 process() 实际用到的最小子集（解封装 + 消息 ID + 确认）。
// *transport.Message 通过 messageDelivery 适配到这个接口，不直接依赖
// *transport.Message 具体类型——transport.AckHandle 只能由 transport 包内部
// 构造（newAckHandle 未导出），单元测试没有办法在 ingest 包外自己拼出一个
// 可控的 *transport.Message；引入这层小接口后，测试可以直接注入假实现驱动
// process() 的分支（成功/解封装失败/验签失败/落库失败都恰好 Ack 一次），
// 不需要一个真实 Broker 投递。
type iDelivery interface {
	Envelope() (*iproto.Envelope, error)
	MessageID() string
	Ack() error
}

// messageDelivery 把 *transport.Message 适配成 iDelivery。
type messageDelivery struct{ msg *transport.Message }

func (m messageDelivery) Envelope() (*iproto.Envelope, error) { return m.msg.Envelope() }
func (m messageDelivery) MessageID() string                   { return m.msg.Ack.MessageID() }
func (m messageDelivery) Ack() error                          { return m.msg.Ack.Ack() }

// handleReport 是 xdts.manager.report 队列的消费入口（MGR-MON-01/02、
// MGR-NODE-04：心跳/状态/进度/事件四类上报）。签名匹配
// transport.Handler，直接传给 transport.Consumer.Run。
func (s *Service) handleReport(msg *transport.Message) {
	s.process(messageDelivery{msg}, s.reportCounters(), s.dispatchReport)
}

// handleLog 是 xdts.manager.log 队列的消费入口（MGR-MON-02：日志集中
// 入库）。
func (s *Service) handleLog(msg *transport.Message) {
	s.process(messageDelivery{msg}, s.logCounters(), s.dispatchLog)
}

// queueCounters 将队列标签与其统计计数器绑定，避免平行参数被调换。
type queueCounters struct {
	label          string
	processed      *atomic.Int64
	failed         *atomic.Int64
	inflight       *atomic.Int64
	ackDuplicate   *atomic.Int64
	ackUnconfirmed *atomic.Int64
}

func (s *Service) reportCounters() queueCounters {
	return queueCounters{
		label:          "report",
		processed:      &s.stats.reportProcessed,
		failed:         &s.stats.reportFailed,
		inflight:       &s.stats.reportInflight,
		ackDuplicate:   &s.stats.reportAckDuplicate,
		ackUnconfirmed: &s.stats.reportAckUnconfirmed,
	}
}

func (s *Service) logCounters() queueCounters {
	return queueCounters{
		label:          "log",
		processed:      &s.stats.logProcessed,
		failed:         &s.stats.logFailed,
		inflight:       &s.stats.logInflight,
		ackDuplicate:   &s.stats.logAckDuplicate,
		ackUnconfirmed: &s.stats.logAckUnconfirmed,
	}
}

// process verifies and dispatches control-plane reports, then acknowledges once.
// Processing errors are logged rather than requeued indefinitely. Failed ACKs are
// counted separately because broker redelivery can repeat committed log chunks.
func (s *Service) process(
	msg iDelivery,
	qc queueCounters,
	dispatch func(context.Context, *iproto.Envelope, uuidx.UUID) error,
) {
	queueLabel := qc.label

	// inflight 的递减必须是**最外层** defer（先注册 ⇒ 最后执行），排在下面
	// 那个 Ack defer 之后：Ack() 自己阻塞时这条投递仍然没处理完，此时
	// inflight 要照样是 1。反过来注册就会出现"投递卡在 Ack 上、inflight 已
	// 归零"，那正是这个量被加进来要消除的那种同形。
	qc.inflight.Add(1)
	defer qc.inflight.Add(-1)

	defer func() {
		err := msg.Ack()
		if err == nil {
			return
		}

		if errors.Is(err, transport.ErrAckHandleAlreadyResolved) {
			qc.ackDuplicate.Add(1)
			slog.Error("ack called twice on the same delivery",
				slog.String("queue", queueLabel),
				slog.String("message_id", msg.MessageID()),
				slog.Any("error", err))
			return
		}
		qc.ackUnconfirmed.Add(1)
		slog.Error("delivery left unconfirmed, broker will redeliver it",
			slog.String("queue", queueLabel),
			slog.String("message_id", msg.MessageID()),
			slog.Any("error", err))
	}()

	// Recover business-handler panics with message context before deferred ACK runs.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		qc.failed.Add(1)
		slog.Error("panic while processing delivery",
			slog.String("queue", queueLabel),
			slog.String("message_id", msg.MessageID()),
			slog.Any("panic", r),
			slog.String("stack", string(debug.Stack())))
	}()

	qc.processed.Add(1)
	ctx := context.Background()
	now := s.now()

	env, err := msg.Envelope()
	if err != nil {
		qc.failed.Add(1)
		slog.Error("envelope decode failed",
			slog.String("queue", queueLabel),
			slog.String("message_id", msg.MessageID()),
			slog.Any("error", err))
		return
	}

	nodeID, err := s.verify(ctx, env, now)
	if err != nil {
		qc.failed.Add(1)
		logVerifyFailure(queueLabel, msg.MessageID(), env.Properties.Type, nodeID, err)
		return
	}

	if err := dispatch(ctx, env, nodeID); err != nil {
		qc.failed.Add(1)
		slog.Error("persist failed",
			slog.String("queue", queueLabel),
			slog.String("message_id", msg.MessageID()),
			slog.String("type", env.Properties.Type),
			slog.String("node_id", nodeID.String()),
			slog.Any("error", err))
		return
	}
}

// logVerifyFailure 按 sign.go 自己的分类建议区分日志级别："时间窗失败可能是
// 时钟漂移，签名失败可能是 token 泄露/被吊销"——后者明显更值得单独告警。
func logVerifyFailure(queueLabel, messageID, msgType string, nodeID uuidx.UUID, err error) {
	fields := []any{
		slog.String("queue", queueLabel),
		slog.String("message_id", messageID),
		slog.String("type", msgType),
		slog.String("node_id", nodeID.String()),
		slog.Any("error", err),
	}
	switch {
	case errors.Is(err, iproto.ErrSignatureInvalid):
		slog.Error("signature verification failed", fields...)
	case errors.Is(err, iproto.ErrTimestampOutOfWindow):
		slog.Warn("timestamp outside allowed window", fields...)
	default:
		slog.Error("verify precondition failed", fields...)
	}
}

// dispatchReport 按 Properties.Type 把 report 队列的消息分派到具体 handler。
func (s *Service) dispatchReport(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	switch env.Properties.Type {
	case iproto.MessageTypeNodeHeartbeat:
		return s.onNodeHeartbeat(ctx, env, nodeID)
	case iproto.MessageTypePipelineStatus:
		return s.onPipelineStatus(ctx, env, nodeID)
	case iproto.MessageTypePipelineProgress:
		return s.onPipelineProgress(ctx, env, nodeID)
	case iproto.MessageTypeEventError, iproto.MessageTypeEventDDL, iproto.MessageTypeEventConflict,
		iproto.MessageTypeEventUnknownOrigin, iproto.MessageTypeEventBackpressure,
		iproto.MessageTypeEventLocalStoreRebuild, iproto.MessageTypeEventLinkProbeFailed:
		return s.onEvent(ctx, env, nodeID)
	default:
		return fmt.Errorf("ingest: report 队列收到未知消息类型 %q", env.Properties.Type)
	}
}

// dispatchLog 按 Properties.Type 把 log 队列的消息分派到具体 handler。
func (s *Service) dispatchLog(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	switch env.Properties.Type {
	case iproto.MessageTypeLogBatch:
		return s.onLogBatch(ctx, env, nodeID)
	default:
		return fmt.Errorf("ingest: log 队列收到未知消息类型 %q", env.Properties.Type)
	}
}
