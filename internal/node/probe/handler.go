package probe

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/xmapst/xhive/chanrpc"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

func (m *Module) onEnvelopeMsg(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(EnvelopeMsg)
	if !ok {
		slog.Error("received unexpected message type", slog.String("want", "probe.EnvelopeMsg"))
		return nil
	}
	if err := req.validate(); err != nil {
		slog.Error("envelopemsg validation failed, discarding", slog.Any("error", err))
		if req.Ack != nil {
			m.discard(req.Ack)
		}
		return nil
	}
	m.AsyncGo("probe.handle_envelope", func() {
		m.handleEnvelope(req.Envelope, req.Ack)
	}, nil)
	return nil
}

// handleEnvelope 是可单测的核心逻辑：只认 probe.request 类型 → 解析回复地址 →
// 验签+解码+分派（verifyAndDispatch）→ 回执（reply）。确认策略见 doc.go"控制面消息
// 处理失败时不持有 delivery"一节。
func (m *Module) handleEnvelope(env *iproto.Envelope, ack iAckHandle) {
	if env.Properties.Type != iproto.MessageTypeProbeRequest {
		// 非 probe.request 消息直接丢弃，避免错误消息被当作探测请求执行。
		slog.Warn("control message type not handled by probe",
			slog.String("type", env.Properties.Type))
		m.discard(ack)
		return
	}

	replyKey, ok := transport.ReplyRoutingKeyFor(env)
	if !ok {
		// 没有回复地址：这条 ProbeRequest 本身不符合协议约定，无法送达任何回执，
		// 持有到天荒地老也不会自愈（见 internal/node/probe/doc.go）。
		slog.Error("probe request missing reply key")
		m.discard(ack)
		return
	}

	budget := m.opts.RequestTimeout
	if iproto.VerifyEnvelope(env, m.signKey, m.now(), m.opts.SignatureWindow) == nil {
		var request iproto.ProbeRequest
		if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &request); err == nil && request.Type == iproto.ProbeType_PROBE_TYPE_LINK {
			budget = 35 * time.Second
			remaining := time.UnixMilli(env.Properties.TimestampMS).Add(40 * time.Second).Sub(m.now())
			budget = min(budget, remaining)
		} else if err == nil && request.Type == iproto.ProbeType_PROBE_TYPE_DATA_CHECK {
			budget = min(125*time.Second, time.UnixMilli(env.Properties.TimestampMS).Add(130*time.Second).Sub(m.now()))
		} else if err == nil && request.Type == iproto.ProbeType_PROBE_TYPE_POSITION {
			budget = 65 * time.Second
			// A queued request must not open a dump after Manager released its
			// server_id lease. The creation timestamp is signature protected.
			remaining := time.UnixMilli(env.Properties.TimestampMS).Add(70 * time.Second).Sub(m.now())
			if remaining < budget {
				budget = remaining
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()

	result := m.verifyAndDispatch(ctx, env)
	m.reply(ctx, replyKey, env.Properties.CorrelationID, result, ack)
}

// verifyAndDispatch 是 handleEnvelope 的业务核心：验签（验不过拒绝执行，见 internal/node/probe/doc.go
// "入站 ProbeRequest 必须验签"一节）→ 解码 → 按 ProbeType 分派。任何一步失败都返回
// 一个 Success=false 的 ProbeResult，从不返回 nil（硬约束 4：失败也要回执）。
func (m *Module) verifyAndDispatch(ctx context.Context, env *iproto.Envelope) *iproto.ProbeResult {
	// 验签必须先于解码：拒绝执行意味着连"这条请求业务上想干什么"都不应该被
	// 信任，签名覆盖的正是信封整体（含 body_hash），解码结果本身不可信，没有
	// 必要——也不应该——在验签之前先看一眼 body。
	if err := iproto.VerifyEnvelope(env, m.signKey, m.now(), m.opts.SignatureWindow); err != nil {
		logVerifyFailure(env.Properties.MessageID, err)
		return &iproto.ProbeResult{
			Success: false,
			Error: &iproto.Error{
				Code:    "PROBE_REQUEST_SIGNATURE_INVALID",
				Message: "ProbeRequest 签名校验失败，已拒绝执行",
			},
		}
	}

	req := &iproto.ProbeRequest{}
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, req); err != nil {
		// 即使请求体本身解不出来，也按硬约束 4 尽力回执一条失败结果（task_id 留空，
		// 没有别的来源可以还原它）——Manager 侧的 RPCClient 按 correlation_id 而不是
		// task_id 路由回复，这条回执仍然能送达等待中的调用方，不是完全沉默。
		slog.Error("decode probe request failed", slog.Any("error", err))
		return &iproto.ProbeResult{
			Success: false,
			Error: &iproto.Error{
				Code:    "PROBE_REQUEST_MALFORMED",
				Message: "解码 ProbeRequest 失败",
			},
		}
	}
	return m.dispatch(ctx, req)
}

// 签名时间窗失败按 Warn 记录，其它验签错误按 Error 记录，日志不输出认证材料。
func logVerifyFailure(messageID string, err error) {
	fields := []any{slog.String("message_id", messageID), slog.Any("error", err)}
	switch {
	case errors.Is(err, iproto.ErrSignatureInvalid):
		slog.Error("probe request signature verification failed", fields...)
	case errors.Is(err, iproto.ErrTimestampOutOfWindow):
		slog.Warn("probe request timestamp outside allowed window", fields...)
	default:
		// 缺 x-sign / 缺或不兼容 x-proto-ver：与签名不匹配同等对待，均为 Error——
		// 协议实现不完整或被篡改，不是时钟漂移这一类良性噪声。
		slog.Error("probe request verify precondition failed", fields...)
	}
}

func (m *Module) reply(ctx context.Context, replyKey, correlationID string, result *iproto.ProbeResult, ack iAckHandle) {
	if err := m.publishResult(ctx, replyKey, correlationID, result); err != nil {
		m.stats.failed.Add(1)
		slog.Error("publish probe result failed",
			slog.String("task_id", result.TaskId), slog.Any("error", err))
		m.discard(ack)
		return
	}
	m.stats.handled.Add(1)
	if err := ack.Ack(); err != nil {
		slog.Error("ack probe request failed", slog.Any("error", err))
	}
}

// discard 统一处理"这条 delivery 没有必要继续持有"的几种情况（结构损坏、类型不认识、
// 没有回复地址、回执发布失败），见 doc.go"控制面消息处理失败时不持有 delivery"。
func (m *Module) discard(ack iAckHandle) {
	if err := ack.NackDiscard(); err != nil {
		slog.Error("nack control message failed", slog.Any("error", err))
	}
}
