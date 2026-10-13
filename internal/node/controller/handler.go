package controller

import (
	"errors"
	"log/slog"
	"runtime/debug"

	"github.com/xmapst/xhive/chanrpc"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/probe"
	"github.com/xmapst/xdts/internal/transport"
)

// 本函数运行在 xamqp 自己的消费者 goroutine 上（transport.Consumer.Run 的文档：
// handler 不进 xhive 事件循环），dispatchEnvelope 内部对 config.changed 的处理
// （onConfigChangedMessage）与对 probe.request 的 AsyncCall 都不做阻塞 IO（两者
// 入队本身都是非阻塞的，AsyncCall 只是比 Cast 多了一个同步返回的 error），因此
// 单一消费者 goroutine 已经足够，见 OnInit 里 WithConcurrency(1) 的说明。
func (m *Module) onDelivery(msg *transport.Message) {
	defer m.recoverDelivery(msg.Properties, msg.Ack)
	if msg.Redelivered {
		slog.Debug("control message redelivered", slog.String("message_id", msg.Properties.MessageID), slog.String("message_type", msg.Properties.Type))
	}

	env, err := msg.Envelope()
	if err != nil {
		slog.Error("parse control envelope failed", slog.Any("error", err))
		m.discard(msg.Ack)
		return
	}
	m.dispatchEnvelope(msg, env, msg.Ack)
}

// Recover and report control-handler panics before acknowledgement; transport
// fallback cannot supply the business message context.
func (m *Module) recoverDelivery(props iproto.Properties, ack iAckHandle) {
	r := recover()
	if r == nil {
		return
	}
	attrs := []any{
		slog.String("message_id", props.MessageID),
		slog.String("message_type", props.Type),
		slog.Any("panic", r),
		slog.String("stack", string(debug.Stack())),
	}
	err := ack.NackDiscard()
	switch {
	case err == nil:
		slog.Error("panic while dispatching control message, delivery discarded", attrs...)
	case errors.Is(err, transport.ErrAckHandleAlreadyResolved):
		slog.Warn("panic while dispatching control message after delivery was resolved", attrs...)
	default:
		slog.Error("panic while dispatching control message and discard failed",
			append(attrs, slog.Any("discard_error", err))...)
	}
}

// dispatchEnvelope 按信封类型分派控制消息，ack 由调用方传入。
func (m *Module) dispatchEnvelope(msg *transport.Message, env *iproto.Envelope, ack iAckHandle) {
	switch env.Properties.Type {
	case iproto.MessageTypePipelineStateExport:
		if err := m.onStateExportMessage(msg, env, ack); err != nil {
			m.discard(ack)
		}
	case iproto.MessageTypeConfigChanged:
		if err := m.onConfigChangedMessage(msg); err != nil {
			slog.Error("handle config.changed failed", slog.Any("error", err))
			m.discard(ack)
			return
		}
		// onConfigChangedMessage 成功即表示信封已经被正确解码并转交
		// onConfigChanged——真正的定向拉取在 Skeleton.AsyncGo 里异步进行，失败时
		// onConfigChanged 自己的 cb 只记一条 WARN（见 internal/node/controller/dispatch.go），不会、也不
		// 应该让这条 config.changed 通知本身被重新投递（重试的是下一次 Manager
		// 主动重发或下一轮启动全量拉取，不是这条 AMQP delivery 本身）。
		m.ack(ack)
	case iproto.MessageTypeProbeRequest:
		// err == nil：入队成功，ack 所有权已经随信封转移给 probe（EnvelopeMsg.
		// Ack），本方法此后不再碰它——两次确认会触发
		// transport.ErrAckHandleAlreadyResolved（其中一次），因此成功分支既不
		// Ack 也不 discard。err != nil：入队从未成立，所有权从未转移，必须由
		// 本方法自己收回并处置——这里统一按 discard 处理，不用 NackRequeue。
		if err := m.AsyncCall(probe.ModuleName, probe.EnvelopeMsg{Envelope: env, Ack: msg.Ack}, m.onProbeEnvelopeCastResult); err != nil {
			slog.Warn("cast probe.request to probe failed, reclaiming ack",
				slog.String("message_id", env.Properties.MessageID), slog.Any("error", err))
			m.discard(ack)
		}
	case iproto.MessageTypeCmdPipeline:

		if err := m.onCmdPipelineMessage(msg, ack); err != nil {
			slog.Error("handle cmd.pipeline failed", slog.Any("error", err))
			m.discard(ack)
		}
	default:

		slog.Warn("control message type not handled by controller",
			slog.String("type", env.Properties.Type))
		m.discard(ack)
	}
}

// onProbeEnvelopeCastResult 是转发给 probe 的 AsyncCall 的回调（AsyncCall 要求
// callback 非空，不能用 Cast 那样彻底不关心结果），只用于诊断观测，绝不能在这里
// 对 ack 做任何操作。
//
// 回调触发的时刻是 probe.onEnvelopeMsg **返回**的那一刻（vendored
// github.com/xmapst/xhive/chanrpc/server.go 的 exec：handler 的返回值直接经 ci.ret 送回调用方，
// 不等待 handler 内部触发的异步工作），也就是 EnvelopeMsg 校验通过、
// AsyncGo(handleEnvelope) 已经排队之后——不是 handleEnvelope 真正跑完之后。
// ack 的所有权在 dispatchEnvelope 里 AsyncCall 成功入队的那一刻就已经转移给
// probe，此刻早已经不属于本模块；无论 handleEnvelope 最终处理结果如何，都是
// probe 自己的职责边界，本回调不应该、也不能安全地把 ack 的处置权要回来——
// 那会与 probe 内部可能已经在途的 Ack/Nack 竞争，触发
// transport.ErrAckHandleAlreadyResolved。ri.Err 非空目前只可能来自
// onEnvelopeMsg 内部 panic 或 chanrpc 消息名未注册这类框架级异常（正常路径下
// onEnvelopeMsg 恒定返回 nil），实际发生的概率并不比"probe 包完全没有注册"
// 更高，这里只记录一条 WARN 便于事后排查，不触发任何补救动作。
func (m *Module) onProbeEnvelopeCastResult(ri *chanrpc.RetInfo) {
	if ri != nil && ri.Err != nil {
		slog.Warn("probe.request delivered to probe but handler reported an error",
			slog.Any("error", ri.Err))
	}
}

// ack 统一处理"这条 delivery 已经正确处理完毕"的确认路径。
func (m *Module) ack(ack iAckHandle) {
	if err := ack.Ack(); err != nil {
		slog.Error("ack control message failed", slog.Any("error", err))
	}
}

// discard 统一处理"这条 delivery 没有必要继续持有"的几种情况（结构损坏、解码失败、
// 类型不认识）——控制面消息之间没有顺序依赖，没有"持有等重试"的价值，与 probe
// 移交前的同名方法同一个理由。
func (m *Module) discard(ack iAckHandle) {
	if err := ack.NackDiscard(); err != nil {
		slog.Error("nack control message failed", slog.Any("error", err))
	}
}
