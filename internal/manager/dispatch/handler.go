package dispatch

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// iDelivery 是 processRPC/processRegister 实际用到的最小子集（解封装 +
// 消息 ID + 确认）——与 internal/manager/ingest 的同名类型同一个理由：
// *transport.Message 的 Ack 字段（*transport.AckHandle）只能由 transport
// 包内部构造，单元测试没有办法在 dispatch 包外自己拼出一个可控的
// *transport.Message；引入这层小接口后，测试可以直接注入假实现驱动
// processRPC/processRegister 的分支（成功/解封装失败/验签失败/业务处理
// 失败都恰好 Ack 一次），不需要一个真实 Broker 投递。两个包各自持有一份
// 同形状的接口，不共享定义，理由同 internal/manager/dispatch/verify.go 顶部注释（ingest 与
// dispatch 是两条独立的消费线）。
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

// HandleRPCMessage 是 xdts.manager.rpc 队列的消费入口——config.pull 真实
// 处理、config.apply.result 落库、cmd.result 落库（internal/manager/dispatch/ctl_cmd_result.go 里的
// `onCmdResultReq`，会走 `repo.InsertEvents`）。签名匹配 transport.Handler，
// cmd/manager/main.go 的接线点直接把它传给 transport.Consumer.Run。
//
// RPC 与 register 队列的处理器在对应控制文件中实现。
func (d *Dispatcher) HandleRPCMessage(msg *transport.Message) {
	d.processRPC(messageDelivery{msg})
}

func (d *Dispatcher) HandleRegisterMessage(msg *transport.Message) {
	d.processRegister(messageDelivery{msg})
}

// ackMessage 无条件确认一条投递——两个队列的处理骨架（processRPC/
// processRegister）都用 defer 调用它，见 processRPC 文档对"为什么失败也
// Ack、不 Nack"的完整论证。
func ackMessage(msg iDelivery) {
	if err := msg.Ack(); err != nil {
		slog.Error("dispatch: ack failed",
			slog.String("message_id", msg.MessageID()), slog.Any("error", err))
	}
}

// recoverHandlerPanic records queue, message ID and stack before deferred ACK.
// Control-plane handlers retain their acknowledge-on-error policy. Do not rely
// on the transport library's panic fallback to report business context.
func recoverHandlerPanic(queue string, msg iDelivery) {
	r := recover()
	if r == nil {
		return
	}
	slog.Error("dispatch: panic in message handler",
		slog.String("queue", queue),
		slog.String("message_id", msg.MessageID()),
		slog.Any("panic", r),
		slog.String("stack", string(debug.Stack())))
}

// processRPC 是 xdts.manager.rpc 队列的处理骨架：解封装 → 验签
// （verifyNode）→ 按类型分发，无论成功失败都在末尾恰好 Ack 一次一条投递。
//
// 策略与 internal/manager/ingest.Service.process 完全一致，直接复用该
// 函数文档已经完整论证过的结论，不在这里重新展开：这条队列上的三类消息
// （config.pull/config.apply.result/cmd.result）都不是"分块提交、
// NackRequeue 会导致重复落库"那种数据面语义，处理失败换成结构化 Error
// 日志 + Ack 掉，不引入无限重投或重复副作用的风险；两个队列都没有配置
// x-delivery-limit，对一条格式永久错误的消息 NackRequeue 还会造成无限
// 重投循环，把"这条处理失败"和"消息卡死整条队列的消费"解耦同样适用于
// 这里。
func (d *Dispatcher) processRPC(msg iDelivery) {
	defer ackMessage(msg)
	// 注册在 ackMessage 之后 ⇒ panic 展开时先跑，先记日志再 Ack。
	defer recoverHandlerPanic("rpc", msg)

	ctx := context.Background()
	now := d.now()

	env, err := msg.Envelope()
	if err != nil {
		slog.Error("dispatch: envelope decode failed",
			slog.String("queue", "rpc"), slog.String("message_id", msg.MessageID()), slog.Any("error", err))
		return
	}

	if env.Properties.Type == iproto.MessageTypeNodeHello {
		if err := d.onNodeHelloReq(ctx, env); err != nil {
			logVerifyFailure("rpc", msg.MessageID(), env.Properties.Type, uuidx.Nil, err)
		}
		return
	}
	nodeID, err := d.verifyNode(ctx, env, now)
	if err != nil {
		logVerifyFailure("rpc", msg.MessageID(), env.Properties.Type, nodeID, err)
		return
	}

	if err := d.dispatchRPC(ctx, env, nodeID); err != nil {
		slog.Error("dispatch: rpc message handling failed",
			slog.String("message_id", msg.MessageID()), slog.String("type", env.Properties.Type),
			slog.String("node_id", nodeID.String()), slog.Any("error", err))
	}
}

// dispatchRPC 按 Properties.Type 把 xdts.manager.rpc 队列的消息分派到
// 具体处理器。
func (d *Dispatcher) dispatchRPC(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	switch env.Properties.Type {
	case iproto.MessageTypeConfigPull:
		return d.onConfigPullReq(ctx, env, nodeID)
	case iproto.MessageTypeSourceRecovery:
		return d.onSourceRecoveryReq(ctx, env, nodeID)
	case iproto.MessageTypeConfigApplyResult:
		return d.onConfigApplyResultReq(ctx, env, nodeID)
	case iproto.MessageTypeCmdResult:
		return d.onCmdResultReq(ctx, env, nodeID)
	default:

		return fmt.Errorf("dispatch: xdts.manager.rpc 队列收到未预期的消息类型 %q", env.Properties.Type)
	}
}

// processRegister 是 xdts.manager.register 队列的处理骨架：只处理
// register.request 一种类型（这是唯一绑定到 xdts.register 交换机
// register 路由键的消息类型），不做验签——见 internal/manager/dispatch/ctl_stub.go onRegisterReq
// 文档"为什么不验签"一节。
func (d *Dispatcher) processRegister(msg iDelivery) {
	defer ackMessage(msg)
	// 同 processRPC：注册在 ackMessage 之后 ⇒ panic 展开时先跑。
	defer recoverHandlerPanic("register", msg)

	ctx := context.Background()
	env, err := msg.Envelope()
	if err != nil {
		slog.Error("dispatch: envelope decode failed",
			slog.String("queue", "register"), slog.String("message_id", msg.MessageID()), slog.Any("error", err))
		return
	}

	if env.Properties.Type != iproto.MessageTypeRegisterRequest {
		slog.Error("dispatch: xdts.manager.register queue received unexpected message type",
			slog.String("message_id", msg.MessageID()), slog.String("type", env.Properties.Type))
		return
	}

	if err := d.onRegisterReq(ctx, env); err != nil {
		slog.Error("dispatch: register.request handling failed",
			slog.String("message_id", msg.MessageID()), slog.Any("error", err))
	}
}

// logVerifyFailure 按 internal/iproto/sign.go 自己的分类建议区分日志级别——与
// internal/manager/ingest.logVerifyFailure 同一个判断标准（签名不对可能是
// token 泄露/被吊销，值得 Error；时间戳超窗多半是时钟漂移，Warn 即可），
// 两个包各自持有一份，不共享实现，理由同 verify.go 顶部注释。
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
		slog.Error("dispatch: signature verification failed", fields...)
	case errors.Is(err, iproto.ErrTimestampOutOfWindow):
		slog.Warn("dispatch: timestamp outside allowed window", fields...)
	default:
		slog.Error("dispatch: verify precondition failed", fields...)
	}
}
