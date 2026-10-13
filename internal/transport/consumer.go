package transport

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/xmapst/xamqp"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// AckHandle owns one manual delivery and permits one successful resolution.
// Unresolved cleanup logs a diagnostic; it cannot release the broker's prefetch slot.
// The cleanup flag is allocated separately so its reference cannot keep the handle alive.
type AckHandle struct {
	delivery  xamqp.Delivery
	messageID string
	resolved  *atomic.Bool // 独立分配（见上），Ack/Nack 方法与 cleanup 共同持有同一个对象
}

func newAckHandle(d xamqp.Delivery, messageID string) *AckHandle {
	resolved := new(atomic.Bool) // 与 AckHandle 本体分开的独立堆分配
	h := &AckHandle{delivery: d, messageID: messageID, resolved: resolved}
	runtime.AddCleanup(h, checkAckHandleLeak, ackHandleCleanupArg{resolved: resolved, messageID: messageID})
	return h
}

// ackHandleCleanupArg 是传给 cleanup 的独立分配对象：只含值语义的字符串与一个独立分配
// 的 *atomic.Bool，不包含指向 AckHandle 本体的指针，避免上面注释里说明的可达性陷阱。
type ackHandleCleanupArg struct {
	resolved  *atomic.Bool
	messageID string
}

func checkAckHandleLeak(arg ackHandleCleanupArg) {
	if !arg.resolved.Load() {
		slog.Error("delivery handle collected without acknowledgement",
			slog.String("message_id", arg.messageID))
	}
}

// MessageID 返回该投递携带的 message_id（AMQP message_id 属性），主要用于日志与诊断。
func (h *AckHandle) MessageID() string { return h.messageID }

// Ack 确认消息已成功处理。目标端语义：目标库提交成功后才调用。
func (h *AckHandle) Ack() error {
	if !h.resolved.CompareAndSwap(false, true) {
		return ErrAckHandleAlreadyResolved
	}
	return h.delivery.Ack(false)
}

// NackDiscard 拒绝消息且不重新入队（丢弃/进死信）。
//
// 需求 NODE-MQI-04/NODE-MQI-07：反复失败的消息不得随意丢弃；本方法只应在"跳过"经过
// Manager 显式操作并审计之后由业务代码调用，transport 本身不做这个业务判断。
func (h *AckHandle) NackDiscard() error {
	if !h.resolved.CompareAndSwap(false, true) {
		return ErrAckHandleAlreadyResolved
	}
	return h.delivery.Nack(false, false)
}

// NackRequeue 拒绝消息并重新入队。
//
// 需求 NODE-MQI-07："失败消息不得 requeue（requeue 会破坏顺序）；重试在节点内部完成，
// 消息保持未 ack 状态"——增量数据消息的重试路径不应该调用本方法，这里仍然提供它是因为
// 控制面/上报面的消费者（不受"严格有序"约束）可能需要标准的 requeue 语义；数据面调用方
// 应该直接持有 AckHandle 反复重试，重试耗尽后走 NackDiscard 并停机告警，而不是 requeue。
func (h *AckHandle) NackRequeue() error {
	if !h.resolved.CompareAndSwap(false, true) {
		return ErrAckHandleAlreadyResolved
	}
	return h.delivery.Nack(false, true)
}

// Message 是消费到的一条投递，经过最小整理（Properties/Headers 转换）但不做签名校验
// 或业务解码——那些需要消息类型相关的目标结构体与（token 等）业务上下文，属于调用方
// 职责，不属于 transport（本包"只做 xamqp 适配"的边界）。
type Message struct {
	Properties iproto.Properties
	Headers    map[string]any
	Body       []byte
	// Redelivered is the Broker delivery flag, independent of envelope identity.
	Redelivered bool

	Ack *AckHandle
}

// Envelope 把 Message 转换为 iproto.Envelope，供调用方接着调用 iproto.Verify /
// iproto.Decode。等价于 iproto.ParseEnvelope(m.Properties, m.Headers, m.Body) 的薄封装，
// 只是省得调用方自己拼三个字段。
func (m *Message) Envelope() (*iproto.Envelope, error) {
	return iproto.ParseEnvelope(m.Properties, m.Headers, m.Body)
}

// VerifyOptions 配置可选的签名校验。所有消息在解析信封时均校验精确协议版本；
// 数据面没有对端节点的验签密钥，通过 GTID、消息身份和路由约束验证执行契约。
type VerifyOptions struct {
	Key    iproto.SignKey
	Now    time.Time
	Window time.Duration // <=0 时使用 iproto.DefaultSignatureWindow（见 iproto.Verify）
}

// ParseAndDecode 先校验信封再解码。verify 非 nil 时追加签名和鉴权时间窗校验。
func ParseAndDecode(msg *Message, out proto.Message, verify *VerifyOptions) (*iproto.Envelope, error) {
	env, err := msg.Envelope()
	if err != nil {
		return nil, err
	}
	if verify != nil {
		if err := iproto.VerifyEnvelope(env, verify.Key, verify.Now, verify.Window); err != nil {
			return nil, err
		}
	}
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, out); err != nil {
		return nil, err
	}
	return env, nil
}

func newMessage(d xamqp.Delivery) *Message {
	headers := HeadersToMap(d.Headers)
	// TimestampMS 不能从 d.Timestamp（AMQP 原生 timestamp property）还原——
	// 那个字段只有秒级精度，跟发送方签名时用的毫秒值不是同一个数，会导致
	// HMAC 验签必现失败（见 iproto.HeaderTimestampMS 的完整说明）。改从
	// x-ts-ms header 还原；header 缺失或不合法时给 0（明显不合法的哨兵值，
	// 1970 年），不用一个"看起来正常但是错的"数字悄悄顶上——真正的显式
	// 报错发生在 iproto.ParseEnvelope（业务代码调用 Message.Envelope() 时
	// 触发），这里只保证不把错误的数字伪装成正常值往下传。
	tsMS, _ := iproto.TimestampMSFromHeaders(headers)
	return &Message{
		Properties: iproto.Properties{
			MessageID:       d.MessageId,
			CorrelationID:   d.CorrelationId,
			Type:            d.Type,
			TimestampMS:     tsMS,
			ContentType:     d.ContentType,
			ContentEncoding: d.ContentEncoding,
		},
		Headers:     headers,
		Body:        d.Body,
		Redelivered: d.Redelivered,
		Ack:         newAckHandle(d, d.MessageId),
	}
}

// Handler processes one delivery and owns its acknowledgement policy.
// Control consumers report invalid requests before ACK; data consumers retain
// unconfirmed deliveries on failure. Recover panics within the business handler
// so queue and message context are recorded before transport fallback runs.
type Handler func(msg *Message)

// Consumer 包装 xamqp.Consumer，固定使用 Manual 确认模式。
type Consumer struct {
	xc *xamqp.Consumer
}

// ConsumerOption 定制 Consumer 的可选参数。
type ConsumerOption func(*xamqp.ConsumerOptions)

// WithPrefetch 设置 QoS 预取数量。
func WithPrefetch(n int) ConsumerOption {
	return func(o *xamqp.ConsumerOptions) { o.QOSPrefetch = n }
}

// WithConcurrency sets handler goroutine count. Incremental queues require one
// for ordered cross-message state; single-active-consumer does not serialize
// multiple goroutines sharing a consumer tag.
func WithConcurrency(n int) ConsumerOption {
	return func(o *xamqp.ConsumerOptions) { o.Concurrency = n }
}

// WithConsumerName 设置服务端消费者标识名，便于在管理控制台里识别。
func WithConsumerName(name string) ConsumerOption {
	return func(o *xamqp.ConsumerOptions) { o.RabbitConsumerOptions.Name = name }
}

// WithConsumerQueueDeclare 覆盖默认的 WithConsumerOptionsQueueNoDeclare，让 Consumer
// 自己在消费用的这条通道上声明并拥有队列（队列名沿用 NewConsumer 的 queue 参数，
// Durable 固定 false——与 internal/transport/topology.go 的 ReplyQueueOptions 一致，独占队列不受 MQ-15
// 的 Durable=true 强制要求约束）。
//
// **只应该用在队列生命周期与这一个 Consumer 完全绑定的场景**——典型例子是独占
// （Exclusive）+自动删除（AutoDelete）队列：这种队列被 broker 删除的唯一原因就是
// "拥有它的连接断开"，这恰好也是这条 Consumer 自己的通道需要恢复的时刻，二者天然
// 同步。真正的原因见下方 NewConsumer 文档——这不是两种同样合理的风格选择，是"拓扑声明
// 收口到 Declarator"这条默认设计对这一类队列**结构性地不成立**。
func WithConsumerQueueDeclare(exclusive, autoDelete bool) ConsumerOption {
	return func(o *xamqp.ConsumerOptions) {
		o.QueueOptions.Declare = true
		o.QueueOptions.Exclusive = exclusive
		o.QueueOptions.AutoDelete = autoDelete
	}
}

// NewConsumer 创建消费者，连接到指定队列。默认不主动声明队列/交换机/绑定
// （拓扑由 Declarator 统一声明，见 topology.go；consumer 只管消费，职责分开，
// 避免拓扑声明散落在多个地方、参数互相打架）——对 durable、非独占的队列，这条职责
// 分开没有问题：这类队列不会因为连接断开被 broker 删除，声明它的 Declarator 通道
// 早关晚关都不影响队列本身继续存在。
//
// 让 Consumer 自己声明就没有这个问题：声明和消费在同一条长期存活的通道上，无论是
// amqp091-go 的原生恢复（同一次恢复里"声明队列→重新绑定→resubscribe"三步顺序执行，
// 不存在跨通道竞态），还是 xamqp 自己在原生恢复失败时兜底的 startConsumerWithRetry
// （3 秒固定间隔无限重试，Declare=true 时这步不再是空操作），都能独立地把队列连同
// 消费者一起正确恢复。
func NewConsumer(conn *Conn, queue string, opts ...ConsumerOption) (*Consumer, error) {
	if conn == nil || conn.xc == nil {
		return nil, ErrConnManagerNil
	}
	funcs := []func(*xamqp.ConsumerOptions){
		// 默认假设队列已由 Declarator 声明，这里只连接——对独占/自动删除队列不成立，
		// 见本函数上方文档与 WithConsumerQueueDeclare。
		xamqp.WithConsumerOptionsQueueNoDeclare,
		xamqp.WithConsumerOptionsConsumerAutoAck(false),
	}
	for _, opt := range opts {
		funcs = append(funcs, func(o *xamqp.ConsumerOptions) { opt(o) })
	}
	xc, err := xamqp.NewConsumer(conn.xc, queue, funcs...)
	if err != nil {
		return nil, fmt.Errorf("transport: 创建消费者失败（queue=%s）: %w", queue, err)
	}
	return &Consumer{xc: xc}, nil
}

// Run 启动消费循环，不阻塞（首次启动成功后立即返回，见 xamqp.Consumer.Run 的文档：
// 后续的重连恢复在后台协程里进行，固定 3 秒重试）。handler 内部 panic 会被 xamqp 捕获
// 并按不重入队处理（见 Handler 类型注释），但不应该依赖这一层兜底。
func (c *Consumer) Run(handler Handler) error {
	err := c.xc.Run(func(d xamqp.Delivery) xamqp.Action {
		handler(newMessage(d))
		// 始终 Manual：确认时机完全交给 AckHandle，见类型与包文档。
		return xamqp.Manual
	})
	if err != nil {
		return fmt.Errorf("transport: 启动消费失败: %w", err)
	}
	return nil
}

// Close waits for active handlers without a deadline. Use CloseWithTimeout
// when shutdown must be bounded.
func (c *Consumer) Close() {
	c.xc.Close()
}

// CloseWithTimeout 关闭消费者，最多等待 timeout 时长处理完成，超时后强制关闭。
//
// 这里传的 timeout 直接进 context.WithTimeout：传 0（或负数）得到的 ctx 当场就已过期，
// 而 xamqp 的 waitForHandlerCompletion 开头即 `else if ctx.Err() != nil { return ctx.Err() }`
// ⇒ 一条正在处理中的消息都不等，直接断开。
func (c *Consumer) CloseWithTimeout(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	c.xc.CloseWithContext(ctx)
}
