package transport

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"time"
	"uuid"

	"github.com/xmapst/xamqp"

	"github.com/xmapst/xdts/internal/iproto"
)

// RPCClient owns its reply queue, bindings and correlation state.
// Replies use the named reply exchange so federation can route them. Logical retries
// retain correlation IDs while each physical publication gets a new message ID.
// Queue declaration and consumption share a channel so reconnect restores both.
type RPCClient struct {
	instanceID string
	pub        *Publisher
	consumer   *Consumer
	cfg        rpcClientConfig

	mu      sync.Mutex
	pending map[string]chan *Message // correlation_id -> 等待回复的调用

	closed    chan struct{} // 关闭一次，通知所有仍在等待的 Call 尽快返回
	closeOnce sync.Once
}

type rpcClientConfig struct {
	initialBackoff    time.Duration
	maxBackoff        time.Duration
	perAttemptTimeout time.Duration
	maxAttempts       int
}

// 默认重试参数：单次尝试预算 5 秒（含发布确认 + 等回复），指数退避从 200ms 到 3s 封顶，
// 最多 5 次尝试——对"联邦绑定传播延迟"这种量级（通常是秒级以内）留了足够余量，
// 同时避免调用方无限期挂起。生产环境可通过 RPCClientOption 按实际链路延迟调整。
const (
	DefaultRPCInitialBackoff    = 200 * time.Millisecond
	DefaultRPCMaxBackoff        = 3 * time.Second
	DefaultRPCPerAttemptTimeout = 5 * time.Second
	DefaultRPCMaxAttempts       = 5
)

// RPCClientOption 定制 RPCClient 的重试参数。
type RPCClientOption func(*rpcClientConfig)

// WithRPCInitialBackoff 设置第一次重试前的等待时长。
func WithRPCInitialBackoff(d time.Duration) RPCClientOption {
	return func(c *rpcClientConfig) { c.initialBackoff = d }
}

// WithRPCMaxBackoff 设置指数退避的上限。
func WithRPCMaxBackoff(d time.Duration) RPCClientOption {
	return func(c *rpcClientConfig) { c.maxBackoff = d }
}

// WithRPCPerAttemptTimeout 设置单次尝试（发布确认 + 等待回复）的时间预算。
func WithRPCPerAttemptTimeout(d time.Duration) RPCClientOption {
	return func(c *rpcClientConfig) { c.perAttemptTimeout = d }
}

// WithRPCMaxAttempts 设置最多尝试次数（含首次），必须 >= 1。
func WithRPCMaxAttempts(n int) RPCClientOption {
	return func(c *rpcClientConfig) { c.maxAttempts = n }
}

// EnvelopeDecorator 在一次 Call 尝试的信封定稿（MessageID/CorrelationID 已经是这次
// attempt 最终取值、x-reply-key header 已经设好）之后、PublishEnvelope 之前对信封做
// 最后加工，典型用途是签名。
//
// 每次尝试都会被调用一次，不是只在第一次尝试时调用——这是本类型存在的唯一原因：
// 如果调用方在调用 Call 之前就把签名算好、直接把签好的信封传进 req，签的是追加
// ".attemptN" 后缀之前的 MessageID，物理发布时用的却是追加后缀之后的 MessageID，
// 两者不一致，验签方必然拒绝。装饰器把"签名"这一步搬到了"MessageID 已经是这次
// attempt 最终取值"之后，每次尝试都重新签一遍，签的对象与实际发出去的完全一致。
//
// 返回非 nil error 时，本次尝试视为失败（等同于发布失败），错误会被包一层"第几次
// 尝试装饰信封失败"后计入退避重试；不会静默跳过装饰直接发布——"装饰失败"（如签名
// 用的密钥不可用）与"没有配置装饰器"是两种不同的情况，调用方必须能分辨，静默跳过
// 会让一个应该报错的场景看起来像正常发出去了。
type EnvelopeDecorator func(env *iproto.Envelope) error

// callConfig 是单次 Call 调用的可选配置，与 rpcClientConfig（整个 RPCClient 生命周期
// 内固定不变的重试参数）分开：签名密钥这类"这一次调用什么目标、用什么凭据"的信息
// 天然是按调用而不是按客户端固定的（同一个 RPCClient 可能先后调用不同节点，各节点的
// 签名密钥互不相同，见 internal/iproto/sign.go 的 HMAC 派生模型），所以装饰器做成 Call 的
// 可选参数而不是 NewRPCClient 的构造参数。
type callConfig struct {
	decorate          EnvelopeDecorator
	perAttemptTimeout time.Duration
	maxAttempts       int
}

// CallOption 定制单次 Call 调用的行为。
type CallOption func(*callConfig)

// WithEnvelopeDecorator 为本次 Call 调用设置信封装饰钩子（见 EnvelopeDecorator 文档）。
func WithEnvelopeDecorator(d EnvelopeDecorator) CallOption {
	return func(c *callConfig) { c.decorate = d }
}

// WithCallRetryPolicy overrides this call's budget without changing shared
// client settings. A single attempt avoids overlapping temporary dump sessions.
func WithCallRetryPolicy(timeout time.Duration, attempts int) CallOption {
	return func(c *callConfig) { c.perAttemptTimeout, c.maxAttempts = timeout, attempts }
}

// NewRPCClient 创建一个跨中心 RPC 客户端：声明并订阅本次调用方专属的回复队列
// xdts.reply.<instanceId>（独占、自动删除，见 internal/transport/topology.go 的 ReplyQueueOptions），
// 并创建一个开启 confirm+mandatory 的 Publisher 用于发送请求。
//
// instanceID 由调用方生成并保证在同一个 Conn 的生命周期内唯一——同一个 RPCClient 内部所有 Call 共用同一个回复队列/instanceID，
// 靠 correlation_id 而不是队列名区分并发中的多次调用。
func NewRPCClient(conn *Conn, instanceID string, opts ...RPCClientOption) (*RPCClient, error) {
	if conn == nil || conn.xc == nil {
		return nil, ErrConnManagerNil
	}
	if instanceID == "" {
		return nil, ErrMissingInstanceID
	}
	cfg := rpcClientConfig{
		initialBackoff:    DefaultRPCInitialBackoff,
		maxBackoff:        DefaultRPCMaxBackoff,
		perAttemptTimeout: DefaultRPCPerAttemptTimeout,
		maxAttempts:       DefaultRPCMaxAttempts,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxAttempts < 1 {
		return nil, ErrInvalidRPCMaxAttempts
	}

	pub, err := NewPublisher(conn)
	if err != nil {
		return nil, err
	}

	rc := &RPCClient{
		instanceID: instanceID,
		pub:        pub,
		cfg:        cfg,
		pending:    make(map[string]chan *Message),
		closed:     make(chan struct{}),
	}

	replyExchange := ReplyExchangeOptions()
	consumer, err := NewConsumer(conn, ReplyQueue(instanceID),
		WithConcurrency(1),

		WithConsumerQueueDeclare(true, true),
		// 交换机 + 绑定同样挪到这条通道上声明：交换机是持久化的，单独声明本来不受
		// 重连影响，但绑定依附于队列，队列被删的同时绑定也没了，必须和队列同一条
		// 通道、同一次恢复里一起重新声明，不能拆开。
		func(o *xamqp.ConsumerOptions) {
			o.ExchangeOptions = []xamqp.ExchangeOptions{{
				Name:    replyExchange.Name,
				Kind:    replyExchange.Kind,
				Durable: replyExchange.Durable,
				Declare: true,
				Bindings: []xamqp.Binding{{
					RoutingKey: RoutingKeyReply(instanceID),
					Declare:    true,
				}},
			}}
		},
	)
	if err != nil {
		pub.Close()
		return nil, err
	}
	rc.consumer = consumer
	if err := consumer.Run(rc.handleReply); err != nil {
		// 这里用不带超时的 Close() 是**已查实无风险**，不是没查过：Run 刚返回错误，
		// 处理器从未启动，xamqp 的 handlerMu 无人持有，优雅关闭的等待立即返回。
		// （Consumer.Close 文档说明了它在一般情况下没有等待上限。）
		consumer.Close()
		pub.Close()
		return nil, err
	}
	return rc, nil
}

func (rc *RPCClient) handleReply(msg *Message) {
	correlationID := msg.Properties.CorrelationID

	rc.mu.Lock()
	ch, ok := rc.pending[correlationID]
	rc.mu.Unlock()

	if err := msg.Ack.Ack(); err != nil {
		slog.Error("rpc reply ack failed",
			slog.String("correlation_id", correlationID),
			slog.Any("error", err))
	}

	if !ok {
		slog.Warn("received unmatched rpc reply",
			slog.String("correlation_id", correlationID))
		return
	}
	select {
	case ch <- msg:
	default:
		// 同一个 correlation_id 收到了第二条回复（例如对端重复应答）：调用方早已拿到
		// 第一条回复并返回，channel 缓冲区已满，多余的直接丢弃，不阻塞消费循环。
		slog.Warn("received duplicate rpc reply, discarding extra",
			slog.String("correlation_id", correlationID))
	}
}

// Call 发送一次 RPC 请求并阻塞等待匹配的回复，失败时按配置的退避策略重试。
//
// req 不会被修改：每次尝试基于 req 的 Properties/Headers/Body 构造一个新的信封，
// 只覆盖 CorrelationID（固定不变）与 MessageID（每次尝试追加序号，保证每条物理消息
// 有独立的 message_id，这是 Publisher.PublishEnvelope 判定 Outcome 的前提）。
// req.Properties.CorrelationID 为空时自动生成一个；req.Properties.MessageID 为空时
// 以 correlation_id 作为基底——但这条回落路径目前只对绕开 iproto.NewEnvelope、手工
// 构造 Envelope 的调用方生效：经 NewEnvelope 构造的信封，MessageID 留空时会被
// NewEnvelope 自己填成一个新 UUID（见该函数文档），传到 Call 时必然非空，不会再走到
// 这条回落分支。这条分支的作用是不让手工构造的 Envelope 出现空 MessageID，不是
// "留空换取以 correlation_id 为基底"的推荐用法——需要 MessageID 承载业务标识的
// 调用方，应当在构造信封时显式传入，不要依赖这里的回落。
//
// exchange/routingKey 是请求要发到的目标（例如 ExchangeRPC/RoutingKeyRPC()），
// 回复固定走 ExchangeReply/RoutingKeyReply(instanceID)，由 x-reply-key header
// 告诉对端——对端按 ReplyRoutingKeyFor 读取。
func (rc *RPCClient) Call(ctx context.Context, exchange, routingKey string, req *iproto.Envelope, opts ...CallOption) (*Message, error) {
	if req == nil {
		return nil, ErrMissingMessageID
	}
	cfg := callConfig{perAttemptTimeout: rc.cfg.perAttemptTimeout, maxAttempts: rc.cfg.maxAttempts}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.perAttemptTimeout <= 0 || cfg.maxAttempts <= 0 {
		return nil, fmt.Errorf("transport: invalid RPC call retry policy")
	}

	correlationID := req.Properties.CorrelationID
	if correlationID == "" {
		correlationID = newCorrelationID()
	}
	baseMessageID := req.Properties.MessageID
	if baseMessageID == "" {
		baseMessageID = correlationID
	}
	headers := make(map[string]any, len(req.Headers)+1)
	maps.Copy(headers, req.Headers)
	headers[iproto.HeaderReplyKey] = RoutingKeyReply(rc.instanceID)

	replyCh := make(chan *Message, 1)
	rc.registerPending(correlationID, replyCh)
	defer rc.unregisterPending(correlationID)

	backoff := rc.cfg.initialBackoff
	var lastErr error
	for attempt := 1; attempt <= cfg.maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("transport: RPC 调用（correlation_id=%s）被取消: %w", correlationID, ctx.Err())
		case <-rc.closed:
			return nil, ErrRPCClientClosed
		default:
		}

		attemptEnv := &iproto.Envelope{
			Properties: req.Properties,
			Headers:    headers,
			Body:       req.Body,
		}
		attemptEnv.Properties.CorrelationID = correlationID
		attemptEnv.Properties.MessageID = fmt.Sprintf("%s.attempt%d", baseMessageID, attempt)

		// 装饰钩子必须每次尝试都重新跑一遍：这次 attempt 的 MessageID 刚刚定稿，
		// 与上一次尝试不同；只在 attempt==1 时调用会让
		// 第 2 次及以后的每一次物理发布都带着"为上一个 MessageID 计算出的结果"，
		// 对签名这种场景来说等于每次重试都必然失败。
		if cfg.decorate != nil {
			if err := cfg.decorate(attemptEnv); err != nil {
				lastErr = fmt.Errorf("第 %d 次尝试装饰信封失败: %w", attempt, err)
				stop, stopErr := rc.waitBeforeNextAttempt(ctx, correlationID, attempt, cfg.maxAttempts, &backoff)
				if stopErr != nil {
					return nil, stopErr
				}
				if stop {
					break
				}
				continue
			}
		}

		attemptCtx, cancel := context.WithTimeout(ctx, cfg.perAttemptTimeout)
		outcome, err := rc.pub.PublishEnvelope(attemptCtx, exchange, routingKey, attemptEnv)
		switch {
		case err != nil:
			cancel()
			lastErr = fmt.Errorf("第 %d 次尝试发布失败: %w", attempt, err)
		case outcome != OutcomeConfirmed:
			cancel()
			lastErr = fmt.Errorf("%w（第 %d 次尝试，outcome=%s）", ErrRPCRequestUnroutable, attempt, outcome)
		default:
			select {
			case msg := <-replyCh:
				cancel()
				return msg, nil
			case <-attemptCtx.Done():
				cancel()
				lastErr = fmt.Errorf("%w（第 %d 次尝试，超过 %s）", ErrRPCTimeout, attempt, cfg.perAttemptTimeout)
			case <-rc.closed:
				cancel()
				return nil, ErrRPCClientClosed
			}
		}

		stop, stopErr := rc.waitBeforeNextAttempt(ctx, correlationID, attempt, cfg.maxAttempts, &backoff)
		if stopErr != nil {
			return nil, stopErr
		}
		if stop {
			break
		}
	}
	return nil, fmt.Errorf("transport: RPC 调用（correlation_id=%s）重试 %d 次后仍失败: %w",
		correlationID, cfg.maxAttempts, lastErr)
}

// waitBeforeNextAttempt 是"这次尝试失败后，该等退避进入下一次尝试、还是该彻底停止"
// 的公共判断——装饰失败（cfg.decorate 返回 error）与发布/等待回复失败共用同一套退避
// 与终止逻辑，不需要为装饰失败单独抄一遍退避等待代码。
//
// 返回值供调用方三选一处理：
//   - stopErr != nil：ctx 被取消或客户端已关闭，调用方应直接把 stopErr 作为 Call 的
//     返回值（已经包好 correlation_id 上下文，不需要调用方再包一层）。
//   - stop == true（stopErr == nil）：已经是最后一次尝试，调用方应该 break 出循环，
//     用累积的 lastErr 走 Call 末尾统一的"重试 N 次后仍失败"收尾。
//   - stop == false：已经等完退避、backoff 已按指数增长封顶，调用方应该 continue
//     进入下一次尝试。
func (rc *RPCClient) waitBeforeNextAttempt(ctx context.Context, correlationID string, attempt, maxAttempts int, backoff *time.Duration) (stop bool, stopErr error) {
	if attempt == maxAttempts {
		return true, nil
	}
	select {
	case <-time.After(*backoff):
	case <-ctx.Done():
		return true, fmt.Errorf("transport: RPC 调用（correlation_id=%s）被取消: %w", correlationID, ctx.Err())
	case <-rc.closed:
		return true, ErrRPCClientClosed
	}
	*backoff *= 2
	if *backoff > rc.cfg.maxBackoff {
		*backoff = rc.cfg.maxBackoff
	}
	return false, nil
}

func (rc *RPCClient) registerPending(correlationID string, ch chan *Message) {
	rc.mu.Lock()
	rc.pending[correlationID] = ch
	rc.mu.Unlock()
}

func (rc *RPCClient) unregisterPending(correlationID string) {
	rc.mu.Lock()
	delete(rc.pending, correlationID)
	rc.mu.Unlock()
}

// Close 关闭消费者与发布者，并让所有仍在等待回复的 Call 立即以 ErrRPCClientClosed
// 返回，而不是继续等到各自的超时——closed 只关闭一次、从不向其中发送数据，
// 因此不存在"handleReply 并发写入一个可能已被关闭的 channel"这种竞态。
//
// rc.consumer.Close() 是不带超时的那个重载（见 Consumer.Close 文档）：它等 handleReply
// 交还 handlerMu，没有上限。触发条件是"关闭的同时恰有一条回复正卡在 msg.Ack.Ack() 上"。
//
// handleReply 里其余部分都不会长时间不返回：查 pending 表在 rc.mu 下且只短暂持有；
// 往调用方 channel 送消息是 `select` 带 `default`，满了就丢不阻塞。唯一不受本包控制的
// 是那次 ack——它走 amqp091 的发送路径，对端不读且 TCP 发送缓冲写满时不返回。
//
// 没有直接改成 CloseWithTimeout：本方法目前没有超时参数，给它挑一个默认值是接口决定
// 而不是注释能承担的，这里先把事实记下来。要改的话，正确的做法是让 Close 接受
// 一个时长（或加 CloseWithTimeout 重载）由调用方决定，而不是在这里硬编码一个魔数。
func (rc *RPCClient) Close() {
	rc.close(0)
}

// CloseWithTimeout bounds reply-consumer shutdown for startup callers.
func (rc *RPCClient) CloseWithTimeout(timeout time.Duration) {
	rc.close(timeout)
}

func (rc *RPCClient) close(timeout time.Duration) {
	rc.closeOnce.Do(func() { close(rc.closed) })
	if rc.consumer != nil {
		if timeout > 0 {
			rc.consumer.CloseWithTimeout(timeout)
		} else {
			rc.consumer.Close()
		}
	}
	if rc.pub != nil {
		rc.pub.Close()
	}
}

// newCorrelationID 为一次 Call 生成一个新的 correlation_id（调用方没有显式提供时使用）。
// 用标准库 uuid.NewV7()：单调递增有利于日志/排障按时间排序，且不需要额外依赖。
func newCorrelationID() string {
	return uuid.NewV7().String()
}

// ReplyRoutingKeyFor 从一条已解析的 RPC 请求信封里读出 x-reply-key header，
// 供响应方（通常是 Manager 的 RPC 处理器，不属于本包）构造回复时使用：
//
//	replyKey, ok := transport.ReplyRoutingKeyFor(reqEnv)
//	if !ok { /* 不是一条期待回复的 RPC 请求，按错误处理 */ }
//	pub.PublishEnvelope(ctx, transport.ExchangeReply, replyKey, replyEnv)
//
// 找不到该 header 时 ok=false，调用方不应该猜测/拼一个默认路由键。
func ReplyRoutingKeyFor(env *iproto.Envelope) (string, bool) {
	return env.HeaderString(iproto.HeaderReplyKey)
}
