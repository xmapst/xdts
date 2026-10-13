// Package logship 实现 Node 的 logship 静态模块：接收本进程通过 slog Handler（见 internal/node/logship/handler.go）单向 Cast 过来的日志
// 记录，独立于 reporter 的通道与队列（不同的 ChanRPC 消息类型、不同的内存队列、不同的
// 发布路由键），周期性批量压缩后发布到管理中心 Broker。
//
// logship 是静态模块，Priority 20。
package logship

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

const (
	timerKindFlush = "logship.flush"

	defaultQueueSize       = 2000
	defaultBatchInterval   = time.Second
	defaultBatchMaxRecords = 500
	defaultPublishTimeout  = 5 * time.Second
)

// iPublisher 是 logship 发布消息所需的最小接口，*transport.Publisher 天然实现它。
// 与 reporter 各自定义自己的 iPublisher 接口（而不是共享一个），是因为两个模块的发布
// 通道要求彼此独立（"logship 独立通道与队列"），不应该通过共享一个内部类型
// 制造隐性耦合——即使两边的接口定义恰好长得一样。
type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

// Options 是构造 Module 所需的运行参数。
type Options struct {
	Center string
	NodeID string
	// Token 是签名用的 node_token（MQ-10），不得写日志。New() 会立即派生出
	// signKey 并缓存（见 Module.signKey），之后的签名调用只用 signKey，不再
	// 读取这份明文；本字段本身没有清空——OnInit 的 validate() 仍需要用它判断
	// "调用方是否提供了 Token"，清空会让该校验永远误判为空（曾在这次改动里
	// 踩到，改用「保留明文但只在 New()/validate() 两处读」这个更简单也更安全
	// 的方案）。
	Token string
	// ProtocolVersion 写入 x-proto-ver；0 时使用 iproto.CurrentProtocolVersion()。
	ProtocolVersion uint32

	// Publisher 是实际发布信封的对象，通常是 *transport.Publisher。
	Publisher iPublisher

	// QueueSize 是内存日志队列的容量上限：队列满时丢弃新到达的记录并计数
	// （NODE-RPT-04）。默认 2000。
	QueueSize int
	// BatchInterval 是发布周期。默认 1s。
	BatchInterval time.Duration
	// BatchMaxRecords 是单条 LogBatch 最多携带的记录数（避免单条 AMQP 消息过大）。
	// 默认 500。
	BatchMaxRecords int
	// PublishTimeout 是单条 LogBatch 等待 Broker confirm 的超时。默认 5s。
	PublishTimeout time.Duration
	// ShutdownTimeout 映射到 xhive 模块级关闭超时选项。
	ShutdownTimeout time.Duration

	now func() time.Time
}

func (o Options) nowFunc() func() time.Time {
	if o.now != nil {
		return o.now
	}
	return time.Now
}

func (o Options) validate() error {
	var errs []error
	if strings.TrimSpace(o.Center) == "" {
		errs = append(errs, errors.New("logship: Center 不能为空"))
	}
	if strings.TrimSpace(o.NodeID) == "" {
		errs = append(errs, errors.New("logship: NodeID 不能为空"))
	}
	if strings.TrimSpace(o.Token) == "" {
		errs = append(errs, errors.New("logship: Token 不能为空"))
	}
	if o.Publisher == nil {
		errs = append(errs, errors.New("logship: Publisher 不能为空"))
	}
	return errors.Join(errs...)
}

// Module 是 logship 静态模块。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time

	signKey iproto.SignKey

	linear *xhive.LinearContext

	// pending 只在事件循环（ChanRPC handler / Timer handler）里读写，不需要加锁
	pending []*iproto.LogRecord

	stats statsCounters

	initErr error
}

// New 构造 logship.Module。
func New(opts Options) *Module {
	if opts.QueueSize <= 0 {
		opts.QueueSize = defaultQueueSize
	}
	if opts.BatchInterval <= 0 {
		opts.BatchInterval = defaultBatchInterval
	}
	if opts.BatchMaxRecords <= 0 {
		opts.BatchMaxRecords = defaultBatchMaxRecords
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = defaultPublishTimeout
	}
	if opts.ProtocolVersion == 0 {
		opts.ProtocolVersion = iproto.CurrentProtocolVersion()
	}

	m := &Module{opts: opts, now: opts.nowFunc(), signKey: iproto.DeriveSignKeyFromToken(opts.Token)}
	m.Skeleton = xhive.NewSkeleton(ModuleName,
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

// Priority 固定为 20（与 reporter 同优先级）。
func (m *Module) Priority() uint { return 20 }

// InitErr 语义与 store.Module.InitErr / reporter.Module.InitErr 完全一致：xhive.Run
// 对模块初始化失败没有返回值，cmd/node 必须靠这个字段判断退出码。该字段与方法不得
// 因「看起来多余」被删除。
func (m *Module) InitErr() error { return m.initErr }

// Stats 返回 logship 的发布/丢弃统计快照（NODE-RPT-04"限流并统计丢弃数"）。
type Stats struct {
	Published            int64
	QueueFullDropped     int64 // 队列满时被丢弃的记录数
	PublishFailedDropped int64 // 发布失败被丢弃的记录数（日志不进 outbox，不重试）
}

func (m *Module) Stats() Stats {
	return Stats{
		Published:            m.stats.published.Load(),
		QueueFullDropped:     m.stats.queueFullDropped.Load(),
		PublishFailedDropped: m.stats.publishFailedDropped.Load(),
	}
}

// OnInit 注册 ChanRPC 处理器与发布周期定时器。
func (m *Module) OnInit() error {
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	m.linear = m.NewLinearContext()

	if err := m.RegisterChanRPC(LogRecordMsg{}, m.onLogRecord); err != nil {
		wrapped := fmt.Errorf("logship: 注册 ChanRPC 处理器失败: %w", err)
		m.initErr = wrapped
		return wrapped
	}

	m.RegisterTimer(timerKindFlush, func(_ int64, _ map[string]string) {
		m.flush()
	})
	m.NewTimer(timerKindFlush, m.opts.BatchInterval, timer.WithTicker())

	return nil
}

// OnDestroy 做最后一次尽力而为的 flush；真正等待它跑完的是随后框架调用的 Close。
func (m *Module) OnDestroy() {
	m.flush()
}

// Close 只需要等待 Skeleton 排空在途异步任务；logship 不持有需要自己关闭的资源
// （Publisher 是外部注入的共享句柄，生命周期由 cmd/node 管理）。
func (m *Module) Close() error {
	return m.Skeleton.Close()
}

// onLogRecord 是 LogRecordMsg 的 ChanRPC 处理器：队列满时直接丢弃新记录并计数
// （NODE-RPT-04），不是错误、不记 ERROR 级别日志（这是设计允许的正常路径，高频出现
// 属于预期，用 ERROR 记录会淹没真正的错误）。
func (m *Module) onLogRecord(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	req, ok := ci.Request.(LogRecordMsg)
	if !ok {
		return nil
	}
	if err := req.validate(); err != nil {
		return nil
	}
	if len(m.pending) >= m.opts.QueueSize {
		m.stats.queueFullDropped.Add(1)
		return nil
	}
	m.pending = append(m.pending, req.Record)
	return nil
}
