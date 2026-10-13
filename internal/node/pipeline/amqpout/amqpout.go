package amqpout

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/xmapst/xhive"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive/timer"
)

const (
	// defaultMaxMessageBytes 是编码后信封 Body 的防御性上限（NODE-MQO-01"单条消息须
	// 小于各中心 Broker 的 max_message_size"）。取 128MiB 是 RabbitMQ 3.12+
	// advanced.config max_message_size 的实际默认值（ENV-02 要求按各中心部署的真实
	// 值核对）；本包不假设所有中心都用这个默认值，Options.MaxMessageBytes 可覆盖。
	defaultMaxMessageBytes = 128 * 1024 * 1024

	// defaultPublishTimeout 是单次 PublishEnvelope 尝试等待 confirm 结算的超时。
	// 数据面消息可能远大于上报面（reporter/logship 默认 5s），取更宽松的默认值。
	defaultPublishTimeout = 30 * time.Second

	// defaultInitialBackoff/defaultMaxBackoff/defaultMultiplier 控制单次 Send* 调用
	// 内部"未投递则退避重发"的指数退避节奏（NODE-MQO-02）。初始值比 binlogin 的重连
	// 退避（默认 1s 起）更小：这里退避的是同一条消息的重发，不是重新建立连接，
	// 代价远低于断线重连，值得更快地第一次重试。
	defaultInitialBackoff = 200 * time.Millisecond
	defaultMaxBackoff     = 30 * time.Second
	defaultMultiplier     = 2.0
)

// iPublisher 是本包发布信封所需的最小接口，*transport.Publisher 天然实现它。
// 与 reporter/logship 各自独立定义同形接口而不共享类型是同一个理由（见
// logship.iPublisher 的注释）：避免通过共享内部接口类型制造隐性耦合。
type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

// ModuleName 按 pipeline_id 拼出该 pipeline amqp_out 实例在 xhive 动态模块注册表
// 里的名字（与 binlogin.ModuleName 同一模式：每条 pipeline 一个实例，名字必须包含
// pipeline_id 才能满足 xhive 动态模块注册表"同名拒绝"的唯一性要求）。
func ModuleName(pipelineID string) string {
	return "amqp_out." + pipelineID
}

type Options struct {
	// PipelineID 写入 ModuleName、routing key 与背压事件。
	PipelineID string
	RunID      uint64
	// Center/NodeID 用于信封 headers（x-center/x-node-id）与签名。
	Center string
	NodeID string
	// Token 是签名用的 node_token（MQ-10），不得写日志。New() 会派生并缓存 signKey
	// （见 Module.signKey 字段注释），与 reporter/logship 同一模式。
	Token string
	// ProtocolVersion 写入 x-proto-ver；0 时使用 iproto.CurrentProtocolVersion()。
	ProtocolVersion uint32

	// Publisher 是实际发布信封的对象，通常是 *transport.Publisher（
	// "句柄注入"：本包不拥有、不关闭此句柄；生产 Loader 为每条源流水线
	// 注入独立 Publisher，并在源模块卸载完成后关闭它）。
	Publisher iPublisher

	// MaxMessageBytes 是编码后信封 Body 的防御性上限，默认 128MiB
	// （defaultMaxMessageBytes 的注释）。
	MaxMessageBytes int
	// PublishTimeout 是单次发布尝试等待 confirm 结算的超时，默认 30s。
	PublishTimeout time.Duration
	// PublishInitialBackoff/PublishMaxBackoff/PublishMultiplier 控制单次 Send* 调用
	// 内部重发的指数退避，默认 200ms / 30s / 2.0。
	PublishInitialBackoff time.Duration
	PublishMaxBackoff     time.Duration
	PublishMultiplier     float64

	// ShutdownTimeout 映射到 xhive 模块级关闭超时选项。
	ShutdownTimeout time.Duration

	now func() time.Time
	// sleep 覆盖退避等待的实现，默认基于 time.After；测试注入快速/可观测的假实现，
	// 避免真实等待数十秒的退避序列。
	sleep func(ctx context.Context, d time.Duration) error
}

func (o Options) nowFunc() func() time.Time {
	if o.now != nil {
		return o.now
	}
	return time.Now
}

func (o Options) sleepFunc() func(ctx context.Context, d time.Duration) error {
	if o.sleep != nil {
		return o.sleep
	}
	return ctxSleep
}

// ctxSleep 是退避等待的默认实现：等待 d 或 ctx 取消，先到者决定返回值。
func ctxSleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o Options) validate() error {
	var errs []error
	if strings.TrimSpace(o.PipelineID) == "" {
		errs = append(errs, errors.New("amqpout: PipelineID 不能为空"))
	}
	if strings.TrimSpace(o.Center) == "" {
		errs = append(errs, errors.New("amqpout: Center 不能为空"))
	}
	if strings.TrimSpace(o.NodeID) == "" {
		errs = append(errs, errors.New("amqpout: NodeID 不能为空"))
	}
	if strings.TrimSpace(o.Token) == "" {
		errs = append(errs, errors.New("amqpout: Token 不能为空"))
	}
	if o.Publisher == nil {
		errs = append(errs, errors.New("amqpout: Publisher 不能为空"))
	}
	return errors.Join(errs...)
}

// Module 是 amqp_out 动态模块，每条 pipeline 一个实例。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time
	// sleep 见 Options.sleep 的注释；从 opts 提取到 Module 字段只是减少每次调用时的
	// 一层 nil 判断，语义与 opts.sleepFunc() 完全一致。
	sleep func(ctx context.Context, d time.Duration) error

	// signKey 是从 opts.Token 派生出的签名密钥，New() 构造期一次性派生并缓存，
	// 不在每次发布时重新计算——与 reporter.Module.signKey/logship.Module.signKey
	// 完全同一理由（见两者字段注释），不再重复展开。
	signKey iproto.SignKey

	stats               statsCounters
	telemetryInstanceID string

	initErr error
}

// New 构造 amqpout.Module。
func New(opts Options) *Module {
	if opts.MaxMessageBytes <= 0 {
		opts.MaxMessageBytes = defaultMaxMessageBytes
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = defaultPublishTimeout
	}
	if opts.PublishInitialBackoff <= 0 {
		opts.PublishInitialBackoff = defaultInitialBackoff
	}
	if opts.PublishMaxBackoff <= 0 {
		opts.PublishMaxBackoff = defaultMaxBackoff
	}
	if opts.PublishMultiplier <= 1 {
		opts.PublishMultiplier = defaultMultiplier
	}
	if opts.ProtocolVersion == 0 {
		opts.ProtocolVersion = iproto.CurrentProtocolVersion()
	}

	m := &Module{
		opts:                opts,
		telemetryInstanceID: uuidx.New().String(),
		now:                 opts.nowFunc(),
		sleep:               opts.sleepFunc(),
		signKey:             iproto.DeriveSignKeyFromToken(opts.Token),
	}
	m.Skeleton = xhive.NewSkeleton(ModuleName(opts.PipelineID),
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

// Priority 小于 binlogin.Module 的默认优先级（50）：binlogin 包文档要求
// "下游的 Priority 更小（先初始化）"。
//
// 值仍然保留 40，不要因为"理由不承重"就去掉这个约束：按 internal/node/controller/diff.go 的 hotUpdateTriggered 论证的
// 同一个道理，多一条廉价约束的代价是零，而少一条约束的代价是将来某次改动让 Sink 真的
// 依赖上 OnInit 时没有任何东西拦住它。
func (m *Module) Priority() uint { return 40 }

// InitErr 语义与 store/reporter/logship/binlogin 的 InitErr 完全一致：xhive.Run/
// AddDynamicModules 对模块初始化失败没有把具体原因传回调用方的通用手段，这里保留
// 字段以便调用方与测试直接断言。
func (m *Module) InitErr() error { return m.initErr }

// Stats 返回 amqp_out 的发布统计快照。
type Stats struct {
	BatchesSent        int64
	WatermarksSent     int64
	SnapshotChunksSent int64
	SnapshotEndsSent   int64
	RetriedCalls       int64 // 需要至少重试一次才最终成功的 Send* 调用数
	FailedCalls        int64 // ctx 取消/超时导致最终失败（未投递成功）的 Send* 调用数
	OversizeRejected   int64 // 因超过 MaxMessageBytes 被直接拒绝（不重试）的调用数
	BackpressureEvents int64 // 上报给 reporter 的背压事件数
}

func (m *Module) Stats() Stats {
	return Stats{
		BatchesSent:        m.stats.batchesSent.Load(),
		WatermarksSent:     m.stats.watermarksSent.Load(),
		SnapshotChunksSent: m.stats.snapshotChunksSent.Load(),
		SnapshotEndsSent:   m.stats.snapshotEndsSent.Load(),
		RetriedCalls:       m.stats.retriedCalls.Load(),
		FailedCalls:        m.stats.failedCalls.Load(),
		OversizeRejected:   m.stats.oversizeRejected.Load(),
		BackpressureEvents: m.stats.backpressureEvents.Load(),
	}
}

// OnInit 只做依赖校验：本包没有需要注册的 ChanRPC 处理器或定时器（见包文档"为什么
// 不需要 ChanRPC/Hold，也不需要自己的会话 goroutine"），Send* 方法在调用方自己的
// goroutine 里同步执行，不依赖事件循环先跑起来。
func (m *Module) OnInit() error {
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}
	m.RegisterTimer("amqpout.telemetry", func(_ int64, _ map[string]string) { m.reportTelemetry() })
	m.NewTimer("amqpout.telemetry", time.Second, timer.WithTicker())
	return nil
}

// OnDestroy 无需任何动作：本包不持有需要在关闭前排空的事件循环私有状态（没有
// LinearContext/AsyncGo 任务，Send* 调用的生命周期完全由调用方的 ctx 决定，不归
// Module 的关闭流程管理）。
func (m *Module) OnDestroy() {}

// Close 只需要等待 Skeleton 排空在途异步任务；amqp_out 不持有需要自己关闭的资源
// （Publisher 是外部注入的共享句柄，生命周期由 cmd/node 管理，"句柄注入"）。
func (m *Module) Close() error {
	return m.Skeleton.Close()
}

// statsCounters 的具体定义见 internal/node/pipeline/amqpout/stats.go。
