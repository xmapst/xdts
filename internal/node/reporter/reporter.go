// Package reporter 实现 Node 的周期性心跳、状态、进度和事件上报。
// Reporter 在自己的事件循环中维护缓冲队列和重试状态。
package reporter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	timerKindFlush     = "reporter.flush"
	timerKindHeartbeat = "reporter.heartbeat"

	defaultBatchInterval       = time.Second
	defaultHeartbeatInterval   = 5 * time.Second
	defaultPublishTimeout      = 5 * time.Second
	defaultEventTTL            = 24 * time.Hour
	defaultEventMaxCount       = 2000
	defaultProgressInterval    = 5 * time.Second
	defaultQueueInspectTimeout = 2 * time.Second
)

type iQueueInspector interface {
	InspectQueue(context.Context, string) (transport.QueueStats, error)
}

type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

// iStoreAccess 是 reporter 持久化 outbox 所需的最小 store 访问接口，*store.Client
// 天然实现它。为空时 reporter 仍可正常工作，只是失败上报不会写入本地状态库
// （只在内存中重试，进程重启后丢失）——单元测试与「未接入 store」的过渡态都用得到。
type iStoreAccess interface {
	Put(op store.Op) error
}

// Options 是构造 Module 所需的运行参数。
type Options struct {
	// Center and NodeID bind report routing and authenticated envelope identity.
	Center string
	NodeID string
	// Token 是签名用的 node_token（MQ-10），不得写日志。
	Token string
	// ProtocolVersion 写入 x-proto-ver；0 时使用 iproto.CurrentProtocolVersion()。
	ProtocolVersion uint32
	// NodeVersion 写入 NodeHeartbeat.node_version；空时使用 "dev"。
	NodeVersion string

	// Publisher 是实际发布信封的对象，通常是 *transport.Publisher（NewPublisher
	// 构造，注入前由上层完成连接与 confirm 模式初始化，「句柄注入」）。
	Publisher iPublisher
	// QueueInspector queries the local broker using the already shared connection.
	QueueInspector      iQueueInspector
	ProgressInterval    time.Duration
	QueueInspectTimeout time.Duration
	// Store 用于把发布失败的状态/进度/事件写入本地状态库 outbox 集合
	// （NODE-XDOC-07/NODE-RPT-07），通常是 store.NewClient(...)。可为空。
	Store iStoreAccess
	// StatsFunc 返回 store 模块的批量提交统计，用于满足 NODE-XDOC-08
	// （状态库大小与提交耗时随心跳上报）；可为空。
	StatsFunc func() store.Stats
	// 典型取值是 store 模块实例的 RebuildEvents 方法（如 mod.RebuildEvents，cmd/node
	// 按此模式注入）。本字段只应在 store.Priority()=5 严格早于本模块 Priority()=20 完成
	// OnInit 这个既有的 xhive 启动顺序保证下才安全——见 store.Module.RebuildEvents 方法
	// 文档的完整证明；本模块只在自己的 OnInit 里调用一次，不在其它时机重复读取。
	RebuildEventsFunc func() []store.RebuildRecord

	// BatchInterval 是发布周期：把这段时间内到达的 Cast 消息与当前应重试的 outbox
	// 事件合并发布一次。默认 1s。
	BatchInterval time.Duration
	// HeartbeatInterval 默认 5s（NODE-RPT-01）。
	HeartbeatInterval time.Duration
	// PublishTimeout 是单条消息等待 Broker confirm 的超时。默认 5s。
	PublishTimeout time.Duration
	// ShutdownTimeout 映射到 xhive 模块级关闭超时选项。
	ShutdownTimeout time.Duration

	// EventTTL/EventMaxCount 约束事件类 outbox 的保留（NODE-RPT-07）：超过 TTL 或
	// 超过条数上限的最旧事件会被丢弃并计数，不是设计默认路径，只是容量兜底。
	EventTTL      time.Duration
	EventMaxCount int

	// ProcessStartedAt is this process's start time for heartbeat uptime.
	ProcessStartedAt time.Time
	// now 供测试注入固定时钟。
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
		errs = append(errs, errors.New("reporter: Center 不能为空"))
	}
	if strings.TrimSpace(o.NodeID) == "" {
		errs = append(errs, errors.New("reporter: NodeID 不能为空"))
	}
	if strings.TrimSpace(o.Token) == "" {
		errs = append(errs, errors.New("reporter: Token 不能为空"))
	}
	if o.Publisher == nil {
		errs = append(errs, errors.New("reporter: Publisher 不能为空"))
	}
	return errors.Join(errs...)
}

// Module 是 reporter 静态模块。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time

	signKey iproto.SignKey

	linear *xhive.LinearContext

	// The reporter actor exclusively owns these pending snapshots and counters.
	statusByPipeline         map[statusKey]*iproto.PipelineStatus
	statusSessionID          string
	statusSessionStartedAtMS int64
	statusSequences          map[statusKey]uint64
	statusInstanceFloors     map[statusKey]statusInstanceFloor
	progressByPipeline       map[statusKey]*iproto.PipelineProgress
	pendingEvents            []*pendingEvent
	lastProgressFlush        time.Time
	telemetryByPipeline      map[statusKey]*telemetryWindow

	// 两个字段而不是一个 nil 判断：空切片与"从未收到过"必须可分辨。前者的意思是
	// "本节点确实承载 0 条"，后者是"此刻不知道"——而心跳协议里 pipelines[] 是
	// repeated、没有 has 位，两者在 wire 上同形，所以 presence 由
	// pipelines_known_count 承载，而它设不设值取决于这里这个 bool。
	// 已知的"不知道"窗口：启动期定向拉取超时 30s 而心跳周期 5s（约 6 个心跳），
	// 以及 controller 先于 reporter 就绪时那次 Cast 被 chanrpc 静默丢弃
	// （见 internal/node/reporter/messages.go PipelinesMirror 文档）。
	//
	// 只在事件循环里读写（onPipelinesMirror 写、publishHeartbeat 读），不加锁。
	pipelinesMirror         []*iproto.PipelineHeartbeatEntry
	pipelinesMirrorReceived bool

	cpu cpuSampler

	stats statsCounters

	initErr error
}

// New 构造 reporter.Module。
func New(opts Options) *Module {
	if opts.ProgressInterval <= 0 {
		opts.ProgressInterval = defaultProgressInterval
	}
	if opts.QueueInspectTimeout <= 0 {
		opts.QueueInspectTimeout = defaultQueueInspectTimeout
	}
	if opts.BatchInterval <= 0 {
		opts.BatchInterval = defaultBatchInterval
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = defaultHeartbeatInterval
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = defaultPublishTimeout
	}
	if opts.EventTTL <= 0 {
		opts.EventTTL = defaultEventTTL
	}
	if opts.EventMaxCount <= 0 {
		opts.EventMaxCount = defaultEventMaxCount
	}
	if opts.ProtocolVersion == 0 {
		opts.ProtocolVersion = iproto.CurrentProtocolVersion()
	}
	if opts.NodeVersion == "" {
		opts.NodeVersion = "dev"
	}
	if opts.ProcessStartedAt.IsZero() {

		opts.ProcessStartedAt = opts.nowFunc()()
		slog.Debug("reporter: process start time not provided, falling back to module construction time",
			slog.Time("fallback_started_at", opts.ProcessStartedAt))
	}

	m := &Module{
		opts:                     opts,
		now:                      opts.nowFunc(),
		signKey:                  iproto.DeriveSignKeyFromToken(opts.Token),
		statusByPipeline:         make(map[statusKey]*iproto.PipelineStatus),
		statusSessionID:          uuidx.New().String(),
		statusSessionStartedAtMS: opts.nowFunc()().UnixMilli(),
		statusSequences:          make(map[statusKey]uint64),
		statusInstanceFloors:     make(map[statusKey]statusInstanceFloor),
		progressByPipeline:       make(map[statusKey]*iproto.PipelineProgress),
	}
	m.Skeleton = xhive.NewSkeleton(ModuleName,
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

// Priority 固定为 20（与 logship 同优先级）。
func (m *Module) Priority() uint { return 20 }

// InitErr 返回 OnInit 的失败原因；语义与 store.Module.InitErr 完全一致
// （见该方法的详细说明：xhive.Run 对模块初始化失败没有返回值，cmd/node 必须靠这个
// 字段判断退出码）。该字段与方法不得因「看起来多余」被删除。
func (m *Module) InitErr() error { return m.initErr }

type Stats struct {
	PublishedTotal     int64
	FailedTotal        int64
	EventsDroppedByCap int64 // 事件类 outbox 超过 EventMaxCount 被丢弃的条数
	EventsDroppedByTTL int64 // 事件类 outbox 超过 EventTTL 被丢弃的条数
}

func (m *Module) Stats() Stats {
	return Stats{
		PublishedTotal:     m.stats.published.Load(),
		FailedTotal:        m.stats.failed.Load(),
		EventsDroppedByCap: m.stats.eventsDroppedByCap.Load(),
		EventsDroppedByTTL: m.stats.eventsDroppedByTTL.Load(),
	}
}

// OnInit 校验依赖、注册 ChanRPC 处理器与两个定时器（发布周期、心跳周期）。
func (m *Module) OnInit() error {
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	m.linear = m.NewLinearContext()

	handlers := []struct {
		msg any
		f   chanrpc.Handler
	}{
		{StatusReport{}, m.onStatus},
		{ProgressReport{}, m.onProgress},
		{iproto.PipelineTelemetry{}, m.onTelemetry},
		{SnapshotRateReport{}, m.onSnapshotRate},
		{EventReport{}, m.onEvent},
		{PipelinesMirror{}, m.onPipelinesMirror},
	}
	for _, h := range handlers {
		if err := m.RegisterChanRPC(h.msg, h.f); err != nil {
			wrapped := fmt.Errorf("reporter: 注册 ChanRPC 处理器失败: %w", err)
			m.initErr = wrapped
			return wrapped
		}
	}

	m.RegisterTimer(timerKindFlush, func(_ int64, _ map[string]string) {
		m.flush()
	})
	m.NewTimer(timerKindFlush, m.opts.BatchInterval, timer.WithTicker())

	m.RegisterTimer(timerKindHeartbeat, func(_ int64, _ map[string]string) {
		m.publishHeartbeat()
	})
	m.NewTimer(timerKindHeartbeat, m.opts.HeartbeatInterval, timer.WithTicker())

	// 必须在上面的 ChanRPC/Timer 注册完成之后调用：reportRebuildEvents 内部直接调用
	// enqueueEvent 把事件放进 m.pendingEvents，随后由 flush 定时器发布；如果提前调用，
	// 这里只是把 append 提前了几行，不影响正确性，放在最后只是让"资源就绪后再产生依赖
	// 它的副作用"这个顺序在阅读上更直观。
	m.reportRebuildEvents()

	return nil
}

func (m *Module) OnDestroy() {
	m.flush()
}

// Close 只需要等待 Skeleton 排空在途异步任务；reporter 不持有需要自己关闭的资源
// （Publisher/Store 都是外部注入的共享句柄，生命周期由 cmd/node 管理，
// 「句柄注入」——模块不关闭不属于自己的连接）。
func (m *Module) Close() error {
	return m.Skeleton.Close()
}

// cpuSampler 供 internal/node/reporter/resource.go 使用，定义在这里是因为它是 Module 的事件循环私有状态
// （只在心跳定时器里读写，不需要加锁）。
type cpuSampler struct {
	have     bool
	lastWall time.Time
	lastCPU  time.Duration
}
