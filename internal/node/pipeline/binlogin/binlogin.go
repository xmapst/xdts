// Package binlogin reads the source binlog with the MySQL replication protocol.
// One source session owns transaction assembly, schema history, checkpoints and
// ordered publication. Filters and transforms run before transport fragmentation.
//
// Durable schema history precedes checkpoint advancement. Runtime policy changes
// occur only at complete transaction boundaries. Snapshot watermarks and DDL guards
// share observed source ordering; endpoint lineage prevents replay loops.
package binlogin

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/node/schema"
	"github.com/xmapst/xdts/internal/rule"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	timerKindReconnect  = "binlogin.reconnect"
	timerKindCheckpoint = "binlogin.checkpoint"

	defaultMaxRowsPerChunk         = 2000
	defaultHeartbeatPeriod         = 30 * time.Second
	defaultCheckpointInterval      = time.Second
	defaultReconnectInitialBackoff = time.Second
	defaultReconnectMaxBackoff     = 30 * time.Second
	defaultReconnectMultiplier     = 2.0
	defaultSendTimeout             = 30 * time.Second
	// stableSessionThreshold：一次会话存活超过这个时长才认为"这次重连恢复正常"，
	// 重置退避计数；否则视为持续故障，退避继续升级。避免"连上又立刻断"的抖动源
	// 被误判成健康连接从而一直用最小退避重试打满源库。
	stableSessionThreshold = 60 * time.Second

	defaultEpochCommitTimeout = 10 * time.Second
)

// ModuleName 按 pipeline_id 拼出该 pipeline 源端实例在 xhive 动态模块注册表里的名字。
// 与 store/reporter/logship 的包级常量 ModuleName 不同：本包每条 pipeline 一个实例，
// 名字必须包含 pipeline_id 才能满足 xhive 动态模块注册表"同名拒绝"的唯一性要求。
func ModuleName(pipelineID string) string {
	return "binlog_in." + pipelineID
}

// 实际的构造方是 internal/node/controller 的 buildSourceOptions（internal/node/controller/options.go），
// 它从 SourceSideConfig（proto/config.proto）摘取字段填这个结构体，再由
// internal/node/controller/loader.go 调 New()。
type Options struct {
	InstanceID        string
	ConfigVersion     uint64
	Budget            *resourcequota.Budget
	PublicationWindow *iproto.IncrementalWindowPolicy
	ShadowTablePolicy *iproto.ShadowTablePolicy
	// TableFilter/ColumnFilter/OriginResolver 见 interfaces.go；均可为空，为空时取
	// 各自的放行/直通默认实现。
	TableFilter       ITableFilter
	ColumnFilter      IColumnFilter
	RowFilter         iproto.ISourceRowPlan
	Transforms        *rule.TransformPlan
	DDLColumnPolicy   *iproto.DDLColumnPolicy
	OriginResolver    IOriginResolver
	OriginFilter      IOriginFilter
	MaxOriginHops     uint32
	DropUnknownOrigin bool
	TargetCenter      string
	DatabaseRoute     iproto.DatabaseLineageRoute
	// Sink 是向下游投递 DataBatch/Watermark 的接口（见 internal/node/pipeline/binlogin/interfaces.go 的 ISink），必填。
	Sink ISink
	// Store 用于检查点持久化（可为空：为空时检查点只在内存，重启后无法恢复，
	// 仅用于测试或"未接入 store"的过渡态，与 reporter.Options.Store 同一约定）。
	Store iStoreAccess
	now   func() time.Time
	// sleep 与 now 同一用途、同一可见性：限流器需要的两个时间原语都可注入，
	// 测试才能用一个假时钟测"等了多久"这个决定本身，而不必真的睡过去
	// （真睡会让一条本该确定的断言变成一条按机器负载抖动的断言）。为空取
	// sleepCtx（生产实现，见 internal/node/pipeline/binlogin/ratelimit.go）。
	sleep func(context.Context, time.Duration) error
	// PipelineID/RunID 写入 DataBatch 与检查点。
	PipelineID string
	NodeID     string
	// RequireEpochRange is enabled by the production loader after obtaining a
	// durable Manager range. Offline restarts use its local remainder.
	RequireEpochRange bool

	Center   string
	Host     string
	User     string
	Password string

	StartGTIDSet string
	// StartGTIDSetProvided distinguishes an explicit empty set on a pristine
	// snapshot source from an omitted start that must recover a checkpoint.
	StartGTIDSetProvided bool
	TLSConfig            *tls.Config
	// Snapshotting suppresses a premature RUNNING report while the source
	// snapshot module owns the snapshot/catchup status transition.
	Snapshotting bool
	// SnapshotDDLGuard coordinates background workflows until the durable H.
	SnapshotDDLGuard      IDDLGuard
	DDLSourceDependencies bool
	RunID                 uint64
	PositionRevision      uint64
	// MaxRowsPerChunk 是大事务顺序分片的阈值（NODE-BIN-03），默认 2000。
	MaxRowsPerChunk int
	// IncrementalRowsPerSec/IncrementalBytesPerSec 是增量投递限流
	// （SourceRuleSet.incremental_rate_limit，"增量限流，源端，P2"）。
	// 0 表示该路不限速——这是协议语义（RateLimit 消息缺省时两个字段都是 0），不是
	// "没设置就按 0 行/秒"。两路各自独立、都不得被超过（取 max 而非求和，理由见
	// internal/node/pipeline/binlogin/ratelimit.go 文件头）。字段名保留协议里 incremental 前缀：本包虽然只负责增量
	// 路径，但协议里另有一个形状完全相同的 SnapshotParams.rate_limit（全量读取限流，
	// 由 internal/node/controller/snapshot.go 装配给全量读取器），去掉前缀会让同时看到两者的人无从分辨这个
	// 值来自哪一个协议字段。
	IncrementalRowsPerSec  uint64
	IncrementalBytesPerSec uint64
	// HeartbeatPeriod 是 dump 心跳周期（NODE-BIN-07：源库空闲期间的水位消息节奏），
	// 默认 30s。
	HeartbeatPeriod time.Duration
	// CheckpointInterval 是检查点落盘周期，默认 1s。
	CheckpointInterval time.Duration
	// ReconnectInitialBackoff/ReconnectMaxBackoff/ReconnectMultiplier 控制断线重连的
	// 指数退避（NODE-BIN-08）。默认 1s / 30s / 2.0。
	ReconnectInitialBackoff time.Duration
	ReconnectMaxBackoff     time.Duration
	ReconnectMultiplier     float64
	// SendTimeout 是每次向 Sink 投递单个 DataBatch/Watermark 的超时保护，默认 30s。
	SendTimeout time.Duration
	// ShutdownTimeout 映射到 xhive 模块级关闭超时选项。
	ShutdownTimeout time.Duration
	// ServerID 是 dump 用的伪从库 server_id，全局唯一（MGR-PL-06），调用方保证不冲突。
	ServerID uint32
	Port     uint16
	// SourceCaseSensitive configures schemaTracker from the probed source policy.
	SourceCaseSensitive bool
}

func (o Options) nowFunc() func() time.Time {
	if o.now != nil {
		return o.now
	}
	return time.Now
}

func (o Options) sleepFunc() func(context.Context, time.Duration) error {
	if o.sleep != nil {
		return o.sleep
	}
	return sleepCtx
}

func (o Options) validate() error {
	if _, err := iproto.NormalizePublicationWindow(o.PublicationWindow); err != nil {
		return err
	}
	var errs []error
	if strings.TrimSpace(o.PipelineID) == "" {
		errs = append(errs, errors.New("binlogin: PipelineID 不能为空"))
	}
	if strings.TrimSpace(o.Host) == "" {
		errs = append(errs, errors.New("binlogin: Host 不能为空"))
	}
	if strings.TrimSpace(o.User) == "" {
		errs = append(errs, errors.New("binlogin: User 不能为空"))
	}
	if o.ServerID == 0 {
		errs = append(errs, errors.New("binlogin: ServerID 不能为 0（replication.NewBinlogSyncer 会 panic）"))
	}
	if o.Sink == nil {
		errs = append(errs, errors.New("binlogin: Sink 不能为空"))
	}
	return errors.Join(errs...)
}

// Module 是 binlog_in 动态模块，每条 pipeline 源端一个实例。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time
	// Mutable configuration belongs to the serial session. The event loop reads
	// only its atomic version mirror when emitting lifecycle observations.
	sourceRuntime  iproto.SourceRuntimeConfig
	runtimeVersion atomic.Uint64
	runtimeUpdates chan runtimeUpdate
	// These fields belong only to the serial source session, including recovery
	// before a replacement dump connection starts. Serve acquires ownership only
	// after joining the source worker during shutdown.
	publishWindow    *publicationWindow
	publishFirstSeq  uint64
	publishRead      gmysql.GTIDSet
	publishConfirmed gmysql.GTIDSet

	// selfClient 供会话 goroutine（AsyncGo 的 f）向本模块自身 Cast 消息，
	// 把中间进展安全地交回事件循环处理（见包文档"阻塞 IO 与 actor 状态边界"）。
	selfClient      *chanrpc.Client
	streamReady     chan struct{}
	streamReadyOnce sync.Once
	streamWorkers   sync.WaitGroup

	// runSessionFn 是一次会话的实际执行体，默认绑定到 m.runSession（真实连接
	// replication.BinlogSyncer，见 internal/node/pipeline/binlogin/session.go）。测试可在 OnInit 之前替换成不碰
	// 网络的假实现，从而验证 scheduleSession/handleSessionEnded/退避重连这整条
	// 编排逻辑，而不必依赖真实 MySQL——与 reporter/logship 把 publisher 定义成
	// 接口、供测试注入假实现是同一个理由，这里只是把"整段会话"当作可替换单元，
	// 不是把连接细节拆成接口（会话内部要用到的 mysqlx/replication 具体类型很难
	// 也没必要抽象成接口）。
	runSessionFn func(ctx context.Context, startGTID string) error

	// sessionCtx/sessionCancel 覆盖本模块整个生命周期；OnDestroy 时取消，
	// 用于唤醒正阻塞在 GetEvent 里的会话 goroutine（GetEvent 支持 ctx 取消，
	// 已通过 go-mysql 源码核实）。
	sessionCtx    context.Context
	sessionCancel context.CancelFunc

	// batchSeq 在源端会话推进，供事件循环读取进度镜像。
	batchSeq atomic.Uint64

	// pipelineState 在事件循环更新，供源端会话读取生命周期状态。
	pipelineState atomic.Int32

	columnFilterUnwired   bool
	originResolverUnwired bool

	schemaTracker *schema.Tracker
	// Owned by the serial session workflow after initialization.
	schemaHistoryLatestID string

	// limiter 是增量投递限流器（internal/node/pipeline/binlogin/ratelimit.go），New() 里总是构造出一个非 nil 实例
	// （两路都未配置时它是一次廉价空操作）。只由会话 goroutine 调用、不加锁，
	// 并发依据与上面 schemaTracker 完全相同（写者唯一：会话严格串行），详见
	// ratelimit.go"并发归属"一节。
	limiter *rateLimiter

	epoch        uint64
	epochCeiling uint64

	// 以下字段只在事件循环（ChanRPC handler / Timer handler）里读写，不需要加锁
	readGTIDSet      string
	publishedGTIDSet string
	lastError        string
	lastEventTsMs    int64
	backoffAttempt   int
	stopping         bool

	stats               statsCounters
	telemetryInstanceID string

	initErr error
}

// New 构造 binlogin.Module。
func New(opts Options) *Module {
	if opts.MaxRowsPerChunk <= 0 {
		opts.MaxRowsPerChunk = defaultMaxRowsPerChunk
	}
	if opts.HeartbeatPeriod <= 0 {
		opts.HeartbeatPeriod = defaultHeartbeatPeriod
	}
	if opts.CheckpointInterval <= 0 {
		opts.CheckpointInterval = defaultCheckpointInterval
	}
	if opts.ReconnectInitialBackoff <= 0 {
		opts.ReconnectInitialBackoff = defaultReconnectInitialBackoff
	}
	if opts.ReconnectMaxBackoff <= 0 {
		opts.ReconnectMaxBackoff = defaultReconnectMaxBackoff
	}
	if opts.ReconnectMultiplier <= 1 {
		opts.ReconnectMultiplier = defaultReconnectMultiplier
	}
	if opts.SendTimeout <= 0 {
		opts.SendTimeout = defaultSendTimeout
	}
	if opts.TableFilter == nil {
		opts.TableFilter = allowAllTableFilter{}
	}
	columnFilterUnwired := opts.ColumnFilter == nil
	if opts.ColumnFilter == nil {
		opts.ColumnFilter = allowAllColumnFilter{}
	}
	opts.DDLColumnPolicy = iproto.CloneDDLColumnPolicy(opts.DDLColumnPolicy)
	opts.PublicationWindow = iproto.ClonePublicationWindow(opts.PublicationWindow)
	originResolverUnwired := opts.OriginResolver == nil
	if opts.OriginResolver == nil {
		opts.OriginResolver = localOriginResolver{center: opts.Center}
	}

	m := &Module{
		opts:                  opts,
		runtimeUpdates:        make(chan runtimeUpdate, 1),
		streamReady:           make(chan struct{}),
		telemetryInstanceID:   uuidx.New().String(),
		now:                   opts.nowFunc(),
		columnFilterUnwired:   columnFilterUnwired,
		originResolverUnwired: originResolverUnwired,
		schemaTracker:         schema.NewTracker(opts.SourceCaseSensitive),
	}
	m.sourceRuntime = iproto.SourceRuntimeConfig{ConfigVersion: opts.ConfigVersion, TableFilter: opts.TableFilter, ColumnFilter: opts.ColumnFilter, RowFilter: opts.RowFilter, Transforms: opts.Transforms, DDLColumnPolicy: iproto.CloneDDLColumnPolicy(opts.DDLColumnPolicy), OriginResolver: opts.OriginResolver, OriginFilter: opts.OriginFilter, TargetCenter: opts.TargetCenter, DatabaseRoute: opts.DatabaseRoute, MaxOriginHops: opts.MaxOriginHops, DropUnknownOrigin: opts.DropUnknownOrigin, RowsPerSec: opts.IncrementalRowsPerSec, BytesPerSec: opts.IncrementalBytesPerSec, PublicationWindow: iproto.ClonePublicationWindow(opts.PublicationWindow), ShadowTablePolicy: iproto.CloneShadowTablePolicy(opts.ShadowTablePolicy)}
	m.runtimeVersion.Store(opts.ConfigVersion)
	m.runSessionFn = m.runSession
	m.limiter = newRateLimiter(opts.IncrementalRowsPerSec, opts.IncrementalBytesPerSec,
		m.now, opts.sleepFunc(), func(d time.Duration) { m.stats.rateLimitWaitNs.Add(int64(d)) })
	m.Skeleton = xhive.NewSkeleton(ModuleName(opts.PipelineID),
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

func (m *Module) Priority() uint { return 50 }

// InitErr 语义与 store/reporter/logship 的 InitErr 完全一致：xhive.Run/
// AddDynamicModules 对模块初始化失败没有把具体原因传回调用方的通用手段
// （AddDynamicModules 的 AddDynamicModuleResult.Err 已经覆盖这一点，这里保留字段
// 是为了与既有模块约定一致，也方便测试直接断言）。
func (m *Module) InitErr() error { return m.initErr }

// PipelineState 返回本实例对自身 pipeline 运行状态的最新认知（原子读取，可从任意
// goroutine 安全调用，见 pipelineState 字段的说明）。生产环境下的权威状态仍由
// Manager 侧依据 reporter 上报的 PipelineStatus 判定，这里只是节点自身的本地视图，
// 主要供测试与运维观测使用。
func (m *Module) PipelineState() iproto.PipelineState {
	return iproto.PipelineState(m.pipelineState.Load())
}

// Stats 返回 binlog_in 的运行统计快照。
type Stats struct {
	TxnsEmitted    int64
	RowsEmitted    int64
	DDLEmitted     int64
	BatchesSent    int64
	WatermarksSent int64
	Reconnects     int64
	PurgeDetected  int64
	TablesFiltered int64 // 因 TableFilter 拒绝而跳过解码的 RowsEvent 计数
	// RateLimitWait 是增量限流累计等待时长。它是"限流正在生效"在运行期唯一的
	// 数值证据：只看投递速率无法区分"被限流器压住了"与"源库本来就慢"，而这两者
	// 的处置完全相反（前者调配置、后者查源库）。
	RateLimitWait time.Duration
}

func (m *Module) Stats() Stats {
	return Stats{
		TxnsEmitted:    m.stats.txnsEmitted.Load(),
		RowsEmitted:    m.stats.rowsEmitted.Load(),
		DDLEmitted:     m.stats.ddlEmitted.Load(),
		BatchesSent:    m.stats.batchesSent.Load(),
		WatermarksSent: m.stats.watermarksSent.Load(),
		Reconnects:     m.stats.reconnects.Load(),
		PurgeDetected:  m.stats.purgeDetected.Load(),
		TablesFiltered: m.stats.tablesFiltered.Load(),
		RateLimitWait:  time.Duration(m.stats.rateLimitWaitNs.Load()),
	}
}

// OnInit 校验依赖、恢复检查点、注册 ChanRPC 处理器与定时器，并发起第一次会话。
func (m *Module) OnInit() error {
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	if m.columnFilterUnwired {
		slog.Warn("columnfilter not injected",
			slog.String("pipeline_id", m.opts.PipelineID))
	}
	if m.originResolverUnwired {
		slog.Warn("originresolver not injected",
			slog.String("pipeline_id", m.opts.PipelineID))
	}

	// 限流一旦生效，投递速率就不再反映源库的实际产出速率——排障时"延迟在涨"这个
	// 现象的第一解释会变成限流本身。把它在启动时说一次，让运维不必先去翻配置文档
	// 才知道这条 pipeline 被主动压着（运行期的数值证据是 Stats.RateLimitWait）。
	if m.limiter.enabled() {
		slog.Info("incremental rate limit enabled",
			slog.String("pipeline_id", m.opts.PipelineID),
			slog.Uint64("rows_per_sec", m.opts.IncrementalRowsPerSec),
			slog.Uint64("bytes_per_sec", m.opts.IncrementalBytesPerSec))
	}

	startGTID, err := m.resolveStartGTIDSet()
	if err != nil {
		m.initErr = err
		return err
	}

	if err := m.loadSchemaHistory(startGTID); err != nil {
		m.initErr = err
		return err
	}
	if m.schemaTracker.Len() == 0 {
		slog.Warn("no local schema history held",
			slog.String("pipeline_id", m.opts.PipelineID))
	}
	// read 与 published 在会话开始（=恢复点）这一刻必然相等：这正是"恢复点"的
	// 定义本身——resolveStartGTIDSet 现在读的是上次持久化的 published_gtid_set
	// （见该函数文档），从这里开始，两者只会在本次会话运行期间因为
	// dispatchOne 的"先 read 后 published"顺序而分裂，不会在 OnInit 这一刻就
	// 不等。
	m.readGTIDSet = startGTID
	m.publishedGTIDSet = startGTID

	epoch, err := m.resolveEpoch(startGTID)
	if err != nil {
		m.initErr = err
		return err
	}
	m.epoch = epoch

	m.selfClient = chanrpc.NewClient()

	if err := m.RegisterChanRPC(progressMsg{}, m.onProgress); err != nil {
		wrapped := fmt.Errorf("binlogin: 注册 ChanRPC 处理器失败: %w", err)
		m.initErr = wrapped
		m.selfClient.Close()
		return wrapped
	}
	if err := m.RegisterChanRPC(iproto.SourceRuntimeUpdate{}, m.onRuntimeUpdate); err != nil {
		m.selfClient.Close()
		m.initErr = err
		return err
	}

	m.sessionCtx, m.sessionCancel = context.WithCancel(context.Background())

	m.RegisterTimer(timerKindCheckpoint, func(_ int64, _ map[string]string) {
		m.persistCheckpoint()
		m.reportTelemetry()
	})
	m.NewTimer(timerKindCheckpoint, m.opts.CheckpointInterval, timer.WithTicker())

	// 重连定时器：一次性（不带 WithTicker），每次触发只发起一次新会话；
	// backoff 的下一次延迟由 handleSessionEnded 在下次失败时重新计算并重新注册。
	m.RegisterTimer(timerKindReconnect, func(_ int64, _ map[string]string) {
		m.scheduleSession()
	})

	initialState := iproto.PipelineState_PIPELINE_STATE_RUNNING
	if m.opts.Snapshotting {
		initialState = iproto.PipelineState_PIPELINE_STATE_SNAPSHOTTING
	}
	m.setPipelineState(initialState, "")
	m.scheduleSession()

	return nil
}

// WaitReady waits for the first event from a live MySQL dump, rather than
// treating StartSyncGTID/OnInit as server acknowledgement. It is called only by
// the loader background workflow before source assembly is acknowledged and
// before snapshot reads are allowed to start.
func (m *Module) WaitReady(ctx context.Context) error {
	select {
	case <-m.streamReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.sessionCtx.Done():
		return m.sessionCtx.Err()
	}
}

// CancelStream is a lifecycle signal; context cancellation is safe across
// goroutines and leaves all mutable pipeline state owned by the event loop.
func (m *Module) CancelStream() { m.sessionCancel() }

// StreamDone is an immutable lifecycle channel, used to stop the snapshot if
// the companion stream encounters a fatal GTID error.
func (m *Module) StreamDone() <-chan struct{} { return m.sessionCtx.Done() }

// SnapshotFailure reads only the guard's synchronized background workflow state.
func (m *Module) SnapshotFailure() error {
	if m.opts.SnapshotDDLGuard == nil {
		return nil
	}
	return m.opts.SnapshotDDLGuard.Failure()
}

// Serve cancels IO as soon as the framework stops the event loop. OnDestroy is
// invoked after Serve exits, which is too late to unblock an active dump during
// rollback of a failed snapshot startup.
func (m *Module) Serve(ctx context.Context) {
	stop := context.AfterFunc(ctx, m.sessionCancel)
	defer stop()
	defer m.sessionCancel()
	m.Skeleton.Serve(ctx)
	m.sessionCancel()
	m.streamWorkers.Wait()
	m.closePublicationWindow()
	m.rejectPendingRuntimeUpdates()
}

// OnDestroy 通知会话 goroutine 停止（取消 sessionCtx 唤醒阻塞的 GetEvent），
// 并做最后一次尽力而为的检查点落盘。
func (m *Module) OnDestroy() {
	m.stopping = true
	if m.sessionCancel != nil {
		m.sessionCancel()
	}
	m.persistCheckpoint()
}

// Close 释放本模块持有的出站资源：selfClient（自投递用）与 Skeleton 的异步任务队列。
// Sink/Store 都是外部注入的共享句柄，生命周期由调用方管理，本包不关闭它们
// （"句柄注入"）。
func (m *Module) Close() error {
	if m.selfClient != nil {
		m.selfClient.Close()
	}
	return m.Skeleton.Close()
}
