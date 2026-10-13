package amqpin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xhive"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive/timer"
	"google.golang.org/protobuf/proto"
)

const (
	defaultMaxReassemblyChunks = 500

	// defaultMaxApplyAttempts/defaultApplyInitialBackoff/defaultApplyMaxBackoff/
	// defaultApplyMultiplier 控制单个完整 DataBatch 调 ApplyBatch 失败后、
	// IsRetryable=true 时的节点内部退避重试节奏（NODE-MQI-04"重试超限"要求
	// 这个上限必须显式存在）。
	defaultMaxApplyAttempts    = 5
	defaultApplyInitialBackoff = 500 * time.Millisecond
	defaultApplyMaxBackoff     = 30 * time.Second
	defaultApplyMultiplier     = 2.0
	defaultApplyAttemptTimeout = 30 * time.Second
	defaultConsumerNamePrefix  = "amqp_in."
)

// Options 是构造 Module 所需的运行参数。
type Options struct {
	AllowPlanExtensions bool
	WritePolicy         *iproto.WritePolicy
	ErrorPolicy         iproto.TargetErrorPolicy
	InstanceID          string
	ConfigVersion       uint64
	Budget              *resourcequota.Budget
	PublicationWindow   *iproto.IncrementalWindowPolicy
	// PipelineID 决定消费的队列名（transport.PipelineDataQueue）与 ModuleName。
	PipelineID    string
	Center        string
	MaxOriginHops uint32
	DatabaseRoute iproto.DatabaseLineageRoute

	// RunID binds accepted batches, watermarks, checkpoints and status to one
	// initialization run. The production controller passes CommonConfig.RunId;
	// run 0 is the initial run, and only REINIT allocates a larger identity.
	RunID uint64

	// Conn 是消费者连接（"句柄注入"：本包不拥有、不关闭这个连接，
	// 生命周期由 cmd/node 管理，与 amqpout.Options.Publisher 同一约定）。
	Conn *transport.Conn

	// Applier 是应用完整 DataBatch 的对象，通常是 *mysqlout.Writer，必填。
	Applier iApplier

	// MaxReassemblyChunks 是大事务分片重组的显式上限（doc.go"prefetch 与重组
	// 上限"一节），Consumer 的 QOSPrefetch 直接取这个值。默认 500。
	MaxReassemblyChunks int

	// MaxApplyAttempts/ApplyInitialBackoff/ApplyMaxBackoff/ApplyMultiplier 控制
	// ApplyBatch 失败且 IsRetryable=true 时的节点内部重试节奏，默认 5 次、
	// 500ms/30s/2.0。
	MaxApplyAttempts    int
	ApplyInitialBackoff time.Duration
	ApplyMaxBackoff     time.Duration
	ApplyMultiplier     float64
	// ApplyAttemptTimeout 是单次 ApplyBatch 调用的超时保护，默认 30s。
	ApplyAttemptTimeout time.Duration

	// ConsumerName 设置服务端消费者标识名，便于在管理控制台里识别；为空时默认
	// "amqp_in.<pipeline_id>"。
	ConsumerName string

	// ShutdownTimeout 映射到 xhive 模块级关闭超时选项，同时是 Close 时等待
	// Consumer 排空在途 handler 调用的超时。
	ShutdownTimeout time.Duration

	Store iStoreAccess

	// SnapshotReady is a one-shot immutable completion value. When set, no
	// incremental consumer exists before the target has applied every chunk.
	SnapshotReady       <-chan *iproto.SnapshotEnd
	RecoveryRequests    <-chan iproto.SnapshotRecoveryRequest
	RecoveryCompletions <-chan iproto.SnapshotRecoveryCompletion
	// FullOnly bounds SQL and consumed watermarks to the stored H. Completion
	// stops input locally before Manager's durable STOP reaches both roles.
	FullOnly bool
	// AfterCatchup is cold-phase work, serialized before the consumer can
	// advance beyond H. It receives the bounded shutdown-aware context.
	AfterCatchup func(context.Context) error

	// sleep 覆盖退避等待的实现，默认基于 time.After；测试注入快速/可观测的假
	// 实现，与 amqpout.Options.sleep 同一模式。
	sleep func(ctx context.Context, d time.Duration) error

	// now 供测试注入固定时钟；nil 时使用 time.Now（"可测试逻辑
	// 通过参数接收 now"，persistCheckpoint 的 updated_at_ms 字段是这个入口的
	// 唯一使用者，与 store.Options.now/binlogin 同一模式）。
	now func() time.Time
}

func (o Options) sleepFunc() func(ctx context.Context, d time.Duration) error {
	if o.sleep != nil {
		return o.sleep
	}
	return ctxSleep
}

func (o Options) nowFunc() func() time.Time {
	if o.now != nil {
		return o.now
	}
	return time.Now
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
	if o.FullOnly && o.SnapshotReady == nil {
		errs = append(errs, errors.New("amqpin: full-only requires snapshot completion"))
	}
	if strings.TrimSpace(o.PipelineID) == "" {
		errs = append(errs, errors.New("amqpin: PipelineID 不能为空"))
	}
	if o.Conn == nil {
		errs = append(errs, errors.New("amqpin: Conn 不能为空"))
	}
	if o.Applier == nil {
		errs = append(errs, errors.New("amqpin: Applier 不能为空"))
	}
	if err := iproto.ValidateTargetErrorPolicy(o.ErrorPolicy); err != nil {
		errs = append(errs, err)
	}
	_, asyncStore := o.Store.(iCriticalAsyncStore)
	if o.ErrorPolicy.SkipUnretryable && !asyncStore {
		errs = append(errs, errors.New("amqpin: skipping errors requires durable store"))
	}
	limits, err := iproto.NormalizePublicationWindow(o.PublicationWindow)
	if err != nil {
		errs = append(errs, err)
	}
	if limits.Batches > o.MaxReassemblyChunks {
		errs = append(errs, errors.New("amqpin: publication window exceeds consumer prefetch"))
	}
	if limits.Batches > 1 && !asyncStore {
		errs = append(errs, errors.New("amqpin: publication window requires durable store"))
	}
	return errors.Join(errs...)
}

// ModuleName 按 pipeline_id 拼出该 pipeline amqp_in 实例在 xhive 动态模块注册表
// 里的名字，与 binlogin.ModuleName/amqpout.ModuleName 同一模式。
func ModuleName(pipelineID string) string {
	return "amqp_in." + pipelineID
}

// Module 是 amqp_in 动态模块，每条 pipeline 目标端一个实例。
type Module struct {
	*xhive.Skeleton

	opts  Options
	sleep func(ctx context.Context, d time.Duration) error
	now   func() time.Time

	consumer           *transport.Consumer
	input              chan ownedInput
	predecodeSlots     chan struct{} // immutable capacity handle, shared only as channel values
	inputDone          chan struct{}
	inputStarted       bool // startup owner; joined before Close
	coalescing         bool // input coordinator owns the complete group
	pendingReplay      *pendingReplay
	publicationWindows map[publicationKey]*publicationGeneration

	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc

	// Consumer callbacks are serial. Runtime updates inspect this state only
	// while handlerMu grants exclusive ownership and handlerActive is false.
	reassembly *reassemblyState
	// Consumer callbacks and Close share only this lifecycle guard. Pipeline
	// state is cleaned by the last callback, or by Close after all callbacks.
	handlerMu      sync.Mutex
	handlerActive  bool
	handlerClosing bool
	heldHandles    []iAckHandle
	// Execution ownership is protected by handlerMu; callbacks own the mutable
	// batch state while active, otherwise only the runtime update handler uses it.
	runtimeReady             bool
	runtimePending           *targetRuntimeUpdate
	runtimeMaxOriginHops     uint32
	runtimeDatabaseRoute     iproto.DatabaseLineageRoute
	runtimeErrorPolicy       iproto.TargetErrorPolicy
	runtimeWritePolicy       *iproto.WritePolicy
	runtimePublicationWindow *iproto.IncrementalWindowPolicy
	skippedSet               gmysql.GTIDSet
	runtimeVersion           atomic.Uint64

	// The input coordinator owns appliedSet, lastEpoch and lastBatchSeq.
	// Committed transactions advance appliedSet even when later transactions fail;
	// seen continuity advances per accepted fragment, independently of SQL application.
	appliedSet   gmysql.GTIDSet
	consumedSet  gmysql.GTIDSet
	rejectedSet  gmysql.GTIDSet
	lastEpoch    uint64
	lastBatchSeq uint64

	lastPersistedAt time.Time

	// continuityBaselineSet is process-local. Persisted checkpoints may lag accepted
	// deliveries and must not seed continuity after a consumer restart.
	continuityBaselineSet bool

	frozenReason atomic.Pointer[string]

	stats statsCounters

	initErr          error
	startupDone      chan struct{}
	catchupHigh      gmysql.GTIDSet
	recoveryRequest  *iproto.SnapshotRecoveryRequest
	recoveryWaiting  bool
	recoverySequence uint64
	recoveryPending  bool
	recoveryTables   []*iproto.SnapshotRecoveryTable
	fullOnlyHigh     gmysql.GTIDSet
	fullOnlyDone     bool
	// Owned by startup, then transferred to the single consumer handler.
	catchupTotalChunks       uint64
	catchupInitialTotal      uint64
	catchupExtensionSequence uint64
	telemetryInstanceID      string
	telemetryMirror          atomic.Pointer[iproto.PipelineTelemetry]
	// Owned by startup, then the serial consumer handler.
	telemetryRows     uint64
	telemetryTxns     uint64
	telemetrySourceTS int64
	telemetryLagMS    float64
}

// New 构造 amqpin.Module。
func New(opts Options) *Module {
	opts.PublicationWindow = iproto.ClonePublicationWindow(opts.PublicationWindow)
	if opts.WritePolicy != nil {
		opts.WritePolicy = proto.Clone(opts.WritePolicy).(*iproto.WritePolicy)
	}
	if opts.MaxReassemblyChunks <= 0 {
		opts.MaxReassemblyChunks = defaultMaxReassemblyChunks
	}
	if opts.MaxApplyAttempts <= 0 {
		opts.MaxApplyAttempts = defaultMaxApplyAttempts
	}
	if opts.ApplyInitialBackoff <= 0 {
		opts.ApplyInitialBackoff = defaultApplyInitialBackoff
	}
	if opts.ApplyMaxBackoff <= 0 {
		opts.ApplyMaxBackoff = defaultApplyMaxBackoff
	}
	if opts.ApplyMultiplier <= 1 {
		opts.ApplyMultiplier = defaultApplyMultiplier
	}
	if opts.ApplyAttemptTimeout <= 0 {
		opts.ApplyAttemptTimeout = defaultApplyAttemptTimeout
	}
	if strings.TrimSpace(opts.ConsumerName) == "" {
		opts.ConsumerName = defaultConsumerNamePrefix + opts.PipelineID
	}

	m := &Module{
		opts:                     opts,
		runtimeMaxOriginHops:     opts.MaxOriginHops,
		runtimeDatabaseRoute:     opts.DatabaseRoute,
		runtimeErrorPolicy:       opts.ErrorPolicy,
		runtimeWritePolicy:       opts.WritePolicy,
		runtimePublicationWindow: opts.PublicationWindow,
		input:                    make(chan ownedInput, opts.MaxReassemblyChunks),
		inputDone:                make(chan struct{}),
		predecodeSlots:           make(chan struct{}, 8),
		telemetryInstanceID:      uuidx.New().String(),
		sleep:                    opts.sleepFunc(),
		now:                      opts.nowFunc(),

		appliedSet:  emptyGTIDSet(),
		consumedSet: emptyGTIDSet(),
		rejectedSet: emptyGTIDSet(),
		skippedSet:  emptyGTIDSet(),
	}
	m.runtimeVersion.Store(opts.ConfigVersion)
	m.Skeleton = xhive.NewSkeleton(ModuleName(opts.PipelineID),
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

func (m *Module) Priority() uint { return 50 }

// InitErr 语义与 store/reporter/logship/binlogin/amqpout 的 InitErr 完全一致。
func (m *Module) InitErr() error { return m.initErr }

// OnInit 建立到 pl.<pipeline_id> 队列的消费者并立即开始消费。队列本身不由本包
// 声明（transport.NewConsumer 内部使用 QueueNoDeclare，拓扑由 Declarator 统一
// 声明，见 transport 包文档），本包假定调用方已经完成声明与绑定。
func (m *Module) OnInit() error {
	if err := iproto.ValidateWritePolicy(m.opts.WritePolicy); err != nil {
		m.initErr = err
		return err
	}
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	m.shutdownCtx, m.shutdownCancel = context.WithCancel(context.Background())

	m.loadCheckpoint()
	if err := m.loadSkippedTransactions(); err != nil {
		m.initErr = err
		m.shutdownCancel()
		return err
	}
	m.captureTelemetry()
	if err := m.RegisterChanRPC(iproto.TargetRuntimeUpdate{}, m.onRuntimeUpdate); err != nil {
		m.initErr = err
		m.shutdownCancel()
		return err
	}
	m.RegisterTimer("amqpin.runtime_update", m.onRuntimeUpdateTimer)
	m.NewTimer("amqpin.runtime_update", 100*time.Millisecond, timer.WithTicker())
	m.RegisterTimer("amqpin.telemetry", func(_ int64, _ map[string]string) { m.reportTelemetry() })
	m.NewTimer("amqpin.telemetry", time.Second, timer.WithTicker())
	if m.opts.SnapshotReady != nil {
		m.reportStatus(iproto.PipelineState_PIPELINE_STATE_SNAPSHOTTING, "")
		return nil
	}
	return m.startConsumer()
}

func (m *Module) startConsumer() error {
	state := iproto.PipelineState_PIPELINE_STATE_RUNNING
	if m.catchupHigh != nil {
		state = iproto.PipelineState_PIPELINE_STATE_CATCHING_UP
	}

	queue := transport.PipelineDataQueue(m.opts.PipelineID)
	c, err := transport.NewConsumer(m.opts.Conn, queue,
		// NODE-MQI-01：增量队列并发度必须为 1，保证全局有序。
		transport.WithConcurrency(1),
		// 见 doc.go"prefetch 与重组上限"：必须能容纳一次重组需要同时在途的
		// 全部分片，否则重组会死锁。
		transport.WithPrefetch(m.opts.MaxReassemblyChunks),
		transport.WithConsumerName(m.opts.ConsumerName),
	)
	if err != nil {
		wrapped := fmt.Errorf("amqpin: 创建消费者失败（queue=%s）: %w", queue, err)
		m.initErr = wrapped
		return wrapped
	}
	m.consumer = c
	m.reportStatus(state, "")
	m.handlerMu.Lock()
	m.runtimeReady = true
	m.handlerMu.Unlock()
	if !m.inputStarted {
		m.inputStarted = true
		go m.runInput()
	}

	if err := c.Run(m.enqueueInput); err != nil {
		m.shutdownCancel()
		wrapped := fmt.Errorf("amqpin: 启动消费失败（queue=%s）: %w", queue, err)
		m.initErr = wrapped
		return wrapped
	}
	// Status was emitted before Run transfers state to the handler goroutine;
	// a fast first batch may immediately report catchup completion.
	return nil
}

// Serve waits in a background workflow; the actor never blocks on the gate or
// broker startup. Closing the module first cancels startup/retries and joins
// this workflow, so Close cannot race with consumer construction.
func (m *Module) Serve(ctx context.Context) {
	stop := context.AfterFunc(ctx, m.shutdownCancel)
	defer stop()
	if m.opts.SnapshotReady != nil {
		m.startupDone = make(chan struct{})
		go func() {
			defer close(m.startupDone)
			select {
			case end := <-m.opts.SnapshotReady:
				if !m.initializeCatchup(end) {
					return
				}
				if m.isFrozen() || m.fullOnlyDone || m.shutdownCtx.Err() != nil {
					return
				}
				if err := m.startConsumer(); err != nil {
					m.freeze("全量完成后启动增量消费失败")
				}
			case <-m.shutdownCtx.Done():
			}
		}()
	}
	m.Skeleton.Serve(ctx)
	m.shutdownCancel()
	if m.startupDone != nil {
		<-m.startupDone
	}
	if m.inputStarted {
		<-m.inputDone
	}
}

func (m *Module) initializeCatchup(end *iproto.SnapshotEnd) bool {
	if !m.beginRuntimeWork() {
		return false
	}
	defer m.endRuntimeWork()
	if err := m.beginCatchup(end); err != nil {
		m.freeze("全量完成信息无效，增量消费未启动")
		return false
	}
	if m.shutdownCtx.Err() != nil {
		return false
	}
	// Release cold-phase ownership before Run can dispatch a callback.
	m.maybeFinishCatchup()
	return true
}

// OnDestroy 通知任何仍在进行的重试退避尽快结束。
func (m *Module) OnDestroy() {
	if m.shutdownCancel != nil {
		m.shutdownCancel()
	}
}

// 为什么这里调用 persistCheckpoint 是安全的，即使调用 Close 的不是 handler
// goroutine：transport.Consumer.Close 文档"默认等待正在处理中的消息完成"，
// 正常路径下走到这一行时 handler goroutine 已经结束，不存在并发访问；
// CloseWithTimeout 在超时时会强制关闭，理论上 handler goroutine 仍可能没退出
// 干净——但 persistCheckpoint 只读 m.appliedGTIDSetString()/m.stats.lastEpoch/
// m.stats.lastBatchSeq 这几个 atomic 快照（不读 appliedSet/lastEpoch/
// lastBatchSeq 本身），因此即使真的撞上这个边界情形，读到的至多是"差一点点没来
// 得及发布的旧值"，不是数据竞争。
func (m *Module) Close() error {
	if m.shutdownCancel != nil {
		m.shutdownCancel()
	}
	m.handlerMu.Lock()
	m.handlerClosing = true
	m.finishRuntimeUpdateLocked()
	if !m.handlerActive {
		m.discardReassembly()
		m.heldHandles = nil
	}
	m.handlerMu.Unlock()
	if m.consumer != nil {
		if m.opts.ShutdownTimeout > 0 {
			m.consumer.CloseWithTimeout(m.opts.ShutdownTimeout)
		} else {
			m.consumer.Close()
		}
	}
	if m.inputStarted {
		<-m.inputDone
		m.releaseQueuedInput()
	}
	m.discardPublicationBuffers()
	m.persistCheckpoint()
	return m.Skeleton.Close()
}

// Stats 返回 amqp_in 的运行统计快照。
type Stats struct {
	BatchesApplied    int64 // 成功调用 ApplyBatch 并 Ack 的完整 DataBatch 数（含重组后的）
	PermanentFailures int64 // 结构性永久损坏、被持有的消息数
	ApplyFailures     int64 // ApplyBatch 重试耗尽或不可重试、被持有的批次数
	AckFailures       int64 // Ack 调用本身失败的次数（ApplyBatch 成功后、或 NODE-MQI-02 判定为重复后，GTID 幂等/内容已处理过，均安全）
	WatermarksAcked   int64
	ProbesAcked       int64
	ChunksReassembled int64 // 已完成重组、成功走到 applyAndFinish 的分片总数（不含冻结丢弃的）
	// DuplicatesAcked 是 NODE-MQI-02 连续性检查（internal/node/pipeline/amqpin/continuity.go）判定为"重复"、
	// 直接 Ack 掉（不进入分片/应用流程）的消息数。
	DuplicatesAcked int64
	// SupersededAcked counts valid queued messages belonging to an older run.
	SupersededAcked int64
	Frozen          bool
	FrozenReason    string

	AppliedGTIDSet string
	LastEpoch      uint64
	LastBatchSeq   uint64
}

// appliedGTIDSetString 返回当前 Stats() 快照里的 applied_gtid_set 字符串，nil
// 快照（从未 recordApplied 过）返回空字符串。
func (m *Module) appliedGTIDSetString() string {
	p := m.stats.appliedGTIDSet.Load()
	if p == nil {
		return ""
	}
	return *p
}

func (m *Module) Stats() Stats {
	reason := m.frozenReason.Load()
	s := Stats{
		BatchesApplied:    m.stats.batchesApplied.Load(),
		PermanentFailures: m.stats.permanentFailures.Load(),
		ApplyFailures:     m.stats.applyFailures.Load(),
		AckFailures:       m.stats.ackFailures.Load(),
		WatermarksAcked:   m.stats.watermarksAcked.Load(),
		ProbesAcked:       m.stats.probesAcked.Load(),
		ChunksReassembled: m.stats.chunksReassembled.Load(),
		DuplicatesAcked:   m.stats.duplicatesAcked.Load(),
		SupersededAcked:   m.stats.supersededAcked.Load(),
		Frozen:            reason != nil,
		AppliedGTIDSet:    m.appliedGTIDSetString(),
		LastEpoch:         m.stats.lastEpoch.Load(),
		LastBatchSeq:      m.stats.lastBatchSeq.Load(),
	}
	if reason != nil {
		s.FrozenReason = *reason
	}
	return s
}

// isFrozen 判定当前是否处于冻结状态。
func (m *Module) isFrozen() bool {
	return m.frozenReason.Load() != nil
}

// freeze 进入冻结状态：只在第一次调用时记录原因并打印 ERROR 日志，之后的调用
// （理论上不会发生，见 internal/node/pipeline/amqpin/handle.go 的调用点）不重复记录。
func (m *Module) freeze(reason string) {
	full := fmt.Sprintf("amqpin: pipeline_id=%s 冻结: %s", m.opts.PipelineID, reason)
	if m.frozenReason.CompareAndSwap(nil, &full) {
		logFreeze(m.opts.PipelineID, reason)
		// 与 internal/node/pipeline/binlogin/session.go 里会话出错时那处 setPipelineState(ERROR,...) 同一
		// 模式：状态变化即时上报（NODE-RPT-02），只在真正第一次冻结（CAS 成功）
		// 时发一次，不是每次 freeze() 被调用都发。
		m.reportStatus(iproto.PipelineState_PIPELINE_STATE_ERROR, reason)
		// 必须放在 CAS 赢了的这个分支内部，不能放在 CAS 判断之前：冻结是一次性
		// 状态变迁，告警也必须是一次性的——如果放外面，一个已冻结的 pipeline
		// 被（理论上不会发生，但也不该依赖这个假设）重复调用 freeze() 命中时，
		// 会对着同一次冻结反复发事件。
		m.reportEvent("AMQP_IN_FROZEN", reason)
	}
}
