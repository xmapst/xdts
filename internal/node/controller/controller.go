package controller

import (
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/timer"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// Controller 是 Node 的静态配置和指令模块，每个节点一个实例。
// 运行中的数据模块按流水线和角色动态装配。
const ModuleName = "controller"

const (
	// DefaultPublishTimeout 是发布 ConfigApplyResult 等待 Broker confirm 的超时，
	// 与 reporter/logship/probe 的同名默认值一致。
	DefaultPublishTimeout = 5 * time.Second

	// DefaultMySQLConnectTimeout 是 convertStartPosition 按 Kind=LATEST 连接源库
	// 查询 @@GLOBAL.gtid_executed（单次拨号 + 鉴权 + 一次查询）的超时，与
	// probe.DefaultMySQLConnectTimeout/pipeline/mysqlout 的 connectTimeout 同一
	// 量级——都是"连接一个 MySQL 实例做一次轻量操作"，没有理由取不同的值（见
	// internal/node/probe/probe.go 同名常量文档）。
	//
	// 不用于 Kind=BINLOG_POS：那条路径的预算是 DefaultBinlogPosResolveTimeout，
	// 理由见该常量文档——两者都叫"连接源库"，但后面做的事完全不是同一量级。
	DefaultMySQLConnectTimeout = 10 * time.Second

	// DefaultBinlogPosResolveTimeout 是 convertStartPosition 按 Kind=BINLOG_POS
	// 换算位点（position.ResolveFileOffset）的超时。
	//
	// 不能复用 DefaultMySQLConnectTimeout：那个常量的文档明确把自己限定在"单次
	// 拨号 + 鉴权 + 一次查询"，而 ResolveFileOffset 是打开一条真实的伪从库探测
	// 连接、从目标 binlog 文件开头逐个事件 GetEvent 扫描直到找到 source_offset
	// 对应的事务边界（internal/node/position/position.go 同名函数文档）——耗时与"文件里从开头到目标偏移
	// 之间有多少事件"成正比，不是一个固定量级的操作，把它按"连接源库做一次轻量
	// 操作"的预算收紧是文不对题。
	DefaultBinlogPosResolveTimeout = 60 * time.Second

	DefaultControlQueueTTL = 24 * time.Hour

	// defaultStartupPullTimeout 是启动期全量拉取的整体预算（含 RPCClient 内部的多次
	// 重试）。RPCClient 默认单次尝试 5s、最多 5 次、指数退避到 3s 封顶，最坏情况
	// 累计接近 5*5+ (200ms+400ms+800ms+1.6s)≈28s，给到 30s 留一点余量而不是刚好
	// 卡着算出来的下限。
	defaultStartupPullTimeout = 30 * time.Second

	defaultMirrorPushInterval = 2 * time.Second
	// defaultTargetedPullTimeout 是收到 config.changed 后定向拉取的整体预算，与
	// 启动期全量拉取同一量级——都是一次 RPCClient.Call，请求体大小不是耗时的主要
	// 来源（真正的耗时来自联邦绑定传播延迟与重试退避，两种拉取面对的是同一个
	// Manager、同一条链路）。
	defaultTargetedPullTimeout = 30 * time.Second

	timerStartup = "controller.startup"

	timerMirror = "controller.mirror"
)

// Options 是构造 Module 所需的运行参数。
type Options struct {
	// Center/NodeID 用于信封 headers 与日志定位；Token 用于派生上行签名密钥（同
	// reporter/logship/probe/amqpout 的既定模式，New() 会派生并缓存 signKey，不
	// 在每次发布/调用时重新计算）。
	Center string
	NodeID string
	Token  string
	// ProtocolVersion 写入 x-proto-ver；0 时使用 iproto.CurrentProtocolVersion()。
	ProtocolVersion uint32

	// Conn 是到本中心 Broker 的共享连接，用于构造 transport.RPCClient（config.pull）
	// 与 internal/node/controller/loader.go 声明目标端数据队列（本包不单独开连接，复用 cmd/node 已建立的连接，
	// 与 probe/reporter/logship 同一条既定约定）。
	Conn *transport.Conn
	// Publisher 是发布 ConfigApplyResult 的对象，通常与 reporter/logship/probe 共用
	// 同一个 *transport.Publisher 实例。
	Publisher iPublisher
	// Store 用于持久化/恢复"已应用配置"快照（NODE-CFG-04）。与 binlogin/amqpin/
	// reporter 的 Store 字段不同：本包要求它必须非空——没有 Store，NODE-CFG-04
	// 字面意义上的"按本地配置恢复运行"根本无法成立，不是"退化到只在内存"这种可以
	// 接受的过渡态（那几个包的检查点丢了只影响可观测性连续性，本包的快照丢了意味着
	// 这条能力整个不存在）。
	Store iStoreAccess
	// Resources is the validated Node-wide runtime quota, independent of Manager
	// assignment quotas. An omitted value uses defaults for existing constructors.
	Resources resourcequota.Limits

	// InstanceID 是本节点 RPCClient 实例的标识，决定回复队列名
	// xdts.reply.<instanceID>（"instanceID 由发起 RPC 的一方生成"）。
	// 为空时 New() 生成一个新的 uuidx.New().String()。
	InstanceID string

	PublishTimeout        time.Duration
	ShutdownTimeout       time.Duration
	RequestRetirementStop func()
	// MySQLConnectTimeout 是 convertStartPosition 按 Kind=LATEST 连接源库解析起始
	// 位点的超时。<=0 时使用 DefaultMySQLConnectTimeout。
	MySQLConnectTimeout time.Duration
	// BinlogPosResolveTimeout 是 convertStartPosition 按 Kind=BINLOG_POS 换算位点
	// 的超时，<=0 时使用 DefaultBinlogPosResolveTimeout；与 MySQLConnectTimeout
	// 分开配置的理由见该默认值常量文档。
	BinlogPosResolveTimeout time.Duration

	loader iPipelineLoader
	now    func() time.Time

	// statusReporter provides independently owned status notifications and test injection.
	statusReporter func(status *iproto.PipelineStatus)
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
		errs = append(errs, errors.New("controller: Center 不能为空"))
	}
	if strings.TrimSpace(o.NodeID) == "" {
		errs = append(errs, errors.New("controller: NodeID 不能为空"))
	}
	if strings.TrimSpace(o.Token) == "" {
		errs = append(errs, errors.New("controller: Token 不能为空"))
	}
	if o.Conn == nil {
		errs = append(errs, errors.New("controller: Conn 不能为空"))
	}
	if o.Publisher == nil {
		errs = append(errs, errors.New("controller: Publisher 不能为空"))
	}
	if o.Store == nil {
		errs = append(errs, errors.New("controller: Store 不能为空（NODE-CFG-04 要求本地持久化，不是可选项）"))
	}
	return errors.Join(errs...)
}

// Module 是 controller 静态模块。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time

	signKey iproto.SignKey

	rpcClient *transport.RPCClient
	loader    iPipelineLoader

	statusReporter func(status *iproto.PipelineStatus)

	// mirrorReporter 是承载清单镜像的出口（internal/node/controller/ctl_mirror.go），默认由 New() 绑定到
	// prodReportPipelinesMirror；测试注入假实现，理由同 statusReporter。
	mirrorReporter func(entries []*iproto.PipelineHeartbeatEntry)

	mirrorReady bool

	// consumer 是本节点控制队列（transport.NodeControlQueue）的唯一消费者，本包
	// 从 probe 手里接过的所有权。
	consumer *transport.Consumer

	// applied is the actor-owned authority for successfully applied role configs.
	// Workers return independent results before this map is updated or persisted.
	applied map[appliedKey]appliedPipeline
	// priorRunApplied is startup-only history read before config assembly.
	// It supplies resume identities and starting positions; it does not itself prove
	// that any modules are running in this process.
	priorRunApplied      map[appliedKey]appliedPipeline
	deletedRoleVersions  map[appliedKey]uint64
	deletingRoles        map[appliedKey]bool
	startupConfigChanges map[string]*iproto.ConfigChanged
	// pending marks pipeline work awaiting actor completion; keep later requests queued.
	pending map[appliedKey]bool
	// pendingPull 记录正在为"本地完全没有这条 pipeline 的配置"发起定向 config.pull
	// 的 pipeline_id（internal/node/controller/cmd_pipeline.go execStartResume）。它按 pipeline_id、不按
	// appliedKey，这不是偷懒：那一刻节点还不知道自己会持有哪些角色（配置正是要
	// 拉的东西），构造不出 role。
	pendingPull map[string]bool
	// trailingItem retains the latest skipped configuration for replay after pending work.
	trailingItem map[appliedKey]*iproto.PipelineConfigItem

	// 三类结局与是否写入的对应关系（完整推导见 internal/node/controller/cmd_pipeline.go markCmdResolved、
	// resolveCmdAck、publishCmdResultThenAck 文档）：
	//   - 上次成功                → 写入（下次同键短路 ack，正确：意图已经达成）
	//   - 上次失败、判定不可重试   → 写入（重试不会变成支持，短路 ack 正确）
	//   - 上次失败、判定可重试     → 不写入（下次同键必须落到执行分支重新
	//     尝试，不能被短路）
	cmdPipelineResolved   map[cmdDedupSlot]cmdPipelineKey
	runtimeInstances      map[appliedKey]iproto.PipelineRuntimeStatus
	runtimeUpdateVersions map[appliedKey]uint64
	runtimeFaults         map[appliedKey]bool
	recoveryWatcher       *transport.RecoveryWatcher
	recoveryEvents        <-chan struct{}
	configRepullPending   bool
	configRepullBusy      bool
	configRepullAttempt   int
	configRepullAfter     time.Time

	initErr                 error
	identityChecking        bool
	identityRetired         bool
	retirementCommitted     bool
	retirementStopRequested bool
}

// New 构造 controller.Module。
func New(opts Options) *Module {
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.MySQLConnectTimeout <= 0 {
		opts.MySQLConnectTimeout = DefaultMySQLConnectTimeout
	}
	if opts.BinlogPosResolveTimeout <= 0 {
		opts.BinlogPosResolveTimeout = DefaultBinlogPosResolveTimeout
	}
	if opts.ProtocolVersion == 0 {
		opts.ProtocolVersion = iproto.CurrentProtocolVersion()
	}
	if opts.InstanceID == "" {
		opts.InstanceID = uuidx.New().String()
	}

	m := &Module{
		opts:                  opts,
		now:                   opts.nowFunc(),
		signKey:               iproto.DeriveSignKeyFromToken(opts.Token),
		applied:               make(map[appliedKey]appliedPipeline),
		deletedRoleVersions:   make(map[appliedKey]uint64),
		deletingRoles:         make(map[appliedKey]bool),
		startupConfigChanges:  make(map[string]*iproto.ConfigChanged),
		pending:               make(map[appliedKey]bool),
		pendingPull:           make(map[string]bool),
		trailingItem:          make(map[appliedKey]*iproto.PipelineConfigItem),
		cmdPipelineResolved:   make(map[cmdDedupSlot]cmdPipelineKey),
		runtimeInstances:      make(map[appliedKey]iproto.PipelineRuntimeStatus),
		runtimeUpdateVersions: make(map[appliedKey]uint64),
		runtimeFaults:         make(map[appliedKey]bool),
		loader:                opts.loader,
		statusReporter:        opts.statusReporter,
	}
	if m.loader == nil {
		limits := opts.Resources
		if limits == (resourcequota.Limits{}) {
			limits = resourcequota.DefaultLimits()
		}
		quota, err := resourcequota.New(limits)
		if err != nil {
			m.initErr = err
		}
		m.loader = newProdLoader(nodeContext{
			Quota:           quota,
			Center:          opts.Center,
			NodeID:          opts.NodeID,
			Token:           opts.Token,
			ProtocolVersion: opts.ProtocolVersion,
			Conn:            opts.Conn,
			Publisher:       opts.Publisher,
			Store:           opts.Store,
			SourceRecovery:  m.requestSourceRecovery,
		})
	}
	m.Skeleton = xhive.NewSkeleton(ModuleName,
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	if m.statusReporter == nil {
		// 必须在 m.Skeleton 赋值之后构造：prodReportPipelineStatus 内部调用
		// m.Cast，依赖 m.Skeleton 非空（Cast 是 *xhive.Skeleton 提升上来的方法）。
		m.statusReporter = prodReportPipelineStatus(m)
	}
	if m.mirrorReporter == nil {
		// 同上：prodReportPipelinesMirror 也走 m.Cast。
		m.mirrorReporter = prodReportPipelinesMirror(m)
	}
	return m
}

// Priority 返回 10，确保 store 和身份先就绪，后续探测及数据面再接入。
func (m *Module) Priority() uint { return 10 }

// InitErr 语义与 store/reporter/logship/probe/binlogin/amqpout 的 InitErr 完全一致：
// xhive.Run 对模块初始化失败没有返回值，cmd/node 必须靠这个字段判断退出码。
func (m *Module) InitErr() error { return m.initErr }

// OnInit 只做不依赖其它模块、不做跨模块协作的构造工作（校验 Options、建立
// RPCClient、建立并启动控制队列消费者——三者都只需要调用方已经建立好的共享 Conn，
// 不等待任何其它模块），并注册一个零延迟一次性 Timer 承接真正依赖跨模块协作的
// 启动逻辑。
//
// 为什么不在这里直接拉取配置：NODE-CFG-01 的启动拉取依赖 Store（跨模块 ChanRPC
// 请求，Manager 不可达时的兜底路径要用到）与网络 RPC（config.pull 本身）。AGENTS.md
// "OnInit 阶段不要假定其它模块已经可调用...依赖持久化或跨模块请求的冷启动工作
// 放到零延迟定时器或明确的启动完成阶段"正是本场景的字面描述——controller 是静态
// 模块，OnInit 运行在 xhive 按 Priority 顺序初始化全部静态模块的阶段，不能照搬
// binlogin.iStoreAccess 文档里"动态模块 OnInit 时全部静态模块必然已经 OnInit *和*
// Serve 完成"这条不变量（那条不变量的前提是 AddDynamicModules 要求 AppStateRun，
// 只对动态模块成立），静态模块之间没有等价的顺序保证。用 RegisterTimer+NewTimer(…,0)
// 把这部分工作挪到 Serve 已经在跑之后执行，是 AGENTS.md 明确建议的做法，不是本包
// 自创。
//
// ⇒ 把两个 Priority 调成相反，或让本包先关，这条链立刻变成"ack 打在已关闭的通道上"
// ——与 cmd.pipeline 那条（见 Close 文档）同一个形状。
func (m *Module) OnInit() (initErr error) {
	if m.initErr != nil {
		return m.initErr
	}
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	rc, err := transport.NewRPCClient(m.opts.Conn, m.opts.InstanceID)
	if err != nil {
		wrapped := fmt.Errorf("controller: 创建 RPC 客户端失败: %w", err)
		m.initErr = wrapped
		return wrapped
	}
	m.rpcClient = rc
	w, err := m.opts.Conn.WatchRecovery()
	if err != nil {
		m.rpcClient.Close()
		return fmt.Errorf("controller: create connection recovery observer: %w", err)
	}
	m.recoveryWatcher, m.recoveryEvents = w, w.Events()
	defer func() {
		if initErr != nil {
			w.Close()
			rc.Close()
		}
	}()

	queue := transport.NodeControlQueue(m.opts.NodeID)
	c, err := transport.NewConsumer(m.opts.Conn, queue,
		// 分派本身不做阻塞 IO：config.changed 交给 onConfigChangedMessage（内部用
		// AsyncGo 把真正的网络拉取挪到独立 goroutine），probe.request 只是
		// AsyncCall（非阻塞投递，入队失败与否才需要同步返回值，见 internal/node/controller/handler.go）。
		// 不需要 probe 曾经的并发度 4（那是为了不让一次较慢的 Management API
		// 调用挡住其它探测/指令，本包这层分派逻辑不存在这个问题），单一消费者
		// goroutine 已经足够，也更简单。
		transport.WithConcurrency(1),
		transport.WithConsumerName(ModuleName),
	)
	if err != nil {
		wrapped := fmt.Errorf("controller: 创建控制队列消费者失败（queue=%s）: %w", queue, err)
		m.initErr = wrapped
		return wrapped
	}
	m.consumer = c
	if err := c.Run(m.onDelivery); err != nil {
		wrapped := fmt.Errorf("controller: 启动控制队列消费失败（queue=%s）: %w", queue, err)
		m.initErr = wrapped
		return wrapped
	}
	slog.Info("controller consumer started", slog.String("queue", queue))

	m.RegisterTimer(timerStartup, m.onStartupTimer)
	if err := m.RegisterChanRPC(iproto.PipelineRuntimeStatus{}, m.onRuntimeStatus); err != nil {
		return err
	}
	if err := m.RegisterChanRPC(stateExportMessage{}, m.onStateExportReq); err != nil {
		return err
	}
	m.NewTimer(timerStartup, 0)
	m.RegisterTimer(timerConfigRecovery, m.onConfigRecoveryTimer)
	m.NewTimer(timerConfigRecovery, 100*time.Millisecond, timer.WithTicker())

	m.RegisterTimer(timerMirror, m.onMirrorTimer)
	m.NewTimer(timerMirror, defaultMirrorPushInterval, timer.WithTicker())
	m.RegisterTimer(timerIdentity, m.onIdentityTimer)
	m.NewTimer(timerIdentity, 5*time.Second, timer.WithTicker())

	return nil
}

// OnDestroy 关闭 RPCClient（释放它持有的回复队列消费者与专属 Publisher）。已装配
// 的 pipeline 动态模块不在这里处理——它们各自是独立的 xhive 动态模块，由框架按
// LIFO 顺序在 controller 之后单独走自己的 OnDestroy/Close（controller 只是曾经调用
// xhive.AddDynamicModules 装配过它们，不是它们的生命周期持有者，与 xhive
// "静态模块不因为不是直接调用方就不需要独立走关闭流程"的既定语义一致）。
func (m *Module) OnDestroy() {
	if m.recoveryWatcher != nil {
		m.recoveryWatcher.Close()
	}
	if m.rpcClient != nil {
		m.rpcClient.Close()
	}
}

// Close cancels and joins background assembly before releasing process resources.
// Do not block the actor while workers still need it to complete asynchronous store calls.
func (m *Module) Close() error {
	if m.consumer != nil {
		if m.opts.ShutdownTimeout > 0 {
			m.consumer.CloseWithTimeout(m.opts.ShutdownTimeout)
		} else {
			m.consumer.Close()
		}
	}
	return m.Skeleton.Close()
}
