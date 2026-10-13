// Package ingest consumes Node reports and logs for Manager.
// Messages are verified before updating persisted runtime, progress, events and
// observations. Current node, run and configuration identities fence stale reports.
//
// Each delivery is acknowledged once after processing; malformed control reports
// are logged rather than requeued indefinitely. Periodic workers own their tasks
// and stop before the shared database and transport resources are closed.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	defaultHeartbeatInterval    = 5 * time.Second // NODE-RPT-01 的默认心跳周期。
	defaultOfflineFactor        = 3               // MGR-NODE-05："默认 3 个心跳周期"。
	defaultOfflineCheckInterval = 5 * time.Second

	// defaultStatsCheckInterval 是失败计数器增量检测的默认周期（见
	// internal/manager/ingest/ctl_stats.go）：要在"分区耗尽导致 InsertLogs/InsertPipelineMetrics
	// 开始报错"这类场景下尽快被看见，取一个比离线巡检更短的默认值。
	defaultStatsCheckInterval = 30 * time.Second
	// defaultStatsSummaryInterval 是周期性汇总心跳日志的默认周期，用于
	// 区分"零失败"和"根本没在跑"，不需要很频繁。
	defaultStatsSummaryInterval = 5 * time.Minute

	// 限制后台巡检的数据库调用时间，超时交由对应错误分支处理。
	checkDBTimeout = 15 * time.Second
)

// iConsumer 是本包消费队列所需的最小接口，*transport.Consumer 天然实现它。
// 定义为接口是为了让单元测试注入假实现直接驱动 handler，不需要依赖真实
// Broker（真实 Broker 的接线验证留给 cmd/manager/main.go 实际接上之后）。
type iConsumer interface {
	Run(handler transport.Handler) error
	CloseWithTimeout(timeout time.Duration)
}

// Options 是构造 Service 所需的运行参数。
type Options struct {
	DB *gorm.DB

	// ReportConsumer/LogConsumer 由调用方用 internal/transport.NewConsumer
	// 连接到 xdts.manager.report / xdts.manager.log 构造好之后注入（设计
	//"句柄注入"，与 internal/node/reporter.Options.Publisher 同一
	// 模式）。拓扑（交换机/队列/绑定）声明不属于本包职责，见包文档"接线
	// 说明"。
	ReportConsumer iConsumer
	LogConsumer    iConsumer

	// TokenKey 解析验签用的 SignKey；为空时 New() 用
	// defaultTokenKeyFunc（internal/manager/ingest/verify.go，按 Node.TokenHash 派生，不带缓存）
	// 补上默认实现。
	TokenKey TokenKeyFunc

	// SignatureWindow 传给 iproto.Verify；<=0 时使用
	// iproto.DefaultSignatureWindow（MQ-10 默认 ±5 分钟）。
	SignatureWindow time.Duration

	// OfflineTimeout 是心跳超时判定离线的时限（MGR-NODE-05 默认 3 个心跳
	// 周期）；<=0 时取 defaultOfflineFactor * HeartbeatInterval。
	OfflineTimeout time.Duration
	// HeartbeatInterval 仅用于在 OfflineTimeout 未显式设置时换算默认值；
	// <=0 时取 defaultHeartbeatInterval。
	HeartbeatInterval time.Duration
	// OfflineCheckInterval 是离线判定巡检的执行周期；<=0 时取
	// defaultOfflineCheckInterval。
	OfflineCheckInterval time.Duration

	// StatsCheckInterval 是 internal/manager/ingest/ctl_stats.go 检查失败计数器增量的执行周期；
	// <=0 时取 defaultStatsCheckInterval。
	StatsCheckInterval time.Duration
	// StatsSummaryInterval 是 internal/manager/ingest/ctl_stats.go 打周期性汇总心跳日志的周期
	// （即使计数器没有变化也打）；<=0 时取 defaultStatsSummaryInterval。
	StatsSummaryInterval time.Duration

	// 以下仓储依赖以函数字段注入：internal/manager/repo 是包级函数而不是
	// 方法上挂的接口，函数字段是让单元测试不连真实数据库、直接注入假函数
	// 校验本包自己的解码/分发/错误处理逻辑的最小成本方式（repo 各函数自身
	// 的正确性已经有 internal/manager/repo 自己的测试覆盖，不需要本包
	// 重复验证）。为空时使用 internal/manager/repo 的真实实现。
	InsertLogs          func(ctx context.Context, db *gorm.DB, rows []model.Log) error
	InsertEvents        func(ctx context.Context, db *gorm.DB, rows []model.Event) error
	RecordNodeHeartbeat func(ctx context.Context, db *gorm.DB, id uuidx.UUID, now time.Time, runtime model.JSON) error
	MarkOfflineNodes    func(ctx context.Context, db *gorm.DB, now time.Time, timeout time.Duration) ([]model.Node, error)

	SetPipelineRoleActualState func(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side, state model.ActualState, lastError string) error

	MarkPipelineRoleUnknown func(ctx context.Context, db *gorm.DB, id uuidx.UUID, side model.Side) error
	// RecordPipelineProgress stores a complete observation and its durable
	// downsampling cursor within the same transaction as snapshot progress.
	RecordPipelineProgress func(ctx context.Context, db *gorm.DB, record model.PipelineProgressRecord) error

	ListPipelinesByNode func(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID) ([]model.Pipeline, error)

	// now 供测试注入固定时钟；为空时使用 time.Now。
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
	if o.DB == nil {
		errs = append(errs, errors.New("ingest: DB 不能为空"))
	}
	if o.ReportConsumer == nil {
		errs = append(errs, errors.New("ingest: ReportConsumer 不能为空"))
	}
	if o.LogConsumer == nil {
		errs = append(errs, errors.New("ingest: LogConsumer 不能为空"))
	}
	// TokenKey 允许为空——New() 会补一个基于 Node.TokenHash 派生的默认实现
	// （见 internal/manager/ingest/verify.go defaultTokenKeyFunc），不像 DB/Consumer 那样是硬性必填项。
	return errors.Join(errs...)
}

// Service 是 Manager 侧上报消费服务：消费 report/log 两个队列 + 周期性
// 离线判定。不是 xhive 模块——internal/manager 当前没有引入 xhive（只有
// internal/node 侧使用），本包按 Manager 现有约定（如
// internal/manager/http.Server 的 Start()/Shutdown()）走普通的生命周期。
type Service struct {
	opts Options
	now  func() time.Time

	stats stats

	stopOffline chan struct{}
	offlineDone chan struct{}

	stopStats chan struct{}
	statsDone chan struct{}

	lastReportFailed int64
	lastLogFailed    int64

	lastReportAckUnconfirmed int64
	lastLogAckUnconfirmed    int64
}

// New 构造 Service。opts 校验失败（缺少必填依赖）时返回 error，不返回一个
// "看起来能用但实际会跳过安全校验"的实例。
func New(opts Options) (*Service, error) {
	if err := opts.validate(); err != nil {
		return nil, err
	}

	if opts.SignatureWindow <= 0 {
		opts.SignatureWindow = iproto.DefaultSignatureWindow
	}
	if opts.HeartbeatInterval <= 0 {
		opts.HeartbeatInterval = defaultHeartbeatInterval
	}
	if opts.OfflineTimeout <= 0 {
		opts.OfflineTimeout = defaultOfflineFactor * opts.HeartbeatInterval
	}
	if opts.OfflineCheckInterval <= 0 {
		opts.OfflineCheckInterval = defaultOfflineCheckInterval
	}
	if opts.StatsCheckInterval <= 0 {
		opts.StatsCheckInterval = defaultStatsCheckInterval
	}
	if opts.StatsSummaryInterval <= 0 {
		opts.StatsSummaryInterval = defaultStatsSummaryInterval
	}
	if opts.InsertLogs == nil {
		opts.InsertLogs = repo.InsertLogs
	}
	if opts.InsertEvents == nil {
		opts.InsertEvents = repo.InsertEvents
	}
	if opts.RecordNodeHeartbeat == nil {
		opts.RecordNodeHeartbeat = repo.RecordNodeHeartbeat
	}
	if opts.MarkOfflineNodes == nil {
		opts.MarkOfflineNodes = repo.MarkOfflineNodes
	}
	if opts.SetPipelineRoleActualState == nil {
		opts.SetPipelineRoleActualState = repo.SetPipelineRoleActualState
	}
	if opts.MarkPipelineRoleUnknown == nil {
		opts.MarkPipelineRoleUnknown = repo.MarkPipelineRoleUnknown
	}
	if opts.RecordPipelineProgress == nil {
		opts.RecordPipelineProgress = repo.RecordPipelineProgress
	}
	if opts.ListPipelinesByNode == nil {
		opts.ListPipelinesByNode = repo.ListPipelinesByNode
	}
	if opts.TokenKey == nil {
		opts.TokenKey = defaultTokenKeyFunc(opts.DB)
	}

	return &Service{
		opts: opts,
		now:  opts.nowFunc(),
	}, nil
}

// Start 启动 report/log 两个消费循环与离线判定定时任务。
// transport.Consumer.Run 本身不阻塞（首次连接成功后立即返回，重连在后台
// 协程里进行，见 internal/transport 包文档），本方法同样不阻塞。
func (s *Service) Start() error {
	if err := s.opts.ReportConsumer.Run(s.handleReport); err != nil {
		return fmt.Errorf("ingest: 启动 report 消费失败: %w", err)
	}
	if err := s.opts.LogConsumer.Run(s.handleLog); err != nil {
		return fmt.Errorf("ingest: 启动 log 消费失败: %w", err)
	}

	s.stopOffline = make(chan struct{})
	s.offlineDone = make(chan struct{})
	go s.runOfflineLoop()

	s.stopStats = make(chan struct{})
	s.statsDone = make(chan struct{})
	go s.runStatsLoop()

	return nil
}

// Close 停止离线判定循环、失败计数器告警循环，并关闭两个消费者（默认等待
// 在途消息处理完成，最多 timeout；语义与 transport.Consumer.CloseWithTimeout
// 一致）。关闭顺序：先停止本包自己的两个周期任务（它们只读 stats/发事件，
// 不持有需要排空的投递），再关闭两个消费者（等待/超时排空在途投递）。
func (s *Service) Close(timeout time.Duration) {
	stopAndWait(s.stopOffline, s.offlineDone, "offline", timeout)
	stopAndWait(s.stopStats, s.statsDone, "stats", timeout)
	s.opts.ReportConsumer.CloseWithTimeout(timeout)
	s.opts.LogConsumer.CloseWithTimeout(timeout)
}

// 缺陷原状：`Close` 的签名里写着 timeout，而四个等待点里只有下面两个
// `CloseWithTimeout` 用到它；两个 `<-done` 是无条件阻塞接收。巡检循环
// 阻塞在无超时的 DB 调用上时，它的 select 到不了，stop 观测不到，done 永不
// close ⇒ Close 永久挂住，整个 manager 停不下来，而 timeout 参数对这
// 条路径完全无效。
//
// 超时后必须记日志、不能静默跳过：静默跳过会把"停不下来"换成"停下来了
// 但不知道漏了什么"——那个循环仍在跑、仍在写库，而调用方已经认为它停了。
//
// timeout <= 0 表示不等待：与 transport.Consumer.CloseWithTimeout 一致
// （它用 context.WithTimeout(bg, timeout)，非正值得到一个已过期的 ctx）。
func stopAndWait(stop chan struct{}, done <-chan struct{}, loop string, timeout time.Duration) {
	if stop == nil {
		return
	}
	close(stop)
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		slog.Warn("periodic loop did not exit before close timeout",
			slog.String("loop", loop),
			slog.Duration("timeout", timeout))
	}
}
