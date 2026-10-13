package probe

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/xmapst/xhive"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
)

const (
	// DefaultHTTPTimeout 是单次 Management API HTTP 调用的超时（硬约束 3）。
	DefaultHTTPTimeout = 8 * time.Second

	// DefaultMySQLConnectTimeout 是单次 CONN_TEST 连接目标 MySQL（含拨号、鉴权与
	// 一次查询）的超时（硬约束 3），量级与 internal/node/pipeline/mysqlout 的
	// connectTimeout 一致——同一类"连接一个 MySQL 实例"操作，没有理由取不同的值。
	DefaultMySQLConnectTimeout = 10 * time.Second

	// DefaultRequestTimeout 是处理一次 ProbeRequest 的整体预算（含多次 HTTP 调用），
	// 比单次 HTTP 超时更宽，避免恰好卡在两次调用之间被过早取消。
	//
	// 本常量是节点从收到请求到回执 publish confirm 完成的总时长上界，不只是"干活"
	// 那部分——internal/node/probe/handler.go 里 `context.WithTimeout(context.Background(), m.opts.RequestTimeout)` 建的 ctx 同时罩住 verifyAndDispatch（干活）与
	// m.reply（发回执），发回执那一步的 DefaultPublishTimeout 是从这个 ctx 派生
	// 出来的上限，不是额外叠加的预算，见 DefaultPublishTimeout 的说明，不要把两个
	// 常量相加当成节点侧总预算。
	DefaultRequestTimeout = 20 * time.Second

	// DefaultPublishTimeout 是发布 ProbeResult 回执等待 Broker confirm 的超时，
	// 与 reporter/logship 的同名默认值一致。
	DefaultPublishTimeout = 5 * time.Second
)

// iPublisher 是 probe 发布回执所需的最小接口，*transport.Publisher 天然实现它。
// 与 reporter/logship 各自定义、不共享同一个类型的理由相同：三个模块的发布调用
// 互相独立，不应该通过共享一个内部接口类型制造隐性耦合。
type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

// Options 是构造 Module 所需的运行参数。
type Options struct {
	// Center/NodeID 用于信封 headers；Token 用于派生上行签名密钥（同 reporter/logship）。
	Center string
	NodeID string
	Token  string
	// ProtocolVersion 写入 x-proto-ver；0 时使用 iproto.CurrentProtocolVersion()。
	ProtocolVersion uint32

	// Publisher 是发布 ProbeResult 回执的对象，通常与 reporter/logship 共用同一个
	// *transport.Publisher 实例（Publisher 内部用 mutex 保护并发状态、支持多个调用方
	// 共享，已在 reporter/logship 接入时确认）。
	Publisher iPublisher
	// Conn is shared with the production pipeline loader. Only LINK uses it.
	Conn  *transport.Conn
	Store store.IReader

	// Vhost 是 Management API 请求路径里的 vhost 段，由调用方从自己已持有的 AMQP
	// 连接串解析得到，不是写死的常量，
	// 也不来自 ProbeRequest（ManagementAPIConn 没有 vhost 字段）。
	Vhost string

	BrokerHost string

	// SignatureWindow 限定控制请求的鉴权时间窗，零值使用协议默认值。
	SignatureWindow time.Duration

	// HTTPTimeout 是单次 Management API 调用的超时。<=0 时使用 DefaultHTTPTimeout。
	HTTPTimeout time.Duration
	// MySQLConnectTimeout 是单次 CONN_TEST 连接目标 MySQL 的超时。<=0 时使用
	// DefaultMySQLConnectTimeout。
	MySQLConnectTimeout time.Duration
	// RequestTimeout 是处理一次 ProbeRequest 的整体超时。<=0 时使用
	// DefaultRequestTimeout。POSITION 使用独立的 65s 扫描与回执预算。
	RequestTimeout time.Duration
	// PublishTimeout 是发布 ProbeResult 回执等待 Broker confirm 的超时。<=0 时使用
	// DefaultPublishTimeout。
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
		errs = append(errs, errors.New("probe: Center 不能为空"))
	}
	if strings.TrimSpace(o.NodeID) == "" {
		errs = append(errs, errors.New("probe: NodeID 不能为空"))
	}
	if strings.TrimSpace(o.Token) == "" {
		errs = append(errs, errors.New("probe: Token 不能为空"))
	}
	if o.Publisher == nil {
		errs = append(errs, errors.New("probe: Publisher 不能为空"))
	}
	if strings.TrimSpace(o.Vhost) == "" {
		errs = append(errs, errors.New("probe: Vhost 不能为空"))
	}
	return errors.Join(errs...)
}

// statsCounters 用原子量存放跨 goroutine 的统计计数：onEnvelopeMsg 把
// handleEnvelope 挪到 Skeleton.AsyncGo 的独立 goroutine 执行（见 internal/node/probe/handler.go 文档，
// 保留移交前"多个 ProbeRequest 并发处理、一次较慢的 Management API 调用不阻塞其它
// 探测/指令"这条属性），加上 Stats() 可能被事件循环之外的地方（如 reporter 心跳
// 采样）读取，与 logship.statsCounters 同一类判断依据。
type statsCounters struct {
	handled atomic.Int64
	failed  atomic.Int64
}

// Module 是 probe 静态模块。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time

	signKey iproto.SignKey

	httpClient *http.Client

	stats statsCounters

	initErr error
}

// New 构造 probe.Module。
func New(opts Options) *Module {
	if opts.HTTPTimeout <= 0 {
		opts.HTTPTimeout = DefaultHTTPTimeout
	}
	if opts.MySQLConnectTimeout <= 0 {
		opts.MySQLConnectTimeout = DefaultMySQLConnectTimeout
	}
	if opts.RequestTimeout <= 0 {
		opts.RequestTimeout = DefaultRequestTimeout
	}
	if opts.PublishTimeout <= 0 {
		opts.PublishTimeout = DefaultPublishTimeout
	}
	if opts.ProtocolVersion == 0 {
		opts.ProtocolVersion = iproto.CurrentProtocolVersion()
	}

	m := &Module{
		opts:    opts,
		now:     opts.nowFunc(),
		signKey: iproto.DeriveSignKeyFromToken(opts.Token),
		httpClient: &http.Client{
			Timeout: opts.HTTPTimeout,
		},
	}
	m.Skeleton = xhive.NewSkeleton(ModuleName,
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	)
	return m
}

// Priority 返回 30，在 store、身份和 Controller 初始化后启动探测。
func (m *Module) Priority() uint { return 30 }

// InitErr 语义与 store/reporter/logship 的 InitErr 完全一致：xhive.Run 对模块初始化
// 失败没有返回值，cmd/node 必须靠这个字段判断退出码。
func (m *Module) InitErr() error { return m.initErr }

// Stats 是 probe 的处理统计快照。
type Stats struct {
	Handled int64 // 已送达回执（success 字段无论真假，只要回执发布成功）的任务数
	Failed  int64 // 连回执都无法送达（解析不出 x-reply-key、或发布回执本身失败）的任务数
}

func (m *Module) Stats() Stats {
	return Stats{
		Handled: m.stats.handled.Load(),
		Failed:  m.stats.failed.Load(),
	}
}

// OnInit 只注册 EnvelopeMsg 的 ChanRPC 处理器。本包不再持有 AMQP 消费者——控制队列
// （transport.NodeControlQueue）由 internal/node/controller 独占消费、按
// Properties.Type 分派，probe.request 类型通过 AsyncCall 转交本包（见 doc.go"与
// controller 的边界"一节、internal/node/controller/handler.go）。
func (m *Module) OnInit() error {
	if err := m.opts.validate(); err != nil {
		m.initErr = err
		return err
	}

	if err := m.RegisterChanRPC(EnvelopeMsg{}, m.onEnvelopeMsg); err != nil {
		wrapped := fmt.Errorf("probe: 注册 ChanRPC 处理器失败: %w", err)
		m.initErr = wrapped
		return wrapped
	}
	return nil
}

// OnDestroy 无需额外收尾：probe 不持有需要在事件循环里落地的内存状态——每个
// ProbeRequest 的处理都是一次性的，成功或失败都已经在 handleEnvelope 里回执完毕。
func (m *Module) OnDestroy() {}

func (m *Module) Close() error {
	return m.Skeleton.Close()
}
