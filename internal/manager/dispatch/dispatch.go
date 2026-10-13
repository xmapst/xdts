// Package dispatch coordinates configuration delivery and pipeline commands.
// Persisted Manager state determines desired configuration, command sequence and
// retry eligibility. RPC handlers verify node identity, signature and freshness
// before applying replies or registration results.
//
// Reconciliation preserves command identities across retries. Node migration
// exports signed role-scoped state before changing ownership and fences old epochs.
// Control messages report errors before acknowledgement; data-plane ACK semantics
// are owned by Node consumers.
package dispatch

import (
	"context"
	"sync/atomic"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// IPipelineDispatcher 是 internal/manager/http 依赖的最小能力集合——与
// internal/manager/http/server.go Options.Audit（IAuditRecorder）同一种"接口 + 生产实现 +
// 测试用 spy"的既有模式，命名以 I 开头（接口即可替换的
// 依赖）。
type IPipelineDispatcher interface {
	// SyncPipeline 检查一条现存 pipeline 当前应下发的配置摘要是否与节点
	// 已确认应用的摘要不同；不同则递增 config_version、记录新的下发记录
	// 并推送 config.changed。调用方在自己的写入成功之后调用，不需要、
	// 也不应该自己判断"是不是真的变了"。
	SyncPipeline(ctx context.Context, pipelineID uuidx.UUID) error

	NotifyDeleted(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, sourceNodeID, targetNodeID uuidx.UUID) error
}

// IPipelineNodeMigrator is the optional extension used by the node migration
// endpoint. Keeping it separate preserves lightweight test dispatchers while
// allowing the production dispatcher to create the new-node release and notify
// both endpoints after the repository transaction commits.
type IPipelineNodeMigrator interface {
	NotifyNodeMigration(ctx context.Context, pipelineID uuidx.UUID, configVersion uint64, side model.Side, oldNodeID, newNodeID uuidx.UUID) error
}

// iPublisher 是本包发布控制面消息（config.changed 推送、config.pull/
// register.request 的回复）所需的最小接口，*transport.Publisher 天然
// 实现它。与 internal/node/controller.iPublisher（probe/logship/amqpout
// 各包同理）同一种做法：方法签名完全相同，但各包各自独立定义、不共享
// 同一个内部接口类型——发布调用彼此独立，共享类型只会制造隐性耦合（见
// controller.iPublisher 的同类注释）。引入这层接口纯粹是为了让
// sync_test.go/handler 相关测试能注入一个不需要真实 AMQP 连接的假实现：
// *transport.Publisher 只能通过 transport.NewPublisher(conn, ...) 构造，
// 必须有一个真实连通的 *transport.Conn，包外无法在单元测试里拼出一个
// "发布必定成功"或"发布返回特定 Outcome"的实例。
type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

var _ iPublisher = (*transport.Publisher)(nil)

// Dispatcher 是 IPipelineDispatcher 唯一的生产实现，同时承载
// handler.go/ctl_*.go 里控制面 RPC 消息处理器的公共依赖（db、发布者、
// 时钟）。
type Dispatcher struct {
	db *gorm.DB

	// pub 用 atomic.Pointer 热插入：AMQP 在后台协程里持续重试直到连通
	// （cmd/manager/main.go connectAMQPWithRetry 既有设计），Dispatcher
	// 必须在 AMQP 接好之前就能被构造、被 http handler 持有——写者只有
	// `cmd/manager/main.go` 里那一个后台协程，读者是各次
	// SyncPipeline/NotifyDeleted/
	// onConfigPullReq/onRegisterReq 调用，与 amqpBoot 的注释是同一个理由。
	//
	// 存的是 *iPublisher（指向接口值的指针），不是 *transport.Publisher：
	// atomic.Pointer[T] 要求 T 是一个确定类型，iPublisher 本身是接口类型，
	// 这里的 T 就是 iPublisher——Load 拿到的是 *iPublisher，解一次引用得到
	// 接口值。SetPublisher 对外仍然只接受具体的 *transport.Publisher（生产
	// 调用方只有 cmd/manager/main.go 一处，不需要放开成接口），包内测试
	// 可以绕过 SetPublisher 直接给这个字段 Store 一个指向假 iPublisher 的
	// 指针（白盒测试，同包可访问未导出字段）。
	pub atomic.Pointer[iPublisher]
	rpc atomic.Pointer[transport.RPCClient]

	// now 可选注入，默认 time.Now；供测试用固定时钟——与
	// internal/node/probe.Module 的既有做法一致。
	now func() time.Time

	// tokenKey 解析节点入站控制面消息的验签 key，默认按 nodeID 查
	// Node.TokenHash 现算（见 internal/manager/dispatch/verify.go defaultTokenKeyFunc），与
	// internal/manager/ingest.TokenKeyFunc 同一种"可插拔，供单测注入固定
	// key、不依赖真实数据库"的做法——两个包各自持有一份同形状的类型，不
	// 互相 import：ingest 与 dispatch 是两条独立的控制面/上报面消费线，
	// 没有理由让其中一个仅为复用一个函数类型别名就依赖另一个。
	tokenKey TokenKeyFunc
}

var _ IPipelineDispatcher = (*Dispatcher)(nil)

// Options 是构造 Dispatcher 的参数。
type Options struct {
	DB *gorm.DB
	// Now 可选，未提供时使用 time.Now。
	Now func() time.Time
	// TokenKey 可选，未提供时使用 defaultTokenKeyFunc（按 nodeID 查
	// Node.TokenHash 现算，见 internal/manager/dispatch/verify.go）。
	TokenKey TokenKeyFunc
}

// New 构造 Dispatcher。发布者留空（未接线状态），需要在 AMQP 接线成功后
// 调用 SetPublisher——与 reclaim.Loop/task.InspectionLoop 的既有模式
// 一致：先以"未配置发布能力"的状态可用，AMQP 接好后热插入真正实现，不
// 需要重启依赖它的模块。
func New(opts Options) *Dispatcher {
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	tokenKey := opts.TokenKey
	if tokenKey == nil {
		tokenKey = defaultTokenKeyFunc(opts.DB)
	}
	return &Dispatcher{db: opts.DB, now: now, tokenKey: tokenKey}
}

func (d *Dispatcher) SetPublisher(pub *transport.Publisher) {
	if pub == nil {
		// 显式传 nil 时不能直接 `var p iPublisher = pub` 再取址存进去——
		// pub 是带具体类型的 nil（*transport.Publisher(nil)），装进接口
		// 变量后是一个"类型非空、值为 nil"的接口，publisher() 的
		// `pub == nil` 判断会认为"已经接好了"，随后调用它的方法会 panic。
		// 直接存一个类型化的 nil 指针，Load 出来的 *iPublisher 本身是
		// nil，publisher() 能正确识别成"未接线"。
		d.pub.Store(nil)
		return
	}
	var p iPublisher = pub
	d.pub.Store(&p)
}

// Bootstrap supplies the established reply consumer before export is usable.
func (d *Dispatcher) SetRPCClient(client *transport.RPCClient) { d.rpc.Store(client) }

func (d *Dispatcher) publisher() iPublisher {
	p := d.pub.Load()
	if p == nil {
		return nil
	}
	return *p
}
