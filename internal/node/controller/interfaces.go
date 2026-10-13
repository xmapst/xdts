package controller

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
)

// iPublisher 是发布 ConfigApplyResult 回执所需的最小接口，*transport.Publisher 天然
// 实现它。与 probe/reporter/logship/amqpout 各自独立定义同形状接口而不共享同一个
// 内部类型的理由相同：几个模块的发布调用互相独立，共享一个内部接口类型只会制造隐性
// 耦合（见 probe.iPublisher 同类注释）。
type iPublisher interface {
	PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (transport.Outcome, error)
}

// iStoreAccess 提供已应用快照的读写能力，并传给源端和目标端模块。
// Put 用于异步写入，PutCritical 用于必须确认持久化后才能继续的流程。
type iStoreAccess interface {
	Put(op store.Op) error
	Get(ctx context.Context, collection, id string, out any) error
	PutCritical(ctx context.Context, op store.Op) error
}

// iPipelineLoader 是本包构造/卸载一条 pipeline 动态模块所需的最小接口。真实实现见
// internal/node/controller/loader.go 的 prodLoader（内部依赖 xhive.AddDynamicModules/RemoveDynamicModule、
// binlogin.New/amqpout.New/amqpin.New/mysqlout.New 与 internal/rule.Compile 完成真正
// 的装配）；测试注入假实现，验证 internal/node/controller/apply.go 的编排逻辑（何时调用、参数是否正确、失败
// 如何回执/持久化）而不依赖真实 AMQP/MySQL 连接。
//
// 接口按"源端/目标端各一个 Load 方法 + 一个按角色区分的 Unload 方法"划分，而不是
// 按 binlogin/amqpout/amqpin 四个具体模块各开一个方法：源端一次 Load 必须同时装配
// binlog_in 与 amqp_out 两个 xhive 动态模块（binlogin.Options.Sink 要求一个已经构造好
// 的 ISink 实现，即 amqpout.Module），拆成两个独立方法只会让调用方（internal/node/controller/apply.go）需要
// 自己保证"先建 amqp_out 再建 binlog_in、失败时手动回滚已建好的那个"这类装配顺序
// 与部分失败的清理逻辑——这些细节属于"如何正确装配源端"这一件事，应该封装在
// LoadSource 内部，不应该泄漏给调用方。
type iPipelineLoader interface {
	// LoadSource 装配 item 对应 pipeline 源端的动态模块（binlog_in + amqp_out）。
	// item.Role 必须是 PIPELINE_ROLE_SOURCE。
	//
	// startGTIDSet is an explicitly resolved baseline, including the empty set.
	// nil requests checkpoint recovery for an unchanged run and position.
	LoadSource(ctx context.Context, item *iproto.PipelineConfigItem, startGTIDSet *string) error
	// LoadTarget 装配 item 对应 pipeline 目标端的动态模块（amqp_in，内部持有
	// *mysqlout.Writer）。item.Role 必须是 PIPELINE_ROLE_TARGET。会话建立（拨号
	// 目标库）是真实网络 IO，调用方必须在 Skeleton.AsyncGo 之外的 goroutine 里调用
	// 本方法，不能直接在事件循环里同步调用（见 apply.go"阻塞 IO 边界"一节）。
	LoadTarget(ctx context.Context, item *iproto.PipelineConfigItem) error
	// Unload 卸载 pipelineID 对应角色的动态模块。role 决定卸载哪一组模块名
	// （源端 binlog_in.<id>+amqp_out.<id>；目标端 amqp_in.<id>，并关闭其内部持有的
	// *mysqlout.Writer 连接）。对不存在的模块名调用是安全的（xhive.RemoveDynamicModule
	// 返回 false 时视为"本来就不在，等价于已卸载"，不当作错误，见 internal/node/controller/loader.go）。
	Unload(pipelineID string, role iproto.PipelineRole) error
}

// Supported roles use this in-place path; the loader must never return success
// before the running worker has actually accepted the new configuration.
type iPipelineHotUpdater interface {
	CanHotUpdate(*iproto.PipelineConfigItem) bool
	HotUpdate(context.Context, *iproto.PipelineConfigItem, string) error
}

// iAckHandle 是本包处理一条控制面消息生命周期所需要的最小能力：确认成功、丢弃且
// 不重新入队、拒绝并重新入队。*transport.AckHandle 天然满足本接口（结构化类型匹配，
// 不需要显式声明实现关系）。本包定义自己的最小接口而不是直接在内部逻辑里传递
// *transport.AckHandle，是为了方便单元测试注入假实现，覆盖 internal/node/controller/handler.go 里各条分支，
// 不需要真实 Broker 连接——与 probe.iAckHandle/amqpin.iAckHandle 同一个理由（各自
// 独立定义、不共享类型，见 internal/node/probe/interfaces.go 同名接口的注释）。
type iAckHandle interface {
	Ack() error
	NackDiscard() error
	NackRequeue() error
}
