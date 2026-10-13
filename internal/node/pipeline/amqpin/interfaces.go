package amqpin

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

// iApplier 是本包应用一个完整 DataBatch 所需的最小接口，*mysqlout.Writer 天然
// 实现它。与 amqpout.iPublisher/binlogin.ISink 同一模式：各包各自独立定义同形
// 接口而不共享 mysqlout.IBatchApplier 类型，避免通过共享内部接口类型制造隐性
// 耦合（见 amqpout.iPublisher 的同类注释）。
type iApplier interface {
	ApplyBatch(ctx context.Context, batch *iproto.DataBatch) error
}

type iResultApplier interface {
	ApplyBatchResult(context.Context, *iproto.DataBatch) (iproto.BatchApplyResult, error)
}

// iAckHandle 是本包处理一条消息生命周期所需要的最小能力：确认成功、取
// message_id 用于日志。*transport.AckHandle 天然满足本接口（结构化类型匹配，
// 不需要显式声明实现关系）。本包定义自己的最小接口而不是直接在内部逻辑里传递
// *transport.AckHandle，是为了方便单元测试注入假实现、覆盖 internal/node/pipeline/amqpin/handle.go 里
// 分派/重组/冻结的各条分支，而不需要真实 Broker 连接——与 iApplier 是同一个
// 理由。真正与 transport 交互的唯一入口是 handle 方法本身（internal/node/pipeline/amqpin/handle.go），拿到
// *transport.Message 后立刻转换成 *iproto.Envelope + iAckHandle 两个更窄的类型，
// 本包内部其余逻辑不再直接依赖 transport 的具体类型。
type iAckHandle interface {
	Ack() error
	MessageID() string
}

// 为空时检查点只在内存（appliedSet/lastEpoch/lastBatchSeq 三个 Module 字段仍然
// 正常更新，供 Stats() 读取），不落盘，重启后从空状态重新开始——不是致命配置，
// 只是失去跨重启的可观测性连续性，不影响正确性（目标端去重权威始终是目标库
// gtid_executed，NODE-CKP-04，本包的检查点不是任何恢复逻辑的输入）。
//
// 观察检查点用 Put。显式跳过决定会影响后续去重，必须通过独立异步能力
// iCriticalAsyncStore 确认关键提交后才使用，不能依赖周期检查点。
type iStoreAccess interface {
	Put(op store.Op) error
	Get(ctx context.Context, collection, id string, out any) error
	PutCritical(ctx context.Context, op store.Op) error
}

type iCriticalAsyncStore interface {
	PutCriticalAsync(context.Context, store.Op) (<-chan error, error)
}
