package probe

// iAckHandle 是本包处理一条控制面消息生命周期所需要的最小能力：确认成功、
// 丢弃且不重新入队。*transport.AckHandle 天然满足本接口（结构化类型匹配，不需要
// 显式声明实现关系）。本包定义自己的最小接口而不是直接在内部逻辑里传递
// *transport.AckHandle，是为了方便单元测试注入假实现，覆盖 internal/node/probe/handler.go 里各条
// 分支，不需要真实 Broker 连接——与 amqpin.iAckHandle 同一个理由（该包接口注释：
// "方便单元测试注入假实现...不需要真实 Broker 连接"）。真正与 transport 交互的
// 唯一入口是 onDelivery 方法本身（internal/node/probe/handler.go），拿到 *transport.Message 后立刻
// 转换成 *iproto.Envelope + iAckHandle 两个更窄的类型，本包内部其余逻辑不再直接
// 依赖 transport 的具体类型。
type iAckHandle interface {
	Ack() error
	NackDiscard() error
}
