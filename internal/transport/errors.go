package transport

import "errors"

var (
	ErrPublisherClosed   = errors.New("transport: publisher is closed")
	ErrMessageIDInFlight = errors.New("transport: message_id is already in flight")

	// ErrMissingMessageID 表示尝试发布一个没有 message_id 的信封。
	//
	// message_id 是本包用来把异步到达的 return 事件（Return.MessageId，
	// amqp091-go 在退回消息时会带上原始 Properties）关联回具体发布调用的唯一依据
	// （见 internal/transport/outcome.go 对 libscout 实测的处理），缺了它就没有办法判断某条 return
	// 事件对应的是哪一次发布，因此在发布入口就拒绝，而不是让 return 静默失配。
	ErrMissingMessageID = errors.New("transport: envelope 缺少 message_id")

	// ErrUnexpectedConfirmationCount 表示一次发布得到的确认对象数量与路由键数量不一致。
	// 正常情况下 xamqp 会为每个路由键返回一个确认对象；数量不匹配说明库内部状态与本包
	// 的假设脱节，按不可恢复错误处理，不静默丢弃多余或缺失的确认。
	ErrUnexpectedConfirmationCount = errors.New("transport: 确认对象数量与路由键数量不一致")

	// ErrAckHandleAlreadyResolved 表示对同一个 AckHandle 重复调用 Ack/NackDiscard/
	// NackRequeue。只有第一次调用生效，之后的调用返回本错误而不是静默忽略或 panic
	// （边界输入必须校验）。
	ErrAckHandleAlreadyResolved = errors.New("transport: AckHandle 已经被确认过")

	// ErrConnManagerNil 表示传入的 Conn 未正确初始化（底层连接管理器为空）。
	ErrConnManagerNil = errors.New("transport: 连接未初始化")

	// ErrMissingInstanceID 表示创建 RPCClient 时未提供 instanceID——instanceID 是回复
	// 队列名 xdts.reply.<instanceId> 的一部分，没有它无法声明本次调用
	// 专属的回复队列。
	ErrMissingInstanceID = errors.New("transport: RPCClient 缺少 instanceID")

	// ErrInvalidRPCMaxAttempts 表示 WithRPCMaxAttempts 配置了非法值（< 1）：至少要尝试
	// 一次，否则 Call 无法定义行为。
	ErrInvalidRPCMaxAttempts = errors.New("transport: RPC 最大尝试次数必须 >= 1")

	// ErrRPCRequestUnroutable 表示某次 RPC 请求发布后 confirm ack=true 但被 return
	// （或直接 nack），即这次尝试没有真正送达。需求 NODE-BOOT-04 明确指出跨中心 RPC
	// 要考虑"联邦绑定传播延迟"：这不一定是错误，只是意味着需要重试。
	ErrRPCRequestUnroutable = errors.New("transport: RPC 请求未能路由（可能是联邦绑定尚未传播）")

	// ErrRPCTimeout 表示某次尝试的请求已确认送达，但在本次尝试的时间预算内没有等到
	// 匹配 correlation_id 的回复。
	ErrRPCTimeout = errors.New("transport: RPC 调用等待回复超时")

	// ErrRPCClientClosed 表示 RPCClient 在调用完成前被 Close，所有仍在等待的 Call
	// 立即以此错误结束，而不是继续等到各自的超时。
	ErrRPCClientClosed = errors.New("transport: RPCClient 已关闭")

	// ErrQueueNotEmpty 表示 DeleteDataQueue 发现队列还有未消费的消息，拒绝删除。
	// 不是失败，是预期分支——调用方（回收循环）应该把这次尝试计入 cleanup_attempts
	// 并留在「待清理」列表里等下一轮重试（MGR-PL-13），不是升级成告警。
	ErrQueueNotEmpty = errors.New("transport: 队列非空，拒绝删除")
)
