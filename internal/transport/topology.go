package transport

import (
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"
)

// 交换机名称。所有中心声明同一组交换机，队列只在消费方所在中心声明。
const (
	ExchangeRegister = "xdts.register"
	ExchangeRPC      = "xdts.rpc"
	ExchangeReply    = "xdts.reply"
	ExchangeControl  = "xdts.control"
	ExchangeReport   = "xdts.report"
	ExchangeData     = "xdts.data"
	ExchangeDLX      = "xdts.dlx"
)

// 队列名称构造函数。

// ManagerRegisterQueue 是管理中心消费节点注册请求的队列名。
func ManagerRegisterQueue() string { return "xdts.manager.register" }

// ManagerRPCQueue 是管理中心消费节点 RPC 请求的队列名。
func ManagerRPCQueue() string { return "xdts.manager.rpc" }

// ManagerReportQueue 是管理中心消费心跳/状态/进度/事件上报的队列名。
func ManagerReportQueue() string { return "xdts.manager.report" }

// ManagerLogQueue 是管理中心消费日志批次的队列名（与 ManagerReportQueue 同交换机、
// 不同路由键，但参数不同——见 ManagerLogQueueOptions 的 x-max-length）。
func ManagerLogQueue() string { return "xdts.manager.log" }

// ReplyQueue 是 Manager 响应某个节点某次 RPC 的独占回复队列名，instanceID 由发起 RPC
// 的一方生成（每次注册/RPC 实例各自独立，避免复用队列名导致回复错投）。
func ReplyQueue(instanceID string) string { return "xdts.reply." + instanceID }

// NodeControlQueue 是某节点接收控制指令的队列名。
func NodeControlQueue(nodeID string) string { return fmt.Sprintf("xdts.node.%s.ctl", nodeID) }

// PipelineDataQueue 是某 pipeline 增量数据队列名，由目标节点在目标中心声明（MGR-PL-07）。
func PipelineDataQueue(pipelineID string) string { return "xdts.pl." + pipelineID }

// PipelineSnapshotQueue 是某 pipeline 全量分块队列名。
func PipelineSnapshotQueue(pipelineID string) string { return "xdts.pl." + pipelineID + ".snap" }

// DeadLetterQueue 是各中心统一的死信队列名。
func DeadLetterQueue() string { return "xdts.dlq" }

// 路由键构造函数。

func RoutingKeyRegister() string { return "register" }
func RoutingKeyRPC() string      { return "rpc" }

func RoutingKeyReply(instanceID string) string { return "reply." + instanceID }

func RoutingKeyNode(nodeID string) string       { return "node." + nodeID }
func RoutingKeyCenter(centerCode string) string { return "center." + centerCode }
func RoutingKeyNodeAll() string                 { return "node.all" }

// reportRoutingKey 是 xdts.report 交换机四类上报路由键的公共构造逻辑
// （report.<center>.<nodeId>.{heartbeat,status,progress,event}）。
func reportRoutingKey(center, nodeID, kind string) string {
	return fmt.Sprintf("report.%s.%s.%s", center, nodeID, kind)
}

func RoutingKeyHeartbeat(center, nodeID string) string {
	return reportRoutingKey(center, nodeID, "heartbeat")
}
func RoutingKeyStatus(center, nodeID string) string {
	return reportRoutingKey(center, nodeID, "status")
}
func RoutingKeyProgress(center, nodeID string) string {
	return reportRoutingKey(center, nodeID, "progress")
}
func RoutingKeyEvent(center, nodeID string) string {
	return reportRoutingKey(center, nodeID, "event")
}
func RoutingKeyLog(center, nodeID string) string {
	return reportRoutingKey(center, nodeID, "log")
}

func RoutingKeyPipelineData(pipelineID string) string     { return "pl." + pipelineID }
func RoutingKeyPipelineSnapshot(pipelineID string) string { return "pl." + pipelineID + ".snap" }

// ===== 交换机声明 =====

func exchangeOptions(name, kind string) xamqp.ExchangeOptions {
	return xamqp.ExchangeOptions{
		Name:    name,
		Kind:    kind,
		Durable: true,
		Declare: true,
	}
}

func RegisterExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeRegister, amqp.ExchangeDirect)
}
func RPCExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeRPC, amqp.ExchangeDirect)
}
func ReplyExchangeOptions() xamqp.ExchangeOptions {
	//："默认交换机不可联邦，故用具名交换机"——回复必须走具名交换机，不能用
	// AMQP 默认交换机 + reply_to 的常见简化写法。
	return exchangeOptions(ExchangeReply, amqp.ExchangeDirect)
}
func ControlExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeControl, amqp.ExchangeTopic)
}
func ReportExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeReport, amqp.ExchangeTopic)
}
func DataExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeData, amqp.ExchangeDirect)
}
func DeadLetterExchangeOptions() xamqp.ExchangeOptions {
	return exchangeOptions(ExchangeDLX, amqp.ExchangeFanout)
}

// ===== 队列声明 =====
//
// 需求 MQ-15（libscout 实测发现）：RabbitMQ 4.x 默认关闭了 transient_nonexcl_queues，
// 非持久化 + 非独占的队列声明会直接返回 INTERNAL_ERROR。下面除 ReplyQueueOptions
// （独占队列）外的全部队列构造函数都固定 Durable: true，不作为可关闭的选项暴露，
// 避免调用方无意中触发这个错误。

func durableQueueOptions(name string, args amqp.Table) xamqp.QueueOptions {
	return xamqp.QueueOptions{
		Name:    name,
		Durable: true,
		Declare: true,
		Args:    args,
	}
}

func ManagerRegisterQueueOptions() xamqp.QueueOptions {
	return durableQueueOptions(ManagerRegisterQueue(), nil)
}

func ManagerRPCQueueOptions() xamqp.QueueOptions {
	return durableQueueOptions(ManagerRPCQueue(), nil)
}

// ReplyQueueOptions 是回复队列：独占（只有发起方自己的连接能用）+ 自动删除
// （连接断开或用完即回收）。独占队列不受 MQ-15 的 Durable=true 强制要求约束。
func ReplyQueueOptions(instanceID string) xamqp.QueueOptions {
	return xamqp.QueueOptions{
		Name:       ReplyQueue(instanceID),
		Exclusive:  true,
		AutoDelete: true,
		Declare:    true,
	}
}

// 节点控制队列持久化指令；ttl<=0 时不设置 x-expires。
func NodeControlQueueOptions(nodeID string, ttl time.Duration) xamqp.QueueOptions {
	var args amqp.Table
	if ttl > 0 {
		args = amqp.Table{"x-expires": ttl.Milliseconds()}
	}
	return durableQueueOptions(NodeControlQueue(nodeID), args)
}

func ManagerReportQueueOptions() xamqp.QueueOptions {
	return durableQueueOptions(ManagerReportQueue(), nil)
}

// ManagerLogQueueOptions 声明日志队列：x-max-length + 溢出丢队头（日志允许丢，
// 需求 MQ-06）。maxLength<=0 时不设置上限（不建议，仅供测试）。
func ManagerLogQueueOptions(maxLength int) xamqp.QueueOptions {
	var args amqp.Table
	if maxLength > 0 {
		args = amqp.Table{
			"x-max-length": maxLength,
			"x-overflow":   "drop-head",
		}
	}
	return durableQueueOptions(ManagerLogQueue(), args)
}

// PipelineDataQueueOptions declares a quorum, single-active-consumer data queue
// with explicit delivery limits and dead-letter routing, without TTL or length caps.
// Declare the dead-letter destination before messages can reach the limit.
// Existing queues require an explicitly coordinated parameter migration; deleting
// a queue discards its unconsumed data.
func PipelineDataQueueOptions(pipelineID string, deliveryLimit int) xamqp.QueueOptions {
	args := amqp.Table{
		"x-queue-type":             "quorum",
		"x-single-active-consumer": true,
		"x-dead-letter-exchange":   ExchangeDLX,
	}
	if deliveryLimit > 0 {
		args["x-delivery-limit"] = deliveryLimit
	}
	return durableQueueOptions(PipelineDataQueue(pipelineID), args)
}

// PipelineSnapshotQueueOptions 声明全量分块队列：quorum，不开 single-active-consumer
// （全量分块允许并发消费）。
//
// RabbitMQ 4.x 的 quorum 队列默认限制失败投递次数；不声明该参数也会耗尽上限。
// 全量写入由 internal/node/pipeline/snapwriter/snapwriter.go 消费，失败分块必须保留
// 等待重试。因此显式使用 -1 关闭 delivery limit；不设置 TTL 或长度上限。
// 已按旧参数声明的队列不得自动删除重建；不兼容声明必须报告给操作方处理。
func PipelineSnapshotQueueOptions(pipelineID string) xamqp.QueueOptions {
	args := amqp.Table{"x-queue-type": "quorum", "x-delivery-limit": int32(-1)}
	return durableQueueOptions(PipelineSnapshotQueue(pipelineID), args)
}

// DeadLetterQueueOptions 声明死信队列：quorum + durable。
//
// 现网升级同样受"队列参数不可变"约束：给已经按旧参数（classic）声明过的 xdts.dlq
// 追加/更改 x-queue-type 会触发 406 PRECONDITION_FAILED，处置方式见
// IsPreconditionFailed 文档，这里不重复展开。生产调用入口为
// internal/transport/pipeline_probe.go 的 PreparePipelineProbe。
func DeadLetterQueueOptions() xamqp.QueueOptions {
	return durableQueueOptions(DeadLetterQueue(), amqp.Table{"x-queue-type": "quorum"})
}

// ===== Declarator 封装 =====

// Declarator 集中管理 xdts 交换机和队列声明，使用 xamqp.Declarator 执行。
type Declarator struct {
	d *xamqp.Declarator
	// xc 保留连接引用，只给删除类方法用（DeleteDataQueue/DeleteControlQueue）：
	// xamqp.Declarator 本身不提供任何删除能力（连它内部依赖的
	// xamqp/internal/manager/channel.Manager 都没有 QueueDeleteSafe 这类方法，
	// 已经确认过，不是漏看），删除只能退回 amqp091-go 的原始 Channel，而拿原始
	// Channel 的唯一导出入口 xamqp.Channel.WithChannelForNewGeneration 要求先用
	// xamqp.NewChannel(conn) 单独开一个通道——Declarator 内部的通道是私有字段，
	// 外部拿不到。
	xc *xamqp.Conn
}

// NewDeclarator 创建声明器，内部使用独立的 AMQP 通道（不与业务发布/消费通道共享）。
func NewDeclarator(conn *Conn) (*Declarator, error) {
	if conn == nil || conn.xc == nil {
		return nil, ErrConnManagerNil
	}
	d, err := xamqp.NewDeclarator(conn.xc)
	if err != nil {
		return nil, fmt.Errorf("transport: 创建声明器失败: %w", err)
	}
	return &Declarator{d: d, xc: conn.xc}, nil
}

// Close 释放声明器占用的 AMQP 通道。
func (dc *Declarator) Close() { dc.d.Close() }

// DeclareExchange 声明单个交换机。
func (dc *Declarator) DeclareExchange(opts xamqp.ExchangeOptions) error {
	if err := dc.d.Exchange(opts); err != nil {
		return fmt.Errorf("transport: 声明交换机 %s 失败: %w", opts.Name, err)
	}
	return nil
}

// DeclareQueue 声明单个队列。
func (dc *Declarator) DeclareQueue(opts xamqp.QueueOptions) error {
	if err := dc.d.Queue(opts); err != nil {
		return fmt.Errorf("transport: 声明队列 %s 失败: %w", opts.Name, err)
	}
	return nil
}

// BindQueue 把队列绑定到交换机的一个路由键。
func (dc *Declarator) BindQueue(exchange, queue, routingKey string) error {
	err := dc.d.BindQueues([]xamqp.Binding{{
		Source:      exchange,
		Destination: queue,
		RoutingKey:  routingKey,
	}})
	if err != nil {
		return fmt.Errorf("transport: 绑定队列 %s -> %s (%s) 失败: %w", exchange, queue, routingKey, err)
	}
	return nil
}

// UnbindQueue 解除队列到交换机某个路由键的绑定，不删除队列本身。幂等——绑定本来就
// 不存在时 RabbitMQ 返回成功（AMQP 规范里 queue.unbind 对不存在的绑定不报错）。
//
// 目前的资源回收路径（DeleteDataQueue/DeleteControlQueue）不需要调用方先手动解绑：
// AMQP 的 queue.delete 会连同该队列的全部绑定一起清掉（amqp091-go 文档："QueueDelete
// removes the queue from the server including all bindings"），单独解绑不是删除队列的
// 必要前置步骤。保留这个方法是因为它对应"声明有 BindQueue 就该有反操作"这个对称性
// 期望，且底层 xamqp.Declarator 本来就已经有 UnbindQueues 能力，只是没有被这一层
// 透出——之前漏掉的是暴露，不是要重新实现一遍。
func (dc *Declarator) UnbindQueue(exchange, queue, routingKey string) error {
	err := dc.d.UnbindQueues([]xamqp.Binding{{
		Source:      exchange,
		Destination: queue,
		RoutingKey:  routingKey,
	}})
	if err != nil {
		return fmt.Errorf("transport: 解绑队列 %s -> %s (%s) 失败: %w", exchange, queue, routingKey, err)
	}
	return nil
}

// DeclareManagerTopology 声明管理中心侧的拓扑：register/rpc/report/log 四个队列及其
// 交换机与绑定。应在 Manager 启动、控制面/上报面连接建立后调用一次。
//
// logQueueMaxLength 会写进 xdts.manager.log 队列的 x-max-length 参数（见
// ManagerLogQueueOptions）——AMQP 队列参数不可变，这个值如果和 broker 上已经声明的队列
// 不一致会导致声明失败（406 PRECONDITION_FAILED），调用方应该用 IsPreconditionFailed
// 判断并按"确定性失败、需人工介入"处理，不要当成普通的连接故障重试（cmd/manager 的
// connectAMQPWithRetry 是现成的参考实现）。
func (dc *Declarator) DeclareManagerTopology(logQueueMaxLength int) error {
	steps := []struct {
		exchange xamqp.ExchangeOptions
		queue    xamqp.QueueOptions
		key      string
	}{
		{RegisterExchangeOptions(), ManagerRegisterQueueOptions(), RoutingKeyRegister()},
		{RPCExchangeOptions(), ManagerRPCQueueOptions(), RoutingKeyRPC()},
	}
	for _, s := range steps {
		if err := dc.DeclareExchange(s.exchange); err != nil {
			return err
		}
		if err := dc.DeclareQueue(s.queue); err != nil {
			return err
		}
		if err := dc.BindQueue(s.exchange.Name, s.queue.Name, s.key); err != nil {
			return err
		}
	}

	// xdts.report 交换机下挂 report 与 log 两个队列，参数不同（见各自的 Options 函数），
	// 路由键用通配符匹配四类上报 / 一类日志（report.*.*.{heartbeat,status,progress,event}
	// 与 report.*.*.log）。
	reportExchange := ReportExchangeOptions()
	if err := dc.DeclareExchange(reportExchange); err != nil {
		return err
	}
	reportQueue := ManagerReportQueueOptions()
	if err := dc.DeclareQueue(reportQueue); err != nil {
		return err
	}
	for _, kind := range []string{"heartbeat", "status", "progress", "event"} {
		if err := dc.BindQueue(reportExchange.Name, reportQueue.Name, "report.*.*."+kind); err != nil {
			return err
		}
	}
	logQueue := ManagerLogQueueOptions(logQueueMaxLength)
	if err := dc.DeclareQueue(logQueue); err != nil {
		return err
	}
	if err := dc.BindQueue(reportExchange.Name, logQueue.Name, "report.*.*.log"); err != nil {
		return err
	}
	return nil
}

// DeclareNodeControlQueue 在节点所在中心声明该节点的控制队列，并绑定
// node.<nodeId>、center.<code>、node.all 三个路由键。
//
// ttl 会写进队列的 x-expires 参数（见 NodeControlQueueOptions）——AMQP 队列参数不可变，
// 这个值如果和 broker 上已经声明的同名队列不一致会导致声明失败（406
// PRECONDITION_FAILED，且是确定性失败，重试不会自愈）。调用方应该用 IsPreconditionFailed
// 判断这类错误、区别于普通连接故障单独处理，不要自动删除冲突队列（会丢弃队列里未消费的
// 指令，这个代价必须由运维决定）。
func (dc *Declarator) DeclareNodeControlQueue(nodeID, centerCode string, ttl time.Duration) error {
	exchange := ControlExchangeOptions()
	if err := dc.DeclareExchange(exchange); err != nil {
		return err
	}
	queue := NodeControlQueueOptions(nodeID, ttl)
	if err := dc.DeclareQueue(queue); err != nil {
		return err
	}
	for _, key := range []string{RoutingKeyNode(nodeID), RoutingKeyCenter(centerCode), RoutingKeyNodeAll()} {
		if err := dc.BindQueue(exchange.Name, queue.Name, key); err != nil {
			return err
		}
	}
	return nil
}

// DeclareReplyQueue 在发起 RPC 一方所在中心声明本次调用专属的独占回复队列。
func (dc *Declarator) DeclareReplyQueue(instanceID string) error {
	exchange := ReplyExchangeOptions()
	if err := dc.DeclareExchange(exchange); err != nil {
		return err
	}
	queue := ReplyQueueOptions(instanceID)
	if err := dc.DeclareQueue(queue); err != nil {
		return err
	}
	return dc.BindQueue(exchange.Name, queue.Name, RoutingKeyReply(instanceID))
}

// DeclarePipelineDataQueues 由目标节点在目标中心为一个 pipeline 声明增量与全量两个
// 数据队列（需求 MGR-PL-07："数据队列由目标节点在目标中心声明"）。
//
// deliveryLimit 会写进增量队列的 x-delivery-limit 参数（见
// PipelineDataQueueOptions）——AMQP 队列参数不可变，这个值如果和 broker 上已经声明的同名
// 队列不一致会导致声明失败（406 PRECONDITION_FAILED，确定性失败，重试不会自愈）。这类队列
// 是 per-pipeline 的，爆炸半径随管道数量放大——调用方必须用 IsPreconditionFailed 判断并按
// "需人工介入"处理，不要当成瞬时连接故障无限重试，也不要自动删除冲突队列（会丢数据）。
func (dc *Declarator) DeclarePipelineDataQueues(pipelineID string, deliveryLimit int) error {
	exchange := DataExchangeOptions()
	if err := dc.DeclareExchange(exchange); err != nil {
		return err
	}
	dataQueue := PipelineDataQueueOptions(pipelineID, deliveryLimit)
	if err := dc.DeclareQueue(dataQueue); err != nil {
		return err
	}
	if err := dc.BindQueue(exchange.Name, dataQueue.Name, RoutingKeyPipelineData(pipelineID)); err != nil {
		return err
	}
	snapQueue := PipelineSnapshotQueueOptions(pipelineID)
	if err := dc.DeclareQueue(snapQueue); err != nil {
		return err
	}
	return dc.BindQueue(exchange.Name, snapQueue.Name, RoutingKeyPipelineSnapshot(pipelineID))
}

// DeclareDeadLetterQueue 在本中心声明死信交换机与队列。
func (dc *Declarator) DeclareDeadLetterQueue() error {
	exchange := DeadLetterExchangeOptions()
	if err := dc.DeclareExchange(exchange); err != nil {
		return err
	}
	queue := DeadLetterQueueOptions()
	if err := dc.DeclareQueue(queue); err != nil {
		return err
	}
	// fanout 交换机的绑定不需要路由键，routingKey 传空字符串即可。
	return dc.BindQueue(exchange.Name, queue.Name, "")
}

// ===== 资源回收（MGR-PL-13 两步删除 / MGR-NODE-06a 节点注销） =====

// DeleteDataQueue 幂等地删除一个数据队列（xdts.pl.<id> 或 xdts.pl.<id>.snap），删除前
// 强制确认队列已空——数据队列绝不能在还有未消费消息时删除，否则丢数据、违反 C-22
// （"删除 pipeline 时由目标节点在消费完后删除"）。队列非空时返回
// ErrQueueNotEmpty，这不是错误，是"这一轮回收先跳过，留给下一轮重试"的预期分支
//
// Quorum queues reject ifUnused/ifEmpty with a connection-level 540. Cleanup
// callers must first confirm the source cannot publish and stop the target;
// then require zero ready messages and consumers. Consumer shutdown requeues
// unfinished deliveries. No queue is deleted while a consumer is still active.
// A passive count alone is not a fence against arbitrary external producers.
func (dc *Declarator) DeleteDataQueue(name string) error {
	ch, err := xamqp.NewChannel(dc.xc)
	if err != nil {
		return fmt.Errorf("transport: 删除队列 %s 时创建一次性通道失败: %w", name, err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(ch.Close)

	var (
		messages   int
		consumers  int
		inspectErr error
		deleteErr  error
		didDelete  bool
	)
	ch.WithChannelForNewGeneration(func(raw *amqp.Channel, _ uint) {
		q, ierr := raw.QueueDeclarePassive(name, false, false, false, false, nil)
		if ierr != nil {
			inspectErr = ierr
			return
		}
		messages = q.Messages
		consumers = q.Consumers
		if messages > 0 || consumers > 0 {
			return
		}
		didDelete = true
		_, deleteErr = raw.QueueDelete(name, false, false, false)
	})

	if inspectErr != nil {
		if isAMQPNotFound(inspectErr) {
			return nil // 查询时已经不存在，视为删除已经达成（幂等）。
		}
		return fmt.Errorf("transport: 查询队列 %s 状态失败: %w", name, inspectErr)
	}
	if !didDelete {
		return fmt.Errorf("%w: %s 还有 %d 条未消费消息和 %d 个消费者", ErrQueueNotEmpty, name, messages, consumers)
	}
	if deleteErr != nil {
		if isAMQPNotFound(deleteErr) {
			return nil
		}
		return fmt.Errorf("transport: 删除队列 %s 失败: %w", name, deleteErr)
	}
	return nil
}

// 同样自己开一次性 Channel、同样固定传 ifUnused=false/ifEmpty=false 的理由见
// DeleteDataQueue 的注释（这里没有"先查询"这一步，因为本来就不检查是否为空）。
func (dc *Declarator) DeleteControlQueue(name string) error {
	ch, err := xamqp.NewChannel(dc.xc)
	if err != nil {
		return fmt.Errorf("transport: 删除队列 %s 时创建一次性通道失败: %w", name, err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(ch.Close)

	var deleteErr error
	ch.WithChannelForNewGeneration(func(raw *amqp.Channel, _ uint) {
		_, deleteErr = raw.QueueDelete(name, false, false, false)
	})
	if deleteErr != nil {
		if isAMQPNotFound(deleteErr) {
			return nil
		}
		return fmt.Errorf("transport: 删除队列 %s 失败: %w", name, deleteErr)
	}
	return nil
}

// isAMQPErrorCode 是 isAMQPNotFound/IsPreconditionFailed 共用的核心判断：errors.As 拿到
// *amqp.Error 后比较错误码，不比较错误文本（broker 换个版本措辞就可能变，错误码不会）。
func isAMQPErrorCode(err error, code int) bool {
	var amqpErr *amqp.Error
	if !errors.As(err, &amqpErr) {
		return false
	}
	return amqpErr.Code == code
}

// isAMQPNotFound 判断一个来自 amqp091-go 的 error 是不是"目标不存在"
// （AMQP 404 NOT_FOUND channel 异常），供删除类方法实现幂等——目标本来就不存在时视为
// 已经达成目标，不是失败。
func isAMQPNotFound(err error) bool {
	return isAMQPErrorCode(err, amqp.NotFound)
}

// IsQueueNotFound identifies a passive queue lookup for an absent queue.
func IsQueueNotFound(err error) bool { return isAMQPNotFound(err) }

// IsPreconditionFailed 判断一个来自 amqp091-go 的 error 是不是 AMQP 406
// PRECONDITION_FAILED channel 异常。
//
// 判断方式与 isAMQPNotFound 同一个模式：errors.As 拿到 *amqp.Error 比错误码，不比较
// 错误文本。
func IsPreconditionFailed(err error) bool {
	return isAMQPErrorCode(err, amqp.PreconditionFailed)
}
