package iproto

// MessageType* 定义 AMQP properties.type。发送和分派消息时统一使用这些常量。
// 数据面使用 routing key 选择流水线队列，使用 type 区分批次、水位和全量消息。
const (
	// 控制面：注册
	MessageTypeRegisterRequest = "register.request"
	MessageTypeRegisterReply   = "register.reply"

	// 控制面：身份确认与协议版本协商
	MessageTypeNodeHello      = "node.hello"
	MessageTypeNodeHelloReply = "node.hello.reply"

	// 控制面：配置拉取与热更新
	MessageTypeConfigPull               = "config.pull"
	MessageTypeConfigPullReply          = "config.pull.reply"
	MessageTypeConfigChanged            = "config.changed"
	MessageTypeConfigApplyResult        = "config.apply.result"
	MessageTypeSourceRecovery           = "source.recovery"
	MessageTypeSourceRecoveryReply      = "source.recovery.reply"
	MessageTypePipelineStateExport      = "pipeline.state.export"
	MessageTypePipelineStateExportReply = "pipeline.state.export.reply"

	// 控制面：指令
	MessageTypeCmdPipeline = "cmd.pipeline"
	MessageTypeCmdNode     = "cmd.node"
	MessageTypeCmdResult   = "cmd.result"

	// 控制面：探测
	MessageTypeProbeRequest = "probe.request"
	MessageTypeProbeResult  = "probe.result"

	// 上报面
	MessageTypeNodeHeartbeat      = "node.heartbeat"
	MessageTypePipelineStatus     = "pipeline.status"
	MessageTypePipelineProgress   = "pipeline.progress"
	MessageTypeLogBatch           = "log.batch"
	MessageTypeEventError         = "event.error"
	MessageTypeEventDDL           = "event.ddl"
	MessageTypeEventConflict      = "event.conflict"
	MessageTypeEventUnknownOrigin = "event.unknown_origin"
	// 事件类型与 proto/report.proto 的 EventKind 对齐。
	MessageTypeEventBackpressure      = "event.backpressure"
	MessageTypeEventLocalStoreRebuild = "event.local_store_rebuild"
	MessageTypeEventLinkProbeFailed   = "event.link_probe_failed"
	MessageTypeEventNodeOffline       = "event.node_offline"

	// 数据面
	MessageTypeDataBatch             = "data.batch"
	MessageTypeSnapshotChunk         = "data.snapshot_chunk"
	MessageTypeSnapshotEnd           = "data.snapshot_end"
	MessageTypeSnapshotRecoveryBegin = "data.snapshot_recovery_begin"
	MessageTypeWatermark             = "data.watermark"
	MessageTypeDataProbe             = "data.probe"
)
