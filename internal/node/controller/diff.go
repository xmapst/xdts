package controller

import (
	"log/slog"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// changeKind 是 diffConfigs/classifyPipelineChange 对一个 pipeline 配置给出的处理
// 决定（NODE-CFG-02，"热更 / 重建判定表"）。
type changeKind int

const (
	// changeNone 表示该角色可见配置没有变化，例如另一侧的私有配置变化。
	changeNone changeKind = iota

	changeLoad
	// changeHotUpdate 表示只有允许原地更新的规则或策略发生变化。
	changeHotUpdate
	// changeRebuild 表示连接、角色或其它需要重建的配置发生变化。
	changeRebuild
	// changeUnload 表示这条 pipeline 不再由本节点承载（PipelineConfigItem.deleted=
	// true，或在一次全量拉取的结果里彻底消失），需要卸载已加载的模块。
	changeUnload
	// changeStop 表示这条 pipeline 应该从"已装配"回到"已配置但未装配"——STOP/PAUSE
	// 语义：卸载已加载的 xhive
	// 模块，但保留 appliedRecord——key 不删除，只把 running 位改写为 false。
	//
	// internal/node/controller/pull.go 启动恢复路径（触点 3）读到"上次持久化时 running=false"的记录时，不
	// 复用这个值——那条路径此刻从未装配过任何模块（进程刚启动，m.applied 还是空
	// map），没有旧实例可"卸载"，走的是完全不同的直接恢复机制（restoreStartupPlans
	// 文档），不经过 startApply/pipelinePlan。changeStop 专指"曾经装配、现在要
	// 卸载但保留记录"这一个动作，不是"running 位为 false"这个状态本身的通用表达。
	changeStop
	// changeReject 保留已应用配置和模块，并回报过期的配置版本。
	changeReject
)

func (k changeKind) String() string {
	switch k {
	case changeNone:
		return "none"
	case changeLoad:
		return "load"
	case changeHotUpdate:
		return "hot_update"
	case changeRebuild:
		return "rebuild"
	case changeUnload:
		return "unload"
	case changeStop:
		return "stop"
	case changeReject:
		return "reject"
	default:
		return "unknown"
	}
}

// toProto maps an internal change kind to its wire enum. Unknown values panic
// so a new internal state cannot silently acquire an unrelated protocol meaning.
func (k changeKind) toProto() iproto.PipelineChangeKind {
	switch k {
	case changeNone:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_NONE
	case changeLoad:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_LOAD
	case changeHotUpdate:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_HOT_UPDATE
	case changeRebuild:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_REBUILD
	case changeUnload:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_UNLOAD
	case changeStop:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_UNLOAD
	case changeReject:
		return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_NONE
	}
	return iproto.PipelineChangeKind_PIPELINE_CHANGE_KIND_UNSPECIFIED
}

// pipelinePlan 是 diffConfigs 产出的一条待执行动作，item 恒非 nil：changeUnload 时
// item 取自调用方持有的旧配置（新配置里已经没有这条记录，或明确标了 deleted），
// 其余情况 item 是最新拉取到的配置。
type pipelinePlan struct {
	item                  *iproto.PipelineConfigItem
	kind                  changeKind
	cmd                   *cmdContext
	previousConfigVersion uint64
}

// isFullPull 区分这次拉取是 NODE-CFG-01 的"全量拉取"（ConfigPullRequest.pipeline_ids
// 为空，Manager 按约定返回该节点承载的全部 pipeline 配置）还是"定向拉取"（收到
// ConfigChanged 后针对特定 pipeline_id 发起，回复只包含被问到的那些）。两者对"current
// 里有、incoming 里没有"这件事的含义完全不同：
//   - 全量拉取：incoming 就是本节点应该承载的完整集合，current 里任何不在 incoming
//     中出现的 pipeline_id 都意味着"已经不再由本节点承载"，必须按 changeUnload 处理
//     ——这正是 NODE-CFG-02"删除 → 卸载模块"分支覆盖"Manager 单纯不再下发它"（而不是
//     显式 deleted=true）这一半的唯一入口。
//   - 定向拉取：incoming 只是"被问到的那几条"的最新状态，current 里其余未被问到的
//     pipeline_id 与本次拉取无关，不能被当成"消失了"从而误卸载——错误地把这个分支
//     应用到定向拉取会把节点上其它正常运行的 pipeline 全部误判为已删除。
//
// 是纯函数：不发起任何网络/存储调用，不修改 current/incoming 指向的数据。
func diffConfigs(current map[appliedKey]appliedPipeline, incoming []*iproto.PipelineConfigItem, isFullPull bool) []pipelinePlan {
	plans := make([]pipelinePlan, 0, len(incoming))
	// seenKey 记"这次回复里出现过的 (pipeline_id, role)"，seenPipeline 记
	// "出现过的 pipeline_id"——两者用途不同，见下面两段收尾逻辑。
	seenKey := make(map[appliedKey]bool, len(incoming))
	seenPipeline := make(map[string]bool, len(incoming))
	rejectedPipeline := make(map[string]bool)
	latestVersionByPipeline := make(map[string]uint64, len(current))
	for key, applied := range current {
		if applied.GetConfigVersion() > latestVersionByPipeline[key.pipelineID] {
			latestVersionByPipeline[key.pipelineID] = applied.GetConfigVersion()
		}
	}

	for _, item := range incoming {
		if item == nil {
			continue
		}
		key := keyOf(item)
		seenKey[key] = true
		seenPipeline[key.pipelineID] = true
		old, existed := current[key]
		latestVersion := latestVersionByPipeline[key.pipelineID]

		switch {
		case item.GetConfigVersion() < latestVersion:
			rejectedPipeline[key.pipelineID] = true
			plans = append(plans, pipelinePlan{item: item, kind: changeReject, previousConfigVersion: latestVersion})
		case item.Deleted || item.GetOwnershipReleased():
			// Even local absence needs an explicit durable unload receipt so a
			// dropped receipt can be retried after restart.
			plans = append(plans, pipelinePlan{item: item, kind: changeUnload})
		case !existed:
			plans = append(plans, pipelinePlan{item: item, kind: changeLoad})
		default:
			plans = append(plans, pipelinePlan{item: item, kind: classifyPipelineChange(old.PipelineConfigItem, item)})
		}
	}

	// internal/manager/dispatch/ctl_config_pull.go 对每条选中的 pipeline 各自独立判断
	// `p.SourceNodeID == nodeID` 与 `p.TargetNodeID == nodeID`（两个独立的 if，
	// 不是 if/else），把 pipelinespec.Assemble 产出的 result.Source / result.Target
	// 分别追加进回复。也就是说：一条 pipeline 一旦出现在回复里，回复中属于它的
	// 角色投影集合就是 Manager 此刻认定本节点持有的全部侧，少了哪一侧就是
	// 那一侧真的没了，与这次是全量还是定向无关。
	for key, old := range current {
		if seenPipeline[key.pipelineID] && !seenKey[key] && !rejectedPipeline[key.pipelineID] {
			plans = append(plans, pipelinePlan{item: old.PipelineConfigItem, kind: changeUnload})
		}
	}

	if isFullPull {

		for key, old := range current {
			if !seenPipeline[key.pipelineID] {
				plans = append(plans, pipelinePlan{item: old.PipelineConfigItem, kind: changeUnload})
			}
		}
	}

	return plans
}

// classifyPipelineChange 判定一条已存在 pipeline 的配置变化应该热更新还是重建
// （"热更 / 重建判定表"）。old/new 必须是同一个 pipeline_id 的前后两版
// 配置；调用方（diffConfigs）保证这一点，本函数不再校验 pipeline_id 是否一致
// （纯函数，不因为调用方传参错误 panic，但也不重复做调用方已经做过的检查）。
//
// 判定算法：先看是否命中"重建"桶——命中任意一个字段即返回 changeRebuild，不再检查
// "热更"桶（重建是"热更"的超集动作，命中重建的同时其余字段是否也变了不影响最终
// 决定）。不命中重建桶时再检查"热更"桶，命中则 changeHotUpdate，否则 changeNone
// （config_version 变了，但两个角色投影里实际观察到的字段完全相同——例如策略里
// 只有另一侧专属字段变化，本侧下发的投影未受影响）。
func classifyPipelineChange(old, new *iproto.PipelineConfigItem) changeKind {
	if old == nil || new == nil {
		// 只应该由 diffConfigs 在 existed=true 时调用，两个参数此时不会是 nil。
		// 防御性返回最保守的动作而不是 panic——本函数是纯函数，不应该因为调用方
		// 传参错误让整条 diff 链路崩溃；重建绝不会比"什么都不做"更不安全。
		return changeRebuild
	}
	if old.Role != new.Role {
		// Different roles cannot be compared as one applied configuration; rebuild conservatively.
		return changeRebuild
	}
	// Equal nonzero versions imply equal published configuration. Manager must advance
	// the version when any endpoint, strategy or pipeline input changes.
	if old.ConfigVersion == new.ConfigVersion {
		if !proto.Equal(old, new) {
			slog.Warn("pipeline config content changed without version increment",
				slog.String("pipeline_id", new.GetPipelineId()),
				slog.Any("role", new.GetRole()),
				slog.Uint64("config_version", new.ConfigVersion))
		}
		return changeNone
	}

	if new.ConfigVersion < old.ConfigVersion {
		slog.Warn("pipeline config_version regressed, rejecting stale config",
			slog.String("pipeline_id", new.GetPipelineId()),
			slog.Uint64("old_config_version", old.ConfigVersion),
			slog.Uint64("new_config_version", new.ConfigVersion))
		return changeReject
	}

	if rebuildTriggered(old, new) {
		return changeRebuild
	}
	if hotUpdateTriggered(old, new) {
		return changeHotUpdate
	}
	return changeNone
}

// rebuildTriggered 判定"重建"行覆盖的字段是否发生变化：连接信息、
// server_id、初始化方式、起始位点、run_id、队列参数、冲突策略。
func rebuildTriggered(old, new *iproto.PipelineConfigItem) bool {
	if old.GetCommon().GetSnapshotTableRecoveryV1() != new.GetCommon().GetSnapshotTableRecoveryV1() {
		return true
	}
	if old.GetCommon().GetSnapshotTableCreationV1() != new.GetCommon().GetSnapshotTableCreationV1() {
		return true
	}
	if old.GetCommon().GetDdlSourceDependenciesV1() != new.GetCommon().GetDdlSourceDependenciesV1() {
		return true
	}
	if old.GetCommon().GetSnapshotPlanExtensionsV1() != new.GetCommon().GetSnapshotPlanExtensionsV1() {
		return true
	}
	if old.GetCommon().GetDatabaseRoute().GetPersistentLineageV1() != new.GetCommon().GetDatabaseRoute().GetPersistentLineageV1() {
		return true
	}
	if old.Common.GetProtocolVersion() != new.Common.GetProtocolVersion() ||
		old.Common.GetDataExchange() != new.Common.GetDataExchange() ||
		old.Common.GetRoutingKey() != new.Common.GetRoutingKey() ||
		old.Common.GetSnapRoutingKey() != new.Common.GetSnapRoutingKey() ||
		old.Common.GetRunId() != new.Common.GetRunId() {
		return true
	}
	if !sameReinitIdentity(old.GetCommon().GetReinitIdentity(), new.GetCommon().GetReinitIdentity()) {
		return true
	}

	if os, ns := old.Source, new.Source; os != nil || ns != nil {
		if !proto.Equal(os.GetEndpoint(), ns.GetEndpoint()) ||
			!proto.Equal(os.GetSnapshotEndpoint(), ns.GetSnapshotEndpoint()) ||
			os.GetBinlogServerId() != ns.GetBinlogServerId() ||
			os.GetInitMode() != ns.GetInitMode() ||
			// StartPosition 比较 kind + gtid_set + timestamp_confirmed，不整体 proto.Equal：
			// source_file/source_offset/source_ts_ms 是 proto/common.proto 明确标注
			// "仅用于留痕，不参与运行期判断"的原始输入字段（kind=BINLOG_POS/TIMESTAMP
			// 时的换算来源），节点实际使用的只有换算后的 gtid_set。整体 proto.Equal
			// 会把这三个留痕字段也算进比较：运维把起始位点从"按时间戳"改成换算结果
			// 完全相同的"按 binlog 位置"（gtid_set 一字未变）也会触发一次多余的重建。
			// kind 是换算方式；timestamp_confirmed 是时间模式的启动门槛，均参与比较。
			os.GetPositionRevision() != ns.GetPositionRevision() ||
			os.GetStartPosition().GetResolved() != ns.GetStartPosition().GetResolved() ||
			os.GetStartPosition().GetKind() != ns.GetStartPosition().GetKind() ||
			os.GetStartPosition().GetGtidSet() != ns.GetStartPosition().GetGtidSet() ||
			os.GetStartPosition().GetTimestampConfirmed() != ns.GetStartPosition().GetTimestampConfirmed() ||
			os.GetSnapshotUuid() != ns.GetSnapshotUuid() ||
			os.GetSnapshot().GetReadFromReplica() != ns.GetSnapshot().GetReadFromReplica() ||
			os.GetSnapshot().GetReplicaEndpointId() != ns.GetSnapshot().GetReplicaEndpointId() ||
			!proto.Equal(os.GetCaps(), ns.GetCaps()) {
			return true
		}
	}

	if ot, nt := old.Target, new.Target; ot != nil || nt != nil {
		if !proto.Equal(ot.GetEndpoint(), nt.GetEndpoint()) ||
			ot.GetInitMode() != nt.GetInitMode() ||
			ot.GetSnapshotUuid() != nt.GetSnapshotUuid() ||
			ot.GetSourceLowerCaseTableNames() != nt.GetSourceLowerCaseTableNames() ||
			!proto.Equal(ot.GetQueueDecl(), nt.GetQueueDecl()) ||
			!proto.Equal(ot.GetConflictPolicy(), nt.GetConflictPolicy()) ||
			!proto.Equal(ot.GetDdlPolicy(), nt.GetDdlPolicy()) ||
			!proto.Equal(ot.GetSchemaPlan(), nt.GetSchemaPlan()) ||
			!proto.Equal(ot.GetReinitPlan(), nt.GetReinitPlan()) ||
			!proto.Equal(ot.GetCaps(), nt.GetCaps()) {
			return true
		}
	}

	return false
}

// hotUpdateTriggered 判定"热更新"行覆盖的字段是否发生变化：过滤/映射/转换
// 规则、限流、批大小，以及拓扑/来源映射。调用前提：rebuildTriggered 已经返回
// false（本函数不重复检查重建桶的字段）。
func hotUpdateTriggered(old, new *iproto.PipelineConfigItem) bool {

	if !proto.Equal(old.Common.GetIncrementalWindow(), new.Common.GetIncrementalWindow()) ||
		!proto.Equal(old.Common.GetDatabaseRoute(), new.Common.GetDatabaseRoute()) ||
		!proto.Equal(old.Common.GetTopology(), new.Common.GetTopology()) ||
		!originMapsEqual(old.Common.GetOriginMap(), new.Common.GetOriginMap()) {
		return true
	}

	if os, ns := old.Source, new.Source; os != nil || ns != nil {
		if !proto.Equal(os.GetRules(), ns.GetRules()) ||
			!proto.Equal(os.GetOriginPolicy(), ns.GetOriginPolicy()) ||
			os.GetSnapshot().GetReadConcurrency() != ns.GetSnapshot().GetReadConcurrency() ||
			os.GetSnapshot().GetChunkRows() != ns.GetSnapshot().GetChunkRows() ||
			os.GetSnapshot().GetMaxTxnRows() != ns.GetSnapshot().GetMaxTxnRows() ||
			os.GetSnapshot().GetMaxTxnBytes() != ns.GetSnapshot().GetMaxTxnBytes() ||
			!proto.Equal(os.GetSnapshot().GetRateLimit(), ns.GetSnapshot().GetRateLimit()) {
			return true
		}
	}

	if ot, nt := old.Target, new.Target; ot != nil || nt != nil {
		if !proto.Equal(ot.GetRules(), nt.GetRules()) ||
			!proto.Equal(ot.GetWritePolicy(), nt.GetWritePolicy()) ||
			!proto.Equal(ot.GetErrorPolicy(), nt.GetErrorPolicy()) ||
			ot.GetMaxOriginHops() != nt.GetMaxOriginHops() ||
			ot.GetSnapshotWriteConcurrency() != nt.GetSnapshotWriteConcurrency() ||
			ot.GetSnapshotMaxTxnRows() != nt.GetSnapshotMaxTxnRows() ||
			ot.GetSnapshotMaxTxnBytes() != nt.GetSnapshotMaxTxnBytes() {
			return true
		}
	}

	return false
}

// planForItem 用当前 m.applied 状态，为一个已知的最新 PipelineConfigItem 重新计算
// 应该执行的 changeKind——分类逻辑与 diffConfigs 处理单个 item 一致。第二个返回值
// 表示是否需要执行；deleted 且未加载时没有任何状态可刷新，返回 false。
//
// Deletion also produces an unload confirmation when the role is already
// absent. This cannot create an applied record and repairs a lost receipt.
func (m *Module) planForItem(item *iproto.PipelineConfigItem) (pipelinePlan, bool) {
	key := keyOf(item)
	old, existed := m.applied[key]
	var latestVersion uint64
	for appliedKey, applied := range m.applied {
		if appliedKey.pipelineID == key.pipelineID && applied.GetConfigVersion() > latestVersion {
			latestVersion = applied.GetConfigVersion()
		}
	}
	if item.GetConfigVersion() < latestVersion {
		return pipelinePlan{item: item, kind: changeReject, previousConfigVersion: latestVersion}, true
	}
	if item.GetDeleted() || item.GetOwnershipReleased() {
		return pipelinePlan{item: item, kind: changeUnload}, true
	}
	if !existed {
		return pipelinePlan{item: item, kind: changeLoad}, true
	}
	return pipelinePlan{item: item, kind: classifyPipelineChange(old.PipelineConfigItem, item)}, true
}

// originMapsEqual 比较 CommonConfig.OriginMap（[]*OriginUUIDSet，中心 → 来源 UUID
// 集合）是否等价。不能直接用 proto.Equal(切片元素顺序敏感)：CommonConfig.OriginMap
// 语义上是一个集合（"中心 → 来源 UUID 集合映射；运行期整体替换"），
// Manager 组装时的遍历顺序（如按数据库查询结果顺序）与上一次不同不代表内容真的变了，
// 不应该仅因为顺序不同就触发一次不必要的热更新。
func originMapsEqual(a, b []*iproto.OriginUUIDSet) bool {
	if len(a) != len(b) {
		return false
	}
	byCenter := make(map[string]*iproto.OriginUUIDSet, len(a))
	for _, s := range a {
		byCenter[s.GetCenterCode()] = s
	}
	for _, s := range b {
		other, ok := byCenter[s.GetCenterCode()]
		if !ok {
			return false
		}
		if !uuidSetsEqual(s.GetUuids(), other.GetUuids()) || !uuidSetsEqual(s.GetServerUuids(), other.GetServerUuids()) {
			return false
		}
	}
	return true
}

// uuidSetsEqual 比较两个 UUID 字符串集合（同样是集合语义，顺序不敏感）。
func uuidSetsEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}
	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}
	return true
}
