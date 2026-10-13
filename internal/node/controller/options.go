package controller

import (
	"context"
	"fmt"
	"math"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpout"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/rule"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/internal/transport"
)

// nodeContext 是下面 buildXxxOptions 系列函数需要的、与具体 pipeline 无关的节点级
// 上下文，来自 controller.Options（本节点自己的身份与共享句柄），不来自协议下发的
// PipelineConfigItem。拆成独立类型而不是把 controller.Options 整个传进去：
// buildXxxOptions 是纯函数（不访问 Module 的任何内部状态），显式列出真正用到的字段
// 让函数签名本身就说明它的依赖范围，也方便测试构造最小输入。
type nodeContext struct {
	Quota           *resourcequota.Coordinator
	Center          string
	NodeID          string
	Token           string
	ProtocolVersion uint32
	Conn            *transport.Conn
	Publisher       iPublisher
	Store           iStoreAccess
	SourceRecovery  func(context.Context, *iproto.SourceRecoveryRequest) (*iproto.SourceRecoveryReply, error)
}

// buildSourceOptions 把 PipelineConfigItem（role 必须是 SOURCE）翻译成
// binlogin.Options 与 amqpout.Options（源端两个动态模块的构造参数）。
//
// startGTIDSet is resolved by apply.go. A non-nil pointer preserves an explicit
// empty initial GTID; nil requests recovery of the existing published position.
// A confirmed position revision retains its resolved baseline in the config.
//
// 不在这里构造 amqpout.Module 再塞进 binlogin.Options.Sink：两个 Options
// 的构造在本函数内各自独立完成，由调用方（internal/node/controller/loader.go）先用 buildSourceOptions
// 产出的 amqpout.Options 构造出 *amqpout.Module，再把它作为 sink 参数传给
// 装配 binlogin.Options 的下一步——这样 buildSourceOptions 本身保持纯函数
// （不调用任何 xhive/网络相关的构造函数），可以独立单测。
func buildSourceOptions(item *iproto.PipelineConfigItem, nc nodeContext, startGTIDSet *string) (binlogin.Options, amqpout.Options, error) {
	if _, err := iproto.NormalizePublicationWindow(item.GetCommon().GetIncrementalWindow()); err != nil {
		return binlogin.Options{}, amqpout.Options{}, err
	}
	if item.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: buildSourceOptions 要求 role=SOURCE，实际 %s", item.GetRole())
	}
	src := item.GetSource()
	if src == nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: pipeline %q role=SOURCE 但 source 字段为空", item.GetPipelineId())
	}
	if err := iproto.ValidateSnapshotReplicaPolicy(src.GetSnapshot()); err != nil {
		return binlogin.Options{}, amqpout.Options{}, err
	}
	if src.GetSnapshot().GetReadFromReplica() {
		if !needsSnapshot(item) || src.GetSnapshotEndpoint() == nil || src.GetSnapshotEndpoint().GetEndpointId() != src.GetSnapshot().GetReplicaEndpointId() || len(src.GetSnapshotEndpoint().GetParams()) != 0 {
			return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: replica snapshot endpoint does not match the configured read policy")
		}
	} else if src.GetSnapshotEndpoint() != nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: replica credentials without replica snapshot mode")
	}
	ep := src.GetEndpoint()
	if ep == nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: pipeline %q source.endpoint 为空", item.GetPipelineId())
	}
	port, err := toPort16(ep.GetPort())
	if err != nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: pipeline %q source.endpoint: %w", item.GetPipelineId(), err)
	}

	// 名称匹配必须沿用探测得到的 lower_case_table_names，
	// 规则编译和 schema 跟踪使用同一比较策略。
	caseSensitive := lowerCaseTableNamesToCaseSensitive(ep.GetLowerCaseTableNames())

	ruleSet, err := ruleSetFromSource(src.GetRules(), src.GetOriginPolicy())
	if err != nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: pipeline %q 规则翻译失败: %w", item.GetPipelineId(), err)
	}
	plan, err := rule.Compile(ruleSet, rule.SideSource, caseSensitive)
	if err != nil {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: pipeline %q 规则编译失败: %w", item.GetPipelineId(), err)
	}
	transforms, err := iproto.CompileSourceTransforms(src.GetRules().GetTransforms(), caseSensitive)
	if err != nil {
		return binlogin.Options{}, amqpout.Options{}, err
	}
	var sourceTableFilter binlogin.ITableFilter = plan
	var sourceColumnFilter binlogin.IColumnFilter = plan
	var sourceRowFilter iproto.ISourceRowPlan = plan
	var shadowPolicy *iproto.ShadowTablePolicy
	if rules := src.GetRules().GetShadowTables(); len(rules) > 0 {
		shadowPolicy = &iproto.ShadowTablePolicy{Rules: iproto.CloneShadowTables(rules), SourceCaseSensitive: caseSensitive}
		if err := iproto.ValidateShadowTablePolicy(shadowPolicy); err != nil {
			return binlogin.Options{}, amqpout.Options{}, err
		}
		shadowPlan, err := iproto.CompileShadowTables(rules, caseSensitive)
		if err != nil {
			return binlogin.Options{}, amqpout.Options{}, err
		}
		shadowRules, err := iproto.NewShadowSourceRules(plan, shadowPlan)
		if err != nil {
			return binlogin.Options{}, amqpout.Options{}, err
		}
		sourceTableFilter, sourceColumnFilter, sourceRowFilter = shadowRules, shadowRules, shadowRules
	}

	resolver, err := newOriginResolver(nc.Center, item.GetCommon().GetOriginMap())
	if err != nil {
		return binlogin.Options{}, amqpout.Options{}, err
	}
	route, err := iproto.DatabaseLineageRouteFromProto(item.GetCommon().GetDatabaseRoute())
	if err != nil || (route.SourceCenter != "" && (route.SourceCenter != nc.Center || route.SourceServerUUID != ep.GetServerUuid())) {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: invalid source database lineage route")
	}
	start := ""
	if startGTIDSet != nil {
		start = *startGTIDSet
	}
	binOpts := binlogin.Options{
		DDLSourceDependencies: item.GetCommon().GetDdlSourceDependenciesV1(),
		PublicationWindow:     iproto.ClonePublicationWindow(item.GetCommon().GetIncrementalWindow()),
		ConfigVersion:         item.GetConfigVersion(),
		Budget:                nc.Quota.Budget(item.GetPipelineId()),
		PipelineID:            item.GetPipelineId(),
		NodeID:                nc.NodeID,
		RunID:                 item.GetCommon().GetRunId(),
		PositionRevision:      src.GetPositionRevision(),
		Center:                nc.Center,
		ServerID:              src.GetBinlogServerId(),
		Host:                  ep.GetHost(),
		Port:                  port,
		User:                  ep.GetUsername(),
		Password:              ep.GetPassword(),
		StartGTIDSet:          start,
		StartGTIDSetProvided:  startGTIDSet != nil,
		SourceCaseSensitive:   caseSensitive,

		TableFilter:       sourceTableFilter,
		ColumnFilter:      sourceColumnFilter,
		RowFilter:         sourceRowFilter,
		Transforms:        transforms,
		DDLColumnPolicy:   &iproto.DDLColumnPolicy{Rules: iproto.CloneColumnFilters(src.GetRules().GetColumnFilters()), SourceCaseSensitive: caseSensitive},
		OriginResolver:    resolver,
		DatabaseRoute:     route,
		OriginFilter:      sourceOriginFilter{plan: plan, local: nc.Center, mode: src.GetOriginPolicy().GetMode()},
		MaxOriginHops:     src.GetOriginPolicy().GetMaxHops(),
		DropUnknownOrigin: src.GetOriginPolicy().GetDropUnknownOrigin(),
		Store:             nc.Store,
		// 增量限流（SourceRuleSet.incremental_rate_limit）直接搬进 binlogin.Options，
		// **不经过 ruleSetFromSource**：那个函数的产物是 rule.RuleSet → rule.Compile →
		// *rule.Plan，而 Plan 回答的是"这张表/这一行放不放行"这个纯判断；限流回答的是
		// "多快"，既不是判断、也不改变放行结果。把它塞进 rule.RuleSet 只会让
		// rule.Compile 背一个与编译无关的透传字段，并让 Plan 长出一个没人当判断用的
		// 旋钮。能"放慢"的那一层是 binlogin（它是投递 DataBatch 的地方），所以协议到
		// 执行方的通路就是这里。
		//
		// GetIncrementalRateLimit() 为 nil（未配置限流，绝大多数 pipeline）时两个
		// getter 都返回 0，而 0 在 binlogin 侧就是"不限速"——这是协议语义本身，不是
		// 兜底默认值，见 binlogin.Options.IncrementalRowsPerSec 的文档。
		IncrementalRowsPerSec:  src.GetRules().GetIncrementalRateLimit().GetRowsPerSec(),
		IncrementalBytesPerSec: src.GetRules().GetIncrementalRateLimit().GetBytesPerSec(),
		ShadowTablePolicy:      iproto.CloneShadowTablePolicy(shadowPolicy),
	}

	if src.GetPositionRevision() > 0 {
		if src.GetPositionRevision() > math.MaxInt64 || !src.GetStartPosition().GetResolved() {
			return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: revised position has no confirmed resolution")
		}
		binOpts.StartGTIDSet, binOpts.StartGTIDSetProvided = src.GetStartPosition().GetGtidSet(), true
	}
	outOpts := amqpout.Options{
		RunID:           item.GetCommon().GetRunId(),
		PipelineID:      item.GetPipelineId(),
		Center:          nc.Center,
		NodeID:          nc.NodeID,
		Token:           nc.Token,
		ProtocolVersion: nc.ProtocolVersion,
		Publisher:       nc.Publisher,
	}

	for _, edge := range item.GetCommon().GetTopology().GetEdges() {
		if edge.GetPipelineId() == item.GetPipelineId() {
			binOpts.TargetCenter = edge.GetTargetCenter()
			break
		}
	}
	if route.TargetCenter != "" && route.TargetCenter != binOpts.TargetCenter {
		return binlogin.Options{}, amqpout.Options{}, fmt.Errorf("controller: database route disagrees with topology")
	}
	return binOpts, outOpts, nil
}

// buildTargetOptions 把 PipelineConfigItem（role 必须是 TARGET）翻译成
// mysqlout.Options 与 amqpin.Options（除 Applier 外的部分——Applier 需要先用
// mysqlout.Options 真正拨号建立 *mysqlout.Writer，这是阻塞网络 IO，不属于本函数
// 这种纯字段翻译，由调用方 internal/node/controller/loader.go 在 goroutine 里完成后再补上）。
func buildTargetOptions(item *iproto.PipelineConfigItem, nc nodeContext) (mysqlout.Options, amqpin.Options, error) {
	if _, err := iproto.NormalizePublicationWindow(item.GetCommon().GetIncrementalWindow()); err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	if item.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_TARGET {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: buildTargetOptions 要求 role=TARGET，实际 %s", item.GetRole())
	}
	tgt := item.GetTarget()
	if tgt == nil {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: pipeline %q role=TARGET 但 target 字段为空", item.GetPipelineId())
	}
	if err := iproto.ValidateWritePolicy(tgt.GetWritePolicy()); err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	if err := iproto.ValidateConflictPolicy(tgt.GetConflictPolicy()); err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	if err := checkConflictPolicySupported(item.GetPipelineId(), tgt.GetConflictPolicy().GetMode()); err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	ep := tgt.GetEndpoint()
	if ep == nil {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: pipeline %q target.endpoint 为空", item.GetPipelineId())
	}
	port, err := toPort16(ep.GetPort())
	if err != nil {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: pipeline %q target.endpoint: %w", item.GetPipelineId(), err)
	}

	mysqlOpts := mysqlout.Options{
		DDLSourceDependencies: item.GetCommon().GetDdlSourceDependenciesV1(),
		WritePolicy:           tgt.GetWritePolicy(),
		PipelineID:            item.GetPipelineId(),
		Host:                  ep.GetHost(),
		Port:                  port,
		User:                  ep.GetUsername(),
		Password:              ep.GetPassword(),
		DDLPolicy:             tgt.GetDdlPolicy(),
		ConflictPolicy:        tgt.GetConflictPolicy(),
		SchemaOptions:         schemamig.Options{TargetVectorType: tgt.GetCaps().GetVectorType(), TargetInvisibleColumn: tgt.GetCaps().GetInvisibleColumn(), SourceCaseSensitive: tgt.GetSourceLowerCaseTableNames() == iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_SENSITIVE},
	}
	mysqlOpts.Mapping, err = targetMapping(tgt.GetRules().GetMappings(), mysqlOpts.SchemaOptions.SourceCaseSensitive)
	if err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	if len(tgt.GetShadowTables()) > 0 {
		mysqlOpts.ShadowTablePolicy = &iproto.ShadowTablePolicy{Rules: iproto.CloneShadowTables(tgt.GetShadowTables()), SourceCaseSensitive: mysqlOpts.SchemaOptions.SourceCaseSensitive}
		if err := iproto.ValidateShadowTablePolicy(mysqlOpts.ShadowTablePolicy); err != nil {
			return mysqlout.Options{}, amqpin.Options{}, err
		}
		targetCaseSensitive := lowerCaseTableNamesToCaseSensitive(ep.GetLowerCaseTableNames())
		mysqlOpts.Mapping, err = iproto.CompileShadowMapping(tgt.GetShadowTables(), mysqlOpts.Mapping, mysqlOpts.SchemaOptions.SourceCaseSensitive, targetCaseSensitive)
		if err != nil {
			return mysqlout.Options{}, amqpin.Options{}, err
		}
	}
	mysqlOpts.Transforms, err = iproto.TargetTransformsFromProtocol(tgt.GetRules().GetTransforms())
	if err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}

	amqpinOpts := amqpin.Options{
		PublicationWindow: iproto.ClonePublicationWindow(item.GetCommon().GetIncrementalWindow()),
		WritePolicy:       tgt.GetWritePolicy(),
		ConfigVersion:     item.GetConfigVersion(),
		Budget:            nc.Quota.Budget(item.GetPipelineId()),
		PipelineID:        item.GetPipelineId(),
		RunID:             item.GetCommon().GetRunId(),
		Center:            nc.Center,
		MaxOriginHops:     tgt.GetMaxOriginHops(),
		Conn:              nc.Conn,
		Store:             nc.Store,
		// Applier 留空，由 internal/node/controller/loader.go 在拨号成功后补上。
	}
	amqpinOpts.DatabaseRoute, err = iproto.DatabaseLineageRouteFromProto(item.GetCommon().GetDatabaseRoute())
	if err != nil || (amqpinOpts.DatabaseRoute.TargetCenter != "" && (amqpinOpts.DatabaseRoute.TargetCenter != nc.Center || amqpinOpts.DatabaseRoute.TargetServerUUID != ep.GetServerUuid())) {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: invalid target database lineage route")
	}
	mysqlOpts.ExpectedServerUUID = amqpinOpts.DatabaseRoute.TargetServerUUID
	mysqlOpts.DatabaseRoute = amqpinOpts.DatabaseRoute
	amqpinOpts.ErrorPolicy, err = iproto.TargetErrorPolicyFromProto(tgt.GetErrorPolicy())
	if err != nil {
		return mysqlout.Options{}, amqpin.Options{}, err
	}
	if amqpinOpts.ErrorPolicy.SkipUnretryable && nc.Store == nil {
		return mysqlout.Options{}, amqpin.Options{}, fmt.Errorf("controller: skipping errors requires durable node store")
	}

	return mysqlOpts, amqpinOpts, nil
}

// checkConflictPolicySupported rejects unsupported policy modes before network
// connections or modules are created. All published modes have actual writers.
// Unspecified retains the published default source-wins policy.
func checkConflictPolicySupported(pipelineID string, mode iproto.ConflictMode) error {
	switch mode {
	case iproto.ConflictMode_CONFLICT_MODE_UNSPECIFIED, iproto.ConflictMode_CONFLICT_MODE_SOURCE_WINS, iproto.ConflictMode_CONFLICT_MODE_LWW, iproto.ConflictMode_CONFLICT_MODE_STOP:
		return nil
	default:
		return fmt.Errorf("controller: pipeline %q 的目标侧冲突策略 conflict_policy.mode=%d 未识别，"+
			"拒绝装配（协议只增不改语义，新增 ConflictMode 取值需要同步更新 "+
			"checkConflictPolicySupported 与 mysqlout 的写入路径）",
			pipelineID, int32(mode))
	}
}

// lowerCaseTableNamesToCaseSensitive rejects unspecified metadata and maps all
// three valid server values to their identifier-comparison policy.
func lowerCaseTableNamesToCaseSensitive(v iproto.LowerCaseTableNames) bool {
	return v == iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_SENSITIVE
}

// toPort16 把协议里的 uint32 端口号转换成 binlogin.Options/mysqlout.Options 要求的
// uint16，越界时报错而不是静默截断（"边界输入必须校验"）——proto3 的
// uint32 字段本身不约束取值范围，恶意或写错的配置完全可能携带一个超出 TCP 端口
// 合法范围（1-65535）的数字，直接 uint16(port) 转换会把它悄悄截断成另一个可能真实
// 存在、语义却完全不同的端口号。
func toPort16(port uint32) (uint16, error) {
	if port == 0 || port > 65535 {
		return 0, fmt.Errorf("端口号 %d 超出合法范围(1-65535)", port)
	}
	return uint16(port), nil
}
