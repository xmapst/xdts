package pipelinespec

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type AssembleResult struct {
	Source       *iproto.PipelineConfigItem
	SourceDigest string
	Target       *iproto.PipelineConfigItem
	TargetDigest string
}

// 只读：不修改 pipeline/endpoint/strategy_version 任何一行，不递增
// config_version（读取 p.ConfigVersion 原样填入两份配置，谁在什么条件下
// 应该先递增再调用本函数，是调用方的职责，见 internal/manager/pipelinespec/doc.go）。
func Assemble(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) (*AssembleResult, error) {
	p, err := repo.GetPipeline(ctx, db, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 读取 pipeline 失败: %w", err)
	}
	if p.DesiredState == model.DesiredStateRunning {
		if err := ValidateFullOnlyTarget(ctx, db, p); err != nil {
			return nil, err
		}
		if err := repo.CheckTopologyStart(ctx, db, p.TopologyID); err != nil {
			return nil, err
		}
	}

	sourceEndpoint, err := repo.GetEndpoint(ctx, db, p.SourceEndpointID)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 读取源端点失败: %w", err)
	}
	targetEndpoint, err := repo.GetEndpoint(ctx, db, p.TargetEndpointID)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 读取目标端点失败: %w", err)
	}

	sv, err := repo.GetStrategyVersion(ctx, db, p.StrategyID, p.StrategyVersion)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 读取策略版本失败: %w", err)
	}
	rulesDoc, err := ParseStrategyRules(sv.Rules)
	if err != nil {
		return nil, err
	}
	var replicaParams *iproto.SnapshotParams
	if rulesDoc.Source != nil {
		replicaParams = rulesDoc.Source.Snapshot
	}
	replicaEndpoint, err := resolveSnapshotReplicaEndpoint(ctx, db, p, sourceEndpoint, replicaParams)
	if err != nil {
		return nil, err
	}

	centers, edges, err := repo.GetTopologyGraph(ctx, db, uuidx.MustParse(repo.AutomaticTopologyID))
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 读取拓扑图失败: %w", err)
	}
	originMap, err := buildOriginMap(ctx, db, centers)
	if err != nil {
		return nil, err
	}

	protoInitMode, startKind, err := initModeToProto(p.InitMode)
	if err != nil {
		return nil, err
	}
	startPos, err := buildStartPosition(startKind, p.StartPosition)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 解析起始位点失败（pipeline_id=%s): %w", p.ID.String(), err)
	}
	if p.PositionRevision > 0 {
		confirmed, err := repo.GetAppliedPositionChange(ctx, db, p)
		if err != nil || confirmed.Record.ConfirmedAt == nil || confirmed.Record.ConfirmedBy.IsZero() || !startPos.GetResolved() || !proto.Equal(startPos, confirmed.Position) {
			return nil, repo.ErrPositionChangeConflict
		}
	}

	sourceConn, err := buildEndpointConn(sourceEndpoint)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 组装源端点连接信息失败: %w", err)
	}
	if err := requireShadowTableCapabilities(ctx, db, p, rulesDoc.Source, sourceConn.GetLowerCaseTableNames() == iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_SENSITIVE); err != nil {
		return nil, err
	}
	targetConn, err := buildEndpointConn(targetEndpoint)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 组装目标端点连接信息失败: %w", err)
	}
	sourceCaps, err := buildCaps(sourceEndpoint)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 计算源端点能力集合失败: %w", err)
	}
	targetCaps, err := buildCaps(targetEndpoint)
	if err != nil {
		return nil, fmt.Errorf("pipelinespec: 计算目标端点能力集合失败: %w", err)
	}

	pipelineIDStr := p.ID.String()
	commonBase := &iproto.CommonConfig{
		PipelineId:      pipelineIDStr,
		ConfigVersion:   p.ConfigVersion,
		RunId:           p.RunID,
		ProtocolVersion: iproto.CurrentProtocolVersion(),
		Topology:        buildTopology(centers, edges),
		OriginMap:       originMap,
		DataExchange:    transport.ExchangeData,
		RoutingKey:      transport.RoutingKeyPipelineData(pipelineIDStr),
		SnapRoutingKey:  transport.RoutingKeyPipelineSnapshot(pipelineIDStr),
	}
	if err := ValidatePublicationWindowRules(rulesDoc); err != nil {
		return nil, err
	}
	commonBase.SnapshotPlanExtensionsV1, err = snapshotExtensionPair(ctx, db, p)
	if err != nil {
		return nil, err
	}
	commonBase.SnapshotTableCreationV1, err = snapshotTableCreationPair(ctx, db, p)
	if err != nil {
		return nil, err
	}
	commonBase.DdlSourceDependenciesV1, err = ddlSourceDependenciesPair(ctx, db, p)
	if err != nil {
		return nil, err
	}
	commonBase.SnapshotTableRecoveryV1, err = snapshotRecoveryPair(ctx, db, p)
	if err != nil {
		return nil, err
	}
	if source := rulesDoc.Source; source != nil && source.IncrementalWindow != nil {
		limits, _ := iproto.NormalizePublicationWindow(source.IncrementalWindow)
		if limits.Batches > 1 {
			if err := requirePublicationWindowCapabilities(ctx, db, p); err != nil {
				return nil, err
			}
		}
		commonBase.IncrementalWindow = proto.Clone(source.IncrementalWindow).(*iproto.IncrementalWindowPolicy)
	}
	for _, edge := range commonBase.Topology.GetEdges() {
		if edge.GetPipelineId() == pipelineIDStr {
			commonBase.DatabaseRoute = &iproto.DatabaseRoute{SourceCenter: edge.GetSourceCenter(), TargetCenter: edge.GetTargetCenter(), SourceServerUuid: sourceConn.GetServerUuid(), TargetServerUuid: targetConn.GetServerUuid()}
			persistent, err := persistentLineagePair(ctx, db, p)
			if err != nil {
				return nil, err
			}
			// Unprobed cross-center routes retain the center guard.
			commonBase.DatabaseRoute.PersistentLineageV1 = persistent && sourceConn.GetServerUuid() != "" && targetConn.GetServerUuid() != ""
			// Recovery UUIDs are generated by the source between generations.
			// Their complete database lineage must precede copied rows so reverse
			// and cascading routes never infer an unknown UUID as local origin.
			commonBase.SnapshotTableRecoveryV1 = commonBase.SnapshotTableRecoveryV1 && commonBase.DatabaseRoute.PersistentLineageV1
			if _, err := iproto.DatabaseLineageRouteFromProto(commonBase.DatabaseRoute); err != nil {
				return nil, err
			}
			break
		}
	}
	var reinitTarget *iproto.ReinitTargetPlan
	var reinitSource *iproto.ReinitSourcePlan
	if p.RunID > 0 {
		stored, err := repo.NewReinitializations(db).GetPreview(ctx, p.ID, p.RunID-1)
		if err != nil || stored.Record.ConfirmedAt == nil || stored.Record.NewRunID != p.RunID || stored.Record.SnapshotUUID != p.SnapshotUUID || stored.Record.SchemaUUID != p.SchemaUUID || stored.Record.ConfigVersion+1 > p.ConfigVersion {
			return nil, repo.ErrReinitConflict
		}
		if p.ReinitPending {
			digest, err := repo.ReinitInputsDigest(p, sourceEndpoint, targetEndpoint, sv.Rules, replicaEndpoint)
			if err != nil || digest != stored.Document.InputsDigest {
				return nil, repo.ErrReinitConflict
			}
		}
		commonBase.ReinitIdentity = &iproto.ReinitIdentity{FromRunId: stored.Record.FromRunID, NewRunId: stored.Record.NewRunID, Digest: stored.Record.Digest, SnapshotUuid: stored.Record.SnapshotUUID.String(), SchemaUuid: uuidStringOrEmpty(stored.Record.SchemaUUID), CmdSeq: p.LastCmdSeq}
		handling := iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_KEEP
		if stored.Document.TargetHandling == model.ReinitTargetClear {
			handling = iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_CLEAR
		}
		reinitTarget = &iproto.ReinitTargetPlan{Handling: handling, PrepareUuid: uuidStringOrEmpty(stored.Record.PrepareUUID), Digest: stored.Record.Digest}
		reinitSource = &iproto.ReinitSourcePlan{Digest: stored.Record.Digest}
		for _, table := range stored.Document.Tables {
			reinitSource.Tables = append(reinitSource.Tables, &iproto.ReinitSourceTable{Schema: table.SourceSchema, Table: table.SourceTable, ExpectedDdl: table.SourceDDL})
			reinitTarget.Tables = append(reinitTarget.Tables, &iproto.ReinitTargetTable{Schema: table.Schema, Table: table.Table, ExpectedDdl: table.ExpectedDDL})
		}
	}
	sourceCommon, err := cloneCommon(commonBase)
	if err != nil {
		return nil, err
	}
	targetCommon, err := cloneCommon(commonBase)
	if err != nil {
		return nil, err
	}

	var binlogServerID uint32
	if p.BinlogServerID != nil {
		binlogServerID = *p.BinlogServerID
	}

	// Executable rules are validated before role projection. Row predicates
	// receive independent protocol ownership alongside column policies.
	sourceRuleSet := &iproto.SourceRuleSet{}
	var originPolicy *iproto.OriginPolicy
	var snapshotParams *iproto.SnapshotParams
	if sr := rulesDoc.Source; sr != nil {
		if _, err := iproto.CompileColumnFilters(sr.ColumnFilters, sourceEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()); err != nil {
			return nil, err
		}
		sourceRuleSet.ColumnFilters = iproto.CloneColumnFilters(sr.ColumnFilters)
		if _, err := iproto.CompileRowFilters(sr.RowFilters, sourceEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()); err != nil {
			return nil, err
		}
		for _, row := range sr.RowFilters {
			sourceRuleSet.RowFilters = append(sourceRuleSet.RowFilters, proto.Clone(row).(*iproto.RowFilterRule))
		}
		for _, filter := range sr.ObjectFilters {
			sourceRuleSet.ObjectFilters = append(sourceRuleSet.ObjectFilters, proto.Clone(filter).(*iproto.ObjectFilterRule))
		}
		sourceRuleSet.OperationFilter = sr.OperationFilter
		if len(sr.ShadowTables) > 0 {
			sourceRuleSet.ShadowTables = iproto.CloneShadowTables(sr.ShadowTables)
			if _, err := iproto.CompileShadowTables(sourceRuleSet.ShadowTables, sourceEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()); err != nil {
				return nil, err
			}
		}
		if sr.IncrementalRateLimit != nil {
			sourceRuleSet.IncrementalRateLimit = proto.Clone(sr.IncrementalRateLimit).(*iproto.RateLimit)
		}
		originPolicy = sr.OriginPolicy
		snapshotParams = sr.Snapshot
	}
	// Also protect old source nodes that do not recognize the new metadata flag.
	sourceRuleSet.ObjectFilters = append(sourceRuleSet.ObjectFilters, &iproto.ObjectFilterRule{Action: iproto.FilterAction_FILTER_ACTION_EXCLUDE, SchemaMatch: iproto.MatchType_MATCH_TYPE_EXACT, SchemaPattern: "xdtsmeta"})
	if (protoInitMode == iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL || protoInitMode == iproto.InitMode_INIT_MODE_SNAPSHOT_ONLY) && snapshotParams == nil {
		snapshotParams = &iproto.SnapshotParams{ReadConcurrency: 2, ChunkRows: 2000, MaxTxnRows: 10000, MaxTxnBytes: 16 << 20}
	}
	var snapshotEndpoint *iproto.EndpointConn
	if replicaEndpoint != nil {
		snapshotEndpoint, err = buildEndpointConn(replicaEndpoint)
		if err != nil {
			return nil, err
		}
	}

	targetRuleSet := &iproto.TargetRuleSet{}
	if err := ValidateWritePolicyRules(rulesDoc); err != nil {
		return nil, err
	}
	if err := ValidateDDLPolicyRules(rulesDoc); err != nil {
		return nil, err
	}
	if err := ValidateMappingRules(rulesDoc); err != nil {
		return nil, err
	}
	if err := RequireETLCapabilities(ctx, db, p, rulesDoc); err != nil {
		return nil, err
	}
	if err := ValidateTransformRules(rulesDoc); err != nil {
		return nil, err
	}
	var writePolicy *iproto.WritePolicy
	var conflictPolicy *iproto.ConflictPolicy
	var ddlPolicy *iproto.DDLPolicy
	var errorPolicy *iproto.ErrorPolicy
	var snapshotWriteConcurrency uint32
	if tr := rulesDoc.Target; tr != nil {
		if tr.Mappings != nil {
			targetRuleSet.Mappings = proto.Clone(tr.Mappings).(*iproto.MappingRules)
		}
		for _, transform := range tr.Transforms {
			if transform.GetExecutionSide() == iproto.TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_SOURCE {
				sourceRuleSet.Transforms = append(sourceRuleSet.Transforms, proto.Clone(transform).(*iproto.CleanseTransformRule))
			} else {
				targetRuleSet.Transforms = append(targetRuleSet.Transforms, proto.Clone(transform).(*iproto.CleanseTransformRule))
			}
		}
		writePolicy = tr.WritePolicy
		conflictPolicy = tr.ConflictPolicy
		ddlPolicy = tr.DDLPolicy
		errorPolicy = tr.ErrorPolicy
		snapshotWriteConcurrency = tr.SnapshotWriteConcurrency
	}
	var schemaPlan *iproto.SchemaMigrationPlan
	if !p.SchemaUUID.IsZero() {
		stored, err := repo.GetSchemaPlan(ctx, db, p.ID, p.RunID)
		if err != nil {
			return nil, err
		}
		if stored.Record.ConfirmedAt == nil || stored.Record.ConfigVersion+1 > p.ConfigVersion {
			return nil, repo.ErrSchemaPlanConflict
		}
		schemaPlan, err = stored.Plan.Protocol(p.SchemaUUID.String())
		if err != nil {
			return nil, err
		}
	}

	// sourceItem：Target 字段恒不填充。这不是"约定不填"，是这条构造语句
	// 里根本没有出现 targetConn/targetCaps 等任何目标侧变量——凭据最小化
	// 靠这个直接检查，不是靠测试断言，见 internal/manager/pipelinespec/doc.go。
	sourceItem := &iproto.PipelineConfigItem{
		PipelineId:    pipelineIDStr,
		ConfigVersion: p.ConfigVersion,
		Role:          iproto.PipelineRole_PIPELINE_ROLE_SOURCE,
		Common:        sourceCommon,
		Source: &iproto.SourceSideConfig{
			PositionRevision: p.PositionRevision,
			Endpoint:         sourceConn,
			SnapshotEndpoint: snapshotEndpoint,
			BinlogServerId:   binlogServerID,
			InitMode:         protoInitMode,
			StartPosition:    startPos,
			Snapshot:         snapshotParams,
			SnapshotUuid:     uuidStringOrEmpty(p.SnapshotUUID),
			Rules:            sourceRuleSet,
			OriginPolicy:     originPolicy,
			Caps:             sourceCaps,
			ReinitPlan:       reinitSource,
		},
	}

	// targetItem：Source 字段恒不填充，理由同上（这条构造语句里没有出现
	// 任何 sourceConn/sourceCaps 等源侧变量）。
	targetItem := &iproto.PipelineConfigItem{
		PipelineId:    pipelineIDStr,
		ConfigVersion: p.ConfigVersion,
		Role:          iproto.PipelineRole_PIPELINE_ROLE_TARGET,
		Common:        targetCommon,
		Target: &iproto.TargetSideConfig{
			ReinitPlan:    reinitTarget,
			MaxOriginHops: originPolicy.GetMaxHops(),
			Endpoint:      targetConn,
			QueueDecl: &iproto.QueueDecl{
				IncrementalQueue: transport.PipelineDataQueue(pipelineIDStr),
				SnapshotQueue:    transport.PipelineSnapshotQueue(pipelineIDStr),
				// IncrementalDeliveryLimit/IncrementalPrefetch：见 internal/manager/pipelinespec/doc.go
				// "已知未覆盖的字段"——目前没有配置来源，产出零值。
			},
			SchemaPlan:     schemaPlan,
			Rules:          targetRuleSet,
			WritePolicy:    writePolicy,
			ConflictPolicy: conflictPolicy,
			DdlPolicy:      ddlPolicy,
			ErrorPolicy:    errorPolicy,

			SnapshotWriteConcurrency:  snapshotWriteConcurrency,
			Caps:                      targetCaps,
			InitMode:                  protoInitMode,
			SnapshotUuid:              uuidStringOrEmpty(p.SnapshotUUID),
			SnapshotMaxTxnRows:        snapshotParams.GetMaxTxnRows(),
			SnapshotMaxTxnBytes:       snapshotParams.GetMaxTxnBytes(),
			SourceLowerCaseTableNames: sourceConn.GetLowerCaseTableNames(),
			ShadowTables:              iproto.CloneShadowTables(sourceRuleSet.ShadowTables),
		},
	}

	sourceItem.Common.StateBundle, err = repo.GetPipelineStateBundle(ctx, db, p, model.SideSource)
	if err != nil {
		return nil, err
	}
	targetItem.Common.StateBundle, err = repo.GetPipelineStateBundle(ctx, db, p, model.SideTarget)
	if err != nil {
		return nil, err
	}

	sourceDigest, err := computeConfigDigest(sourceItem)
	if err != nil {
		return nil, err
	}
	targetDigest, err := computeConfigDigest(targetItem)
	if err != nil {
		return nil, err
	}

	return &AssembleResult{
		Source:       sourceItem,
		SourceDigest: sourceDigest,
		Target:       targetItem,
		TargetDigest: targetDigest,
	}, nil
}

// buildEndpointConn 将端点转换为协议连接参数；调用方按角色只投影对应端点。
// TLS 和 params 不包含协议枚举，使用 encoding/json 解析。
func buildEndpointConn(e *model.Endpoint) (*iproto.EndpointConn, error) {
	conn := &iproto.EndpointConn{
		EndpointId: e.ID.String(),
		Host:       e.Host,
		Port:       uint32(e.Port),
		Username:   e.Username,
		Password:   e.Password,
		ServerUuid: e.ServerUUID,
	}
	if len(e.TLS) > 0 {
		var tls iproto.TLSConfig
		if err := json.Unmarshal(e.TLS, &tls); err != nil {
			return nil, fmt.Errorf("解析端点 TLS 配置失败（endpoint_id=%s）: %w", e.ID.String(), err)
		}
		conn.Tls = &tls
	}
	if len(e.Params) > 0 {
		var params map[string]string
		if err := json.Unmarshal(e.Params, &params); err != nil {
			return nil, fmt.Errorf("解析端点连接参数失败（endpoint_id=%s）: %w", e.ID.String(), err)
		}
		conn.Params = params
	}
	// Preserve the probed case-comparison policy. Missing probe data remains unspecified
	// and must be validated by the consuming node before use.
	conn.LowerCaseTableNames = lowerCaseTableNamesToProto(e.LowerCaseTableNames)
	return conn, nil
}

// lowerCaseTableNamesToProto 把 model.Endpoint.LowerCaseTableNames 存的
// 枚举名字符串（internal/mysqlx.LowerCaseTableNames.String() 的产物：
// "SENSITIVE"/"LOWERCASE_STORED"/"LOWERCASE_COMPARE"，空字符串表示未
// 探测）转换成协议枚举。internal/mysqlx 只提供了反方向的
// ParseLowerCaseTableNames（解析 SELECT @@lower_case_table_names 的原始
// "0"/"1"/"2"，不是这里要处理的名字字符串），因此这个方向的转换是
// pipelinespec 自己的职责，和 initModeToProto 是同一种"落库表示→协议
// 枚举"的拆分需要。
//
// 空字符串或任何未识别的取值一律映射到 UNSPECIFIED，不报错：与
// buildCaps 对 Version 为空的处理一致，"未探测"是合法的正常状态，不是
// 需要中断组装的错误；出现未识别的非空取值理论上不该发生（落库编码只有
// 这三种，见 internal/manager/repo/ddl_endpoint_lower_case_table_names.go），但组装阶段没有
// 更好的处理方式，回退到协议自己规定的安全默认值（大小写不敏感）比中断
// 整条 pipeline 的配置下发更稳妥。
func lowerCaseTableNamesToProto(s string) iproto.LowerCaseTableNames {
	switch s {
	case "SENSITIVE":
		return iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_SENSITIVE
	case "LOWERCASE_STORED":
		return iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_LOWERCASE_STORED
	case "LOWERCASE_COMPARE":
		return iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_LOWERCASE_COMPARE
	default:
		return iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_UNSPECIFIED
	}
}

func buildCaps(e *model.Endpoint) (*iproto.MySQLCaps, error) {
	if e.Version == "" {
		return &iproto.MySQLCaps{}, nil
	}
	ver, err := mysqlx.ParseVersion(e.Version)
	if err != nil {
		return nil, fmt.Errorf("解析端点 MySQL 版本失败（endpoint_id=%s, version=%q）: %w", e.ID.String(), e.Version, err)
	}
	caps := mysqlx.DetectCaps(ver)
	return &iproto.MySQLCaps{
		RowMetadataFull:    caps.RowMetadataFull,
		SessionVarAdmin:    caps.SessionVarAdmin,
		ReplicationApplier: caps.ReplicationApplier,
		ShowRoutinePriv:    caps.ShowRoutinePriv,
		TxnCompression:     caps.TxnCompression,
		InvisibleColumn:    caps.InvisibleColumn,
		Gipk:               caps.GIPK,
		SetAnyDefiner:      caps.SetAnyDefiner,
		BinlogStatusStmt:   caps.BinlogStatusStmt,
		VectorType:         caps.VectorType,
	}, nil
}

// initModeToProto 把 model.InitMode（7 个值，UI/存储层用一个枚举同时表达
// "初始化方式"与"仅增量时的起始位点子类型"两个维度）拆回协议的两个正交
// 枚举：iproto.InitMode（4 个广义模式）与 iproto.InitPositionKind（4 个
// 起始位点子类型，只在 INCREMENTAL_ONLY 时有意义）。
func initModeToProto(m model.InitMode) (iproto.InitMode, iproto.InitPositionKind, error) {
	switch m {
	case model.InitModeFullAndIncremental:
		return iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL, iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED, nil
	case model.InitModeIncrementalOnlyLatest:
		return iproto.InitMode_INIT_MODE_INCREMENTAL_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_LATEST, nil
	case model.InitModeIncrementalOnlyGTIDSet:
		return iproto.InitMode_INIT_MODE_INCREMENTAL_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET, nil
	case model.InitModeIncrementalOnlyBinlogPos:
		return iproto.InitMode_INIT_MODE_INCREMENTAL_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS, nil
	case model.InitModeIncrementalOnlyTimestamp:
		return iproto.InitMode_INIT_MODE_INCREMENTAL_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP, nil
	case model.InitModeFullOnly:
		return iproto.InitMode_INIT_MODE_SNAPSHOT_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED, nil
	case model.InitModeSchemaOnly:
		return iproto.InitMode_INIT_MODE_SCHEMA_ONLY, iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED, nil
	default:
		return 0, 0, fmt.Errorf("pipelinespec: 未知的 InitMode %q", m)
	}
}

// buildStartPosition 解析 Pipeline.StartPosition（"具体 schema 由 service
// 层与节点协商"，model.Pipeline 字段注释——本函数是第一个真正约定它的
// 调用方）。kind 恒由 initModeToProto 派生而来，是权威值：不管 JSON 里
// 有没有携带/携带了什么 kind，解析完都会被强制覆盖回 kind 形参，避免
// 两处描述"当前用哪种起始位点"的信息互相打架。
//
// kind=UNSPECIFIED（全量+增量/仅全量/仅结构迁移，起始位点由快照过程本身
// 决定，不是用户输入）或 raw 为空（创建/更新入口 internal/manager/http/pipelines.go 把它当
// `unknown` 透传，现有 pipeline 绝大多数是这个状态，
// 见 web/src/api/pipelines.ts"startPosition?: unknown"）时，直接返回只有
// kind 字段的零值，不当错误处理。
func buildStartPosition(kind iproto.InitPositionKind, raw model.JSON) (*iproto.StartPosition, error) {
	pos := &iproto.StartPosition{Kind: kind}
	if kind == iproto.InitPositionKind_INIT_POSITION_KIND_UNSPECIFIED || len(raw) == 0 {
		return pos, nil
	}
	if err := protojson.Unmarshal(raw, pos); err != nil {
		return nil, fmt.Errorf("解析失败: %w", err)
	}
	pos.Kind = kind
	return pos, nil
}

// buildTopology 把 repo.GetTopologyGraph 的查询结果转换成协议的 Topology
// 消息，纯数据搬运，不做任何过滤——centers/edges 是整个拓扑（可能覆盖
// 多条 pipeline）的公共视图，源、目标节点拿到的是同一份，不是"只给这条 pipeline 自己那条边"。
//
// 已确认的缺口：同一拓扑里新建/删除一条 pipeline，会改变本函数对其它
// pipeline 算出的 Topology 内容，但那些 pipeline 的 config_version 不会 bump、
// 不会重新下发，于是它们手里的 Topology 会一直停在旧值，直到下一次因别的原因
// 重新下发。不修，理由是代价与收益不成比例：修就意味着任一条 pipeline 增删
// 都要对同拓扑另外 N-1 条各做一次完整 Assemble + 下发，而换来的只是刷新一个
// 节点侧不读其内容的字段。
func buildTopology(centers []repo.TopologyGraphCenter, edges []repo.TopologyGraphEdge) *iproto.Topology {
	t := &iproto.Topology{
		Centers: make([]*iproto.CenterInfo, 0, len(centers)),
		Edges:   make([]*iproto.TopologyEdge, 0, len(edges)),
	}
	for _, c := range centers {
		t.Centers = append(t.Centers, &iproto.CenterInfo{
			Code:            c.Code,
			Name:            c.Name,
			IsManagerCenter: c.IsManagerCenter,
		})
	}
	for _, e := range edges {
		t.Edges = append(t.Edges, &iproto.TopologyEdge{
			SourceCenter: e.SourceCenterCode,
			TargetCenter: e.TargetCenterCode,
			PipelineId:   e.PipelineID.String(),
		})
	}
	return t
}

// Node internal/node/controller/options.go constructs the binlog origin resolver from this
// map. Every identity change must refresh all edges of the affected topologies;
// center_origin_refresh retains the work until both node roles apply it.
func buildOriginMap(ctx context.Context, db *gorm.DB, centers []repo.TopologyGraphCenter) ([]*iproto.OriginUUIDSet, error) {
	result := make([]*iproto.OriginUUIDSet, 0, len(centers))
	for _, c := range centers {
		origins, err := repo.ListCenterOriginUUIDs(ctx, db, c.ID)
		if err != nil {
			return nil, fmt.Errorf("pipelinespec: 读取中心 %s 的来源 UUID 集合失败: %w", c.Code, err)
		}
		uuids := make([]string, 0, len(origins))
		var servers []string
		for _, o := range origins {
			uuids = append(uuids, o.UUID.String())
			if o.Kind == model.OriginKindServer {
				servers = append(servers, o.UUID.String())
			}
		}
		result = append(result, &iproto.OriginUUIDSet{
			CenterCode:  c.Code,
			Uuids:       uuids,
			ServerUuids: servers,
		})
	}
	return result, nil
}

func uuidStringOrEmpty(id uuidx.UUID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

// cloneCommon 深拷贝 CommonConfig，让 source/target 两份 PipelineConfigItem
// 各自持有独立的 Common 对象，不共享指针——两份配置的字段内容目前相同
// （公共部分本就该相同），但独立持有可以避免"以后谁不小心改了其中一份的
// Common 字段，另一份跟着变"这种隔着走的副作用。
func cloneCommon(c *iproto.CommonConfig) (*iproto.CommonConfig, error) {
	cloned, ok := proto.Clone(c).(*iproto.CommonConfig)
	if !ok || cloned == nil {
		return nil, fmt.Errorf("pipelinespec: proto.Clone(CommonConfig) 返回了意外的类型")
	}
	return cloned, nil
}
