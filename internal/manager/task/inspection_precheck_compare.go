package task

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func precheckManagerItem(code string, level model.PrecheckLevel, pass bool, message string, evidence any) model.PipelinePrecheck {
	status := model.PrecheckStatusFail
	if pass {
		status = model.PrecheckStatusPass
	}
	detail, _ := json.Marshal(struct {
		Message  string `json:"message"`
		Evidence any    `json:"evidence,omitempty"`
	}{message, evidence})
	return model.PipelinePrecheck{Item: "manager/" + code, Level: level, Status: status, Detail: model.JSON(detail)}
}

func precheckDefinitions(result *iproto.PrecheckResult, tables []*iproto.PrecheckTable, sensitive bool, allowMissing bool) ([]schemaplan.Definition, error) {
	if result == nil || len(result.GetTableDefinitions()) > len(tables) {
		return nil, ErrPrecheckProbeFailed
	}
	key := func(schema, table string) string {
		s := schema + "\x00" + table
		if !sensitive {
			s = strings.ToLower(s)
		}
		return s
	}
	expected := map[string]bool{}
	for _, table := range tables {
		expected[key(table.GetSchema(), table.GetTable())] = true
	}
	seen := map[string]bool{}
	out := []schemaplan.Definition{}
	bytes := 0
	for _, d := range result.GetTableDefinitions() {
		k := key(d.GetSchema(), d.GetObjectName())
		bytes += len(d.GetDefinition()) + len(d.GetSchema()) + len(d.GetObjectName()) + len(d.GetTableCollationV1())
		if d == nil || !expected[k] || seen[k] || d.GetKind() != iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE || d.GetDefinition() == "" || bytes > 4<<20 {
			return nil, ErrPrecheckProbeFailed
		}
		seen[k] = true
		out = append(out, schemaplan.Definition{Schema: d.GetSchema(), Name: d.GetObjectName(), Kind: model.ObjectTypeTable, DDL: d.GetDefinition(), TableCollationV1: d.GetTableCollationV1()})
	}
	if !allowMissing && len(seen) != len(expected) {
		return nil, ErrPrecheckProbeFailed
	}
	return out, nil
}

func precheckCompatibleTables(scope *pipelinePrecheckScope, source, target *iproto.PrecheckResult) (bool, any) {
	src, err := precheckDefinitions(source, scope.source.Tables, scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE", false)
	if err != nil {
		return false, map[string]string{"code": "source_definitions_incomplete"}
	}
	dst, err := precheckDefinitions(target, scope.target.Tables, scope.targetEndpoint.LowerCaseTableNames == "SENSITIVE", scope.target.GetAllowMissingTargetTables())
	if err != nil {
		return false, map[string]string{"code": "target_definitions_incomplete"}
	}
	version, err := mysqlx.ParseVersion(target.GetVersion())
	if err != nil || !mysqlx.SupportedVersion(version) {
		return false, map[string]string{"code": "target_version_unverified"}
	}
	caps := mysqlx.DetectCaps(version)
	plan, err := schemaplan.Build(schemaplan.Request{Source: src, Target: dst, Mapping: scope.mapping, TargetCaseSensitive: scope.targetEndpoint.LowerCaseTableNames == "SENSITIVE", Options: schemamig.Options{SourceCaseSensitive: scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE", TargetInvisibleColumn: caps.InvisibleColumn, TargetVectorType: caps.VectorType}})
	if err != nil {
		return false, map[string]string{"code": "definitions_invalid"}
	}
	issues := []map[string]string{}
	for _, step := range plan.Steps {
		if step.Action == model.MigrationActionCreate && scope.migration != nil && scope.migration.TablesVerified && scope.plan != nil {
			covered := false
			for _, approved := range scope.plan.Plan.Steps {
				if approved.Kind == step.Kind && (step.Kind == model.ObjectTypeTable || step.Kind == model.ObjectTypeForeignKey) && approved.Schema == step.Schema && approved.Name == step.Name && approved.Action == model.MigrationActionCreate && approved.DDL == step.DDL {
					covered = true
					break
				}
			}
			if covered {
				continue
			}
		}
		if step.Action == model.MigrationActionBlock || step.Action != model.MigrationActionSkip {
			issues = append(issues, map[string]string{"schema": step.Schema, "table": step.Name, "reason": step.BlockReason, "action": string(step.Action)})
		}
	}
	// Missing tables require an independently verified migration plan. A local
	// existence check alone cannot authorize creating an object on the target.
	return len(issues) == 0, map[string]any{"tables": issues}
}

func precheckEnvironmentComparison(source, target *iproto.PrecheckResult) (bool, any) {
	diff := []map[string]string{}
	for _, key := range []string{"character_set_server", "collation_server", "time_zone", "system_time_zone", "sql_mode"} {
		a, b := source.GetKeyParams()[key], target.GetKeyParams()[key]
		_, sourceKnown := source.GetKeyParams()[key]
		_, targetKnown := target.GetKeyParams()[key]
		if !sourceKnown || !targetKnown || a == "" && key != "sql_mode" || b == "" && key != "sql_mode" || a != b {
			diff = append(diff, map[string]string{"parameter": key, "source": a, "target": b})
		}
	}
	if source == nil || target == nil {
		return false, map[string]string{"code": "environment_unverified"}
	}
	return len(diff) == 0, diff
}

func precheckClockComparison(source, target precheckSideOutcome) (bool, any) {
	maxOffset := float64(0)
	uncertainty := time.Duration(0)
	for _, o := range []precheckSideOutcome{source, target} {
		if o.err != nil || o.result.GetNodeTimeMs() <= 0 || o.started.IsZero() || o.finished.Before(o.started) {
			return false, map[string]string{"code": "node_clock_unverified"}
		}
		mid := o.started.Add(o.finished.Sub(o.started) / 2).UnixMilli()
		maxOffset = math.Max(maxOffset, math.Abs(float64(o.result.GetNodeTimeMs())-float64(mid)))
		uncertainty = max(uncertainty, o.finished.Sub(o.started)/2)
	}
	return maxOffset <= 1000 && uncertainty <= time.Second, map[string]any{"maximum_observed_offset_ms": maxOffset, "measurement_uncertainty_ms": uncertainty.Milliseconds(), "threshold_ms": 1000}
}

func (l *InspectionLoop) precheckIdentities(ctx context.Context, p model.Pipeline, scope *pipelinePrecheckScope, source, target *iproto.PrecheckResult) (bool, error) {
	seen := map[uuidx.UUID]model.Endpoint{}
	for _, side := range []struct {
		ep     model.Endpoint
		result *iproto.PrecheckResult
	}{{scope.sourceEndpoint, source}, {scope.targetEndpoint, target}} {
		id, err := uuidx.Parse(side.result.GetServerUuid())
		if err != nil || id.IsZero() || side.result.GetServerUuid() != side.ep.ServerUUID || side.result.GetVersion() != side.ep.Version {
			return false, nil
		}
		actualCase, err := mysqlx.ParseLowerCaseTableNames(side.result.GetKeyParams()["lower_case_table_names"])
		if err != nil || actualCase.String() != side.ep.LowerCaseTableNames {
			return false, nil
		}
		if old, exists := seen[id]; exists && (old.CenterID != side.ep.CenterID || old.Host != side.ep.Host || old.Port != side.ep.Port) {
			return false, nil
		}
		seen[id] = side.ep
		var endpoints []model.Endpoint
		if err := l.db.WithContext(ctx).Where("server_uuid=? AND id<>? AND (center_id<>? OR host<>? OR port<>?)", id.String(), side.ep.ID, side.ep.CenterID, side.ep.Host, side.ep.Port).Limit(1).Find(&endpoints).Error; err != nil {
			return false, err
		}
		if len(endpoints) > 0 {
			return false, nil
		}
		origin, err := repo.GetCenterOriginUUIDByUUID(ctx, l.db, id)
		if err != nil || origin.CenterID != side.ep.CenterID || (origin.Kind != model.OriginKindServer && origin.Kind != model.OriginKindManual) {
			return false, nil
		}
		if group := side.result.GetKeyParams()["group_replication_group_name"]; group != "" {
			groupID, err := uuidx.Parse(group)
			if err != nil || groupID.IsZero() {
				return false, nil
			}
			origin, err := repo.GetCenterOriginUUIDByUUID(ctx, l.db, groupID)
			if err != nil || origin.CenterID != side.ep.CenterID || origin.Kind == model.OriginKindSynthetic {
				return false, nil
			}
		}
	}
	for _, id := range []uuidx.UUID{p.SnapshotUUID, p.SchemaUUID} {
		if id.IsZero() {
			continue
		}
		if _, exists := seen[id]; exists {
			return false, nil
		}
		seen[id] = model.Endpoint{}
		origin, err := repo.GetCenterOriginUUIDByUUID(ctx, l.db, id)
		if err != nil || origin.CenterID != scope.sourceEndpoint.CenterID || origin.Kind != model.OriginKindSynthetic || origin.RefID != p.ID.String() {
			return false, nil
		}
	}
	return true, nil
}

func (l *InspectionLoop) managerPrecheckItems(ctx context.Context, p model.Pipeline, scope *pipelinePrecheckScope, source, target precheckSideOutcome) ([]model.PipelinePrecheck, error) {
	out := []model.PipelinePrecheck{}
	for _, req := range scope.required {
		if !strings.HasPrefix(req.Item, "manager/") {
			continue
		}
		code := strings.TrimPrefix(req.Item, "manager/")
		pass := false
		message := "缺少该检查所需的完整证据"
		var evidence any
		switch code {
		case "PRE-09":
			message = "尚未取得本管道的端到端联邦与队列绑定证明"
		case "PRE-10":
			var err error
			if source.err == nil && target.err == nil {
				pass, err = l.precheckIdentities(ctx, p, scope, source.result, target.result)
			}
			if err != nil {
				return nil, err
			}
			message = "实际实例身份必须与端点及全局来源目录一致，合成身份独立"
		case "PRE-11":
			se, sn, err := l.loadManualProbeTargets(ctx, p.SourceEndpointID, p.SourceNodeID)
			if err == nil {
				te, tn, e := l.loadManualProbeTargets(ctx, p.TargetEndpointID, p.TargetNodeID)
				pass = e == nil && se.CenterID == sn.CenterID && te.CenterID == tn.CenterID
			}
			message = "源与目标节点必须分别与自身端点同中心且在线"
		case "PRE-12":
			if source.err == nil && target.err == nil {
				pass, evidence = precheckCompatibleTables(scope, source.result, target.result)
			}
			message = "实际目标表定义必须与映射后的源表兼容，缺失表必须有完整迁移证明"
		case "PRE-13":
			if scope.migration != nil {
				pass, evidence = scope.migration.CapabilitiesVerified, scope.migration
			}
			message = "已确认迁移计划必须与实际对象、目标类型和字符集/排序规则能力一致"
		case "PRE-14":
			if scope.migration != nil {
				pass, evidence = scope.migration.DefinersVerified, scope.migration
			}
			message = "迁移 SQL 的明确 DEFINER 必须在目标存在，存储程序执行权限须可证明"
		case "PRE-20":
			analysis, err := repo.GetTopologyAnalysis(ctx, l.db, uuidx.MustParse(repo.AutomaticTopologyID))
			if err != nil {
				return nil, err
			}
			pass = !analysis.Incomplete && len(analysis.Warnings) == 0 && repo.CheckTopologyInitialization(ctx, l.db, uuidx.MustParse(repo.AutomaticTopologyID)) == nil
			evidence = analysis.Result
			message = "拓扑回环和重复到达需明确确认，初始化方向必须合法"
		case "PRE-22":
			pass, evidence = precheckClockComparison(source, target)
			message = "LWW 节点时钟偏差应不超过 1 秒，测量不确定时需确认"
		case "PRE-23":
			estimate := source.result.GetCapacity()
			pass, evidence = l.precheckBrokerCapacity(ctx, scope, estimate)
			message = "本次全量与增量积压估算应小于两中心 Broker 各节点扣除磁盘阈值后的余量，未知需确认"
		case "PRE-24":
			pass, evidence = precheckEnvironmentComparison(source.result, target.result)
			message = "源与目标字符集、排序规则、时区及 sql_mode 差异需确认"
		case "PRE-26":
			tables := scope.lwwFallbackTables()
			pass = len(tables) == 0
			evidence = struct {
				Tables []precheckLWWFallbackTable `json:"tables"`
			}{Tables: tables}
			message = "未配置 LWW 时间戳列的表回退源覆盖；延迟窗口内双写可能互相覆盖，需按表划分写入归属并明确确认"
		}
		out = append(out, precheckManagerItem(code, req.Level, pass, message, evidence))
	}
	return out, nil
}
