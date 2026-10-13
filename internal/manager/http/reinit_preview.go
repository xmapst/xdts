package http

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
)

type reinitPreviewRequest struct {
	Schemas         []string                   `json:"schemas"`
	TargetHandling  model.ReinitTargetHandling `json:"targetHandling"`
	SchemaMigration bool                       `json:"schemaMigration"`
	DefinerFallback string                     `json:"definerFallback"`
}

type reinitPreviewError struct{ code, message string }

func (e *reinitPreviewError) Error() string { return e.code }

func reinitPreviewInvalid(code, message string) error {
	return &reinitPreviewError{code: code, message: message}
}

func validReinitSchema(name string) bool {
	if name == "" || !utf8.ValidString(name) || utf8.RuneCountInString(name) > 64 || strings.TrimSpace(name) != name || strings.ContainsRune(name, 0) {
		return false
	}
	switch strings.ToLower(name) {
	case "mysql", "sys", "information_schema", "performance_schema":
		return false
	}
	return true
}

func reinitObjectKey(schema, name string, sensitive bool) string {
	if !sensitive {
		schema, name = strings.ToLower(schema), strings.ToLower(name)
	}
	return schema + "\x00" + name
}

func (h *reinitHandler) preview(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	if _, err := currentReinitUser(c); err != nil {
		return
	}
	var input reinitPreviewRequest
	if c.ShouldBindJSON(&input) != nil || len(input.Schemas) == 0 || len(input.Schemas) > 100 || (input.TargetHandling != model.ReinitTargetKeep && input.TargetHandling != model.ReinitTargetClear) {
		Fail(c, http.StatusBadRequest, "invalid_scope", "请提供明确业务库范围和目标处理方式 keep 或 clear")
		return
	}
	seen := map[string]bool{}
	for _, schema := range input.Schemas {
		if !validReinitSchema(schema) || seen[schema] {
			Fail(c, http.StatusBadRequest, "invalid_scope", "业务库范围包含非法、系统或重复库名")
			return
		}
		seen[schema] = true
	}
	if !reinitStopped(p) {
		h.fail(c, repo.ErrReinitConflict)
		return
	}
	if !p.SchemaUUID.IsZero() && !input.SchemaMigration {
		Fail(c, http.StatusUnprocessableEntity, "schema_migration_required", "已有结构迁移的线路重新初始化必须生成新的结构计划")
		return
	}
	if !input.SchemaMigration && strings.TrimSpace(input.DefinerFallback) != "" {
		Fail(c, http.StatusBadRequest, "invalid_definer_account", "未启用结构迁移时不能指定 DEFINER")
		return
	}
	if h.prober == nil || h.defs == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 90*time.Second)
	defer cancel()
	doc, err := h.buildPreview(ctx, p, input)
	if err != nil {
		h.fail(c, err)
		return
	}
	stored, err := repo.NewReinitializations(h.db).SavePreview(ctx, p.ID, p.RunID, p.ConfigVersion, doc)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, viewReinit(stored))
}

func (h *reinitHandler) buildPreview(ctx context.Context, p *model.Pipeline, input reinitPreviewRequest) (repo.ReinitDocument, error) {
	var empty repo.ReinitDocument
	sourceEndpoint, err := repo.GetEndpoint(ctx, h.db, p.SourceEndpointID)
	if err != nil {
		return empty, err
	}
	targetEndpoint, err := repo.GetEndpoint(ctx, h.db, p.TargetEndpointID)
	if err != nil {
		return empty, err
	}
	caseSensitive := func(ep *model.Endpoint) (bool, error) {
		switch ep.LowerCaseTableNames {
		case "SENSITIVE":
			return true, nil
		case "LOWERCASE_STORED", "LOWERCASE_COMPARE":
			return false, nil
		default:
			return false, task.ErrCatalogNotProbed
		}
	}
	sourceSensitive, err := caseSensitive(sourceEndpoint)
	if err != nil {
		return empty, err
	}
	targetSensitive, err := caseSensitive(targetEndpoint)
	if err != nil {
		return empty, err
	}
	version, err := mysqlx.ParseVersion(targetEndpoint.Version)
	if err != nil {
		return empty, task.ErrCatalogNotProbed
	}
	strategy, err := repo.GetStrategyVersion(ctx, h.db, p.StrategyID, p.StrategyVersion)
	if err != nil {
		return empty, err
	}
	var replicaEndpoint *model.Endpoint
	if replicaID, enabled, err := repo.SnapshotReplicaEndpointID(strategy.Rules); err != nil {
		return empty, err
	} else if enabled {
		replicaEndpoint, err = repo.GetEndpoint(ctx, h.db, replicaID)
		if err != nil {
			return empty, err
		}
	}
	digest, err := repo.ReinitInputsDigest(p, sourceEndpoint, targetEndpoint, strategy.Rules, replicaEndpoint)
	if err != nil {
		return empty, err
	}
	rules, err := pipelinespec.ParseStrategyRules(strategy.Rules)
	if err != nil {
		return empty, reinitPreviewInvalid("invalid_strategy", "当前策略规则无法解析")
	}
	var filters []*iproto.ObjectFilterRule
	var mappings *iproto.MappingRules
	if rules.Source != nil {
		filters = rules.Source.ObjectFilters
	}
	if rules.Target != nil {
		mappings = rules.Target.Mappings
	}
	mapping, err := schemaplan.MappingFromProtocol(mappings)
	if err != nil {
		return empty, reinitPreviewInvalid("invalid_mapping", "策略映射规则不合法")
	}
	if rules.Source != nil && len(rules.Source.ColumnFilters) != 0 {
		mapping.ColumnFilter, err = iproto.CompileColumnFilters(rules.Source.ColumnFilters, sourceSensitive)
		if err != nil {
			return empty, reinitPreviewInvalid("invalid_filter", "策略列过滤规则不合法")
		}
	}
	catalog, err := h.prober.GetEndpointCatalog(ctx, p.SourceEndpointID)
	if err != nil {
		return empty, err
	}
	selected, err := selectReinitCatalog(catalog, filters, input.Schemas, sourceSensitive)
	if err != nil {
		return empty, err
	}
	sourceResult, err := h.defs.GetEndpointDefinitions(ctx, p.SourceEndpointID, &iproto.DefsProbeArgs{Schemas: input.Schemas})
	if err != nil {
		return empty, err
	}
	allSource, err := schemaplan.ProbeDefinitions(sourceResult)
	if err != nil {
		return empty, task.ErrDefinitionsFailed
	}
	source, err := schemaplan.SelectDefinitions(allSource, filters, sourceSensitive)
	if err != nil {
		return empty, reinitPreviewInvalid("invalid_filter", "策略对象过滤规则不合法")
	}
	// A table added or removed between catalog and definition reads changes the
	// confirmation scope. Retry the entire preview instead of accepting a subset.
	tableDefs := []schemaplan.Definition{}
	for _, def := range source {
		if def.Kind == model.ObjectTypeTable {
			usable, _, err := schemamig.InitializationTableProperties(def.DDL)
			if err != nil || !usable {
				return empty, reinitPreviewInvalid("snapshot_key_required", "待同步源表没有完整非空主键或唯一键，无法执行全量")
			}
			key := reinitObjectKey(def.Schema, def.Name, sourceSensitive)
			if !selected[key] {
				return empty, reinitPreviewInvalid("source_scope_changed", "源端表范围在探测期间变化，请重新预览")
			}
			delete(selected, key)
			tableDefs = append(tableDefs, def)
		}
	}
	if len(selected) != 0 {
		return empty, reinitPreviewInvalid("source_scope_changed", "源端表范围在探测期间变化，请重新预览")
	}
	if !input.SchemaMigration {
		source = tableDefs
	}
	fallback := strings.TrimSpace(input.DefinerFallback)
	accounts, err := schemaplan.DefinitionAccounts(source, fallback)
	if err != nil {
		return empty, reinitPreviewInvalid("invalid_definer_accounts", "DEFINER 账号不合法或超过查询上限")
	}
	targetSchemas := schemaplan.TargetSchemas(source, mapping, sourceSensitive)
	for _, schema := range targetSchemas {
		if !validReinitSchema(schema) {
			return empty, reinitPreviewInvalid("invalid_mapping", "目标映射包含系统库或非法库名")
		}
	}
	targetResult, err := h.defs.GetEndpointDefinitions(ctx, p.TargetEndpointID, &iproto.DefsProbeArgs{Schemas: targetSchemas, AllowMissingSchemas: true, Accounts: accounts})
	if err != nil {
		return empty, err
	}
	target, err := schemaplan.ProbeDefinitions(targetResult)
	if err != nil {
		return empty, task.ErrDefinitionsFailed
	}
	verified, err := schemaplan.VerifiedDefinerAccounts(targetResult, accounts)
	if err != nil {
		return empty, task.ErrDefinitionsFailed
	}
	fallback, err = reinitDefinerFallback(fallback, targetResult, verified)
	if err != nil {
		return empty, err
	}
	caps := mysqlx.DetectCaps(version)
	plan, err := schemaplan.Build(schemaplan.Request{Source: source, Target: target, Mapping: mapping, TargetCaseSensitive: targetSensitive, DefinerAccounts: verified, Options: schemamig.Options{SourceCaseSensitive: sourceSensitive, TargetInvisibleColumn: caps.InvisibleColumn, TargetVectorType: caps.VectorType, DefinerFallback: fallback}})
	if err != nil {
		return empty, reinitPreviewInvalid("invalid_definitions", "源或目标定义身份、映射不合法")
	}
	plan.ApplyEnvironmentChecks(targetResult)
	plan.ApplyDefinerExecuteChecks(targetResult)
	// Use node-returned spelling so a case-insensitive request still records
	// the exact source identities used by the plan and snapshot scope.
	scope := map[string]bool{}
	for _, def := range source {
		scope[def.Schema] = true
	}
	schemas := make([]string, 0, len(scope))
	for schema := range scope {
		schemas = append(schemas, schema)
	}
	sort.Strings(schemas)
	doc := repo.ReinitDocument{InputsDigest: digest, Schemas: schemas, TargetHandling: input.TargetHandling, TargetCaseSensitive: targetSensitive}
	if input.SchemaMigration {
		doc.SchemaPlan = &plan
	}
	actualTables := map[string]schemaplan.Definition{}
	actualViews := map[string]bool{}
	for _, def := range target {
		if def.Kind == model.ObjectTypeView {
			actualViews[reinitObjectKey(def.Schema, def.Name, targetSensitive)] = true
		}
		if def.Kind == model.ObjectTypeTable {
			actualTables[reinitObjectKey(def.Schema, def.Name, targetSensitive)] = def
		}
	}
	for _, step := range plan.Steps {
		if step.Kind != model.ObjectTypeTable {
			continue
		}
		actual, exists := actualTables[reinitObjectKey(step.Schema, step.Name, targetSensitive)]
		if actualViews[reinitObjectKey(step.Schema, step.Name, targetSensitive)] {
			return empty, reinitPreviewInvalid("reinit_blocked", "目标同名对象是视图，不能作为全量写入目标表")
		}
		if !exists && !input.SchemaMigration {
			return empty, reinitPreviewInvalid("schema_migration_required", "目标表缺失，请启用结构迁移或先建立兼容表")
		}
		if step.Action == model.MigrationActionBlock {
			return empty, reinitPreviewInvalid("reinit_blocked", step.BlockReason)
		}
		if input.TargetHandling == model.ReinitTargetClear && exists {
			_, engine, err := schemamig.InitializationTableProperties(actual.DDL)
			if err != nil || !strings.EqualFold(engine, "InnoDB") {
				return empty, reinitPreviewInvalid("transactional_target_required", "清表要求目标表使用 InnoDB")
			}
		}
		doc.Tables = append(doc.Tables, repo.ReinitTable{SourceSchema: step.SourceSchema, SourceTable: step.SourceName, SourceDDL: step.SourceDDL, Schema: step.Schema, Table: step.Name, ExpectedDDL: actual.DDL})
	}
	if input.TargetHandling == model.ReinitTargetClear {
		if err := validateReinitTriggers(doc.Tables, target, targetResult.GetGrants(), targetSensitive); err != nil {
			return empty, err
		}
	}
	return doc, nil
}

func selectReinitCatalog(catalog []task.CatalogEntry, filters []*iproto.ObjectFilterRule, schemas []string, sensitive bool) (map[string]bool, error) {
	scope := map[string]bool{}
	for _, schema := range schemas {
		key := reinitObjectKey(schema, "", sensitive)
		if scope[key] {
			return nil, reinitPreviewInvalid("invalid_scope", "业务库范围存在大小写重复")
		}
		scope[key] = true
	}
	defs := make([]schemaplan.Definition, 0, len(catalog))
	keys := map[string]bool{}
	for _, table := range catalog {
		key := reinitObjectKey(table.Schema, table.Table, sensitive)
		if !validReinitSchema(table.Schema) || table.Table == "" || keys[key] {
			return nil, task.ErrCatalogFailed
		}
		keys[key] = true
		defs = append(defs, schemaplan.Definition{Schema: table.Schema, Name: table.Table, Kind: model.ObjectTypeTable})
	}
	selected, err := schemaplan.SelectDefinitions(defs, filters, sensitive)
	if err != nil {
		return nil, reinitPreviewInvalid("invalid_filter", "策略对象过滤规则不合法")
	}
	if len(selected) == 0 {
		return nil, reinitPreviewInvalid("empty_scope", "当前策略未选中任何源表")
	}
	out := map[string]bool{}
	for _, table := range selected {
		if !scope[reinitObjectKey(table.Schema, "", sensitive)] {
			return nil, reinitPreviewInvalid("incomplete_scope", "所选业务库未覆盖当前策略的全部待同步表")
		}
		out[reinitObjectKey(table.Schema, table.Name, sensitive)] = true
	}
	for _, table := range catalog {
		if out[reinitObjectKey(table.Schema, table.Table, sensitive)] && !table.HasPrimaryKey && !table.HasUniqueKey {
			return nil, reinitPreviewInvalid("snapshot_key_required", "待同步源表没有主键或唯一键，无法执行全量")
		}
	}
	return out, nil
}

func reinitDefinerFallback(fallback string, target *iproto.DefsResult, verified map[string]bool) (string, error) {
	if fallback == "" {
		return "", nil
	}
	a, err := schemamig.ParseDefinerAccount(fallback)
	if err != nil {
		return "", reinitPreviewInvalid("invalid_definer_account", "兜底 DEFINER 账号不合法")
	}
	if a.CurrentUser {
		a, err = schemamig.ParseDefinerAccount(target.GetCurrentUser())
		if err != nil {
			return "", task.ErrDefinitionsFailed
		}
	} else if !verified[a.Key()] {
		return "", reinitPreviewInvalid("definer_precheck_required", "无法确认兜底 DEFINER 账号存在")
	}
	return a.SQL(), nil
}

func validateReinitTriggers(tables []repo.ReinitTable, target []schemaplan.Definition, grants []string, sensitive bool) error {
	selected := map[string]bool{}
	for _, table := range tables {
		if table.ExpectedDDL == "" {
			continue
		}
		if !schemamig.GrantsTableTrigger(grants, table.Schema, table.Table) {
			return reinitPreviewInvalid("trigger_visibility_required", "清表需要完整读取目标表触发器，请 DBA 授予 TRIGGER 权限")
		}
		selected[reinitObjectKey(table.Schema, table.Table, sensitive)] = true
	}
	for _, def := range target {
		if def.Kind != model.ObjectTypeTrigger {
			continue
		}
		refs, known := schemamig.DDLObjects(def.Schema, def.DDL)
		if !known || len(refs) != 1 {
			return task.ErrDefinitionsFailed
		}
		if !selected[reinitObjectKey(refs[0].Schema, refs[0].Table, sensitive)] {
			continue
		}
		guarded, err := schemamig.TriggerSuppressesApply(def.DDL)
		if err != nil || !guarded {
			return reinitPreviewInvalid("trigger_guard_required", "选中目标表存在未完整守卫的触发器，请先由 DBA 处理")
		}
	}
	return nil
}
