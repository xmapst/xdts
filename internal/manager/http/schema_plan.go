package http

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type schemaPlanHandler struct {
	db         *gorm.DB
	prober     iEndpointDefinitionsProber
	audit      IAuditRecorder
	dispatcher dispatch.IPipelineDispatcher
}
type schemaPlanView struct {
	PipelineID    string          `json:"pipelineId"`
	RunID         uint64          `json:"runId"`
	ConfigVersion uint64          `json:"configVersion"`
	Digest        string          `json:"digest"`
	ConfirmedAt   *time.Time      `json:"confirmedAt"`
	Plan          schemaplan.Plan `json:"plan"`
}

func viewSchemaPlan(stored *repo.StoredSchemaPlan) schemaPlanView {
	return schemaPlanView{PipelineID: stored.Record.PipelineID.String(), RunID: stored.Record.RunID, ConfigVersion: stored.Record.ConfigVersion, Digest: stored.Record.Digest, ConfirmedAt: stored.Record.ConfirmedAt, Plan: stored.Plan}
}
func registerSchemaPlanRoutes(api *gin.RouterGroup, db *gorm.DB, prober IEndpointProber, audit IAuditRecorder, dispatcher dispatch.IPipelineDispatcher) {
	definitions, _ := prober.(iEndpointDefinitionsProber)
	h := &schemaPlanHandler{db: db, prober: definitions, audit: audit, dispatcher: dispatcher}
	api.POST("/pipelines/:id/schema-plan/preview", RequirePipelineScope(db, true), h.preview)
	api.GET("/pipelines/:id/schema-plan", RequirePipelineScope(db, false), h.get)
	api.GET("/pipelines/:id/schema-plan/ddl", RequirePipelineScope(db, false), h.export)
	api.POST("/pipelines/:id/schema-plan/confirm", RequirePipelineScope(db, true), h.confirm)
}
func (h *schemaPlanHandler) pipeline(c *gin.Context) *model.Pipeline {
	if !requireDB(c, h.db) {
		return nil
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return nil
	}
	p, err := repo.GetPipeline(c.Request.Context(), h.db, id)
	if err != nil {
		h.fail(c, err)
		return nil
	}
	return p
}
func (h *schemaPlanHandler) fail(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repo.ErrPipelineNotFound):
		Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
	case errors.Is(err, repo.ErrSchemaPlanNotFound):
		Fail(c, http.StatusNotFound, "plan_not_found", "本次运行尚无结构迁移计划")
	case errors.Is(err, repo.ErrSchemaPlanConflict):
		Fail(c, http.StatusConflict, "plan_changed", "计划或配置已变化，请重新预览；已确认计划不可替换")
	case errors.Is(err, topologycheck.ErrBidirectionalInitialization):
		Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "双向初始化只能由一个方向执行全量或结构迁移，反方向需使用仅增量：当前最新")
	case errors.Is(err, repo.ErrSchemaPlanBlocked):
		Fail(c, http.StatusUnprocessableEntity, "plan_blocked", "计划存在阻断项，请先处理差异")
	case errors.Is(err, task.ErrProbeUnavailable):
		Fail(c, http.StatusServiceUnavailable, "probe_unavailable", "结构定义查询服务暂不可用")
	case errors.Is(err, task.ErrCatalogNotProbed):
		Fail(c, http.StatusConflict, "not_probed", "请先完成源和目标的端点连接探测")
	case errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, http.StatusConflict, "node_offline", "端点所属中心没有在线节点")
	case errors.Is(err, task.ErrDefinitionsTooLarge):
		Fail(c, http.StatusUnprocessableEntity, "definitions_too_large", "对象数量或定义文本超过上限，请缩小范围")
	case errors.Is(err, task.ErrDefinitionsFailed):
		Fail(c, http.StatusBadGateway, "definitions_failed", "节点未能完整读取对象定义")
	default:
		FailInternal(c, err, "schema_plan_failed")
	}
}

func (h *schemaPlanHandler) preview(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	if h.prober == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	var request struct {
		Schemas         []string `json:"schemas"`
		DefinerFallback string   `json:"definerFallback"`
	}
	if err := c.ShouldBindJSON(&request); err != nil || len(request.Schemas) == 0 || len(request.Schemas) > 100 {
		Fail(c, http.StatusBadRequest, "invalid_scope", "请提供明确的业务库范围")
		return
	}
	if p.DesiredState != model.DesiredStateStopped || !p.SchemaUUID.IsZero() {
		h.fail(c, repo.ErrSchemaPlanConflict)
		return
	}
	ctx := c.Request.Context()
	sourceEndpoint, err := repo.GetEndpoint(ctx, h.db, p.SourceEndpointID)
	if err != nil {
		h.fail(c, err)
		return
	}
	targetEndpoint, err := repo.GetEndpoint(ctx, h.db, p.TargetEndpointID)
	if err != nil {
		h.fail(c, err)
		return
	}
	sourceSensitive := sourceEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()
	targetSensitive := targetEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()
	if sourceEndpoint.LowerCaseTableNames == "" || targetEndpoint.LowerCaseTableNames == "" {
		h.fail(c, task.ErrCatalogNotProbed)
		return
	}
	version, err := mysqlx.ParseVersion(targetEndpoint.Version)
	if err != nil {
		h.fail(c, task.ErrCatalogNotProbed)
		return
	}
	caps := mysqlx.DetectCaps(version)
	strategy, err := repo.GetStrategyVersion(ctx, h.db, p.StrategyID, p.StrategyVersion)
	if err != nil {
		h.fail(c, err)
		return
	}
	rules, err := pipelinespec.ParseStrategyRules(strategy.Rules)
	if err != nil {
		h.fail(c, err)
		return
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
		Fail(c, http.StatusUnprocessableEntity, "invalid_mapping", "策略映射规则不合法")
		return
	}
	if rules.Source != nil && len(rules.Source.ColumnFilters) != 0 {
		mapping.ColumnFilter, err = iproto.CompileColumnFilters(rules.Source.ColumnFilters, sourceSensitive)
		if err != nil {
			Fail(c, http.StatusUnprocessableEntity, "invalid_filter", "策略列过滤规则不合法")
			return
		}
	}
	sourceResult, err := h.prober.GetEndpointDefinitions(ctx, p.SourceEndpointID, &iproto.DefsProbeArgs{Schemas: request.Schemas})
	if err != nil {
		h.fail(c, err)
		return
	}
	all, err := schemaplan.ProbeDefinitions(sourceResult)
	if err != nil {
		h.fail(c, err)
		return
	}
	source, err := schemaplan.SelectDefinitions(all, filters, sourceSensitive)
	if err != nil {
		Fail(c, http.StatusUnprocessableEntity, "invalid_filter", "策略对象过滤规则不合法")
		return
	}
	if len(source) == 0 {
		Fail(c, http.StatusUnprocessableEntity, "empty_scope", "策略过滤后没有可迁移的对象")
		return
	}
	fallback := strings.TrimSpace(request.DefinerFallback)
	accounts, err := schemaplan.DefinitionAccounts(source, fallback)
	if err != nil {
		Fail(c, http.StatusUnprocessableEntity, "invalid_definer_accounts", "DEFINER 账号定义不合法或数量超过上限")
		return
	}
	targetResult, err := h.prober.GetEndpointDefinitions(ctx, p.TargetEndpointID, &iproto.DefsProbeArgs{Schemas: schemaplan.TargetSchemas(source, mapping, sourceSensitive), AllowMissingSchemas: true, Accounts: accounts})
	if err != nil {
		h.fail(c, err)
		return
	}
	target, err := schemaplan.ProbeDefinitions(targetResult)
	if err != nil {
		h.fail(c, err)
		return
	}
	verified, err := schemaplan.VerifiedDefinerAccounts(targetResult, accounts)
	if err != nil {
		h.fail(c, task.ErrDefinitionsFailed)
		return
	}
	if fallback != "" {
		account, err := schemamig.ParseDefinerAccount(fallback)
		if err != nil {
			Fail(c, http.StatusUnprocessableEntity, "invalid_definer_account", "兜底 DEFINER 账号不合法")
			return
		}
		if account.CurrentUser {
			account, err = schemamig.ParseDefinerAccount(targetResult.GetCurrentUser())
			if err != nil {
				h.fail(c, task.ErrDefinitionsFailed)
				return
			}
		} else if !verified[account.Key()] {
			Fail(c, http.StatusUnprocessableEntity, "definer_precheck_required", "无法确认兜底 DEFINER 账号存在，请检查账号与探测权限")
			return
		}
		fallback = account.SQL()
	}
	plan, err := schemaplan.Build(schemaplan.Request{Source: source, Target: target, Mapping: mapping, TargetCaseSensitive: targetSensitive, DefinerAccounts: verified, Options: schemamig.Options{SourceCaseSensitive: sourceSensitive, TargetInvisibleColumn: caps.InvisibleColumn, TargetVectorType: caps.VectorType, DefinerFallback: fallback}})
	if err != nil {
		Fail(c, http.StatusUnprocessableEntity, "invalid_definitions", "对象定义身份或映射不合法")
		return
	}
	plan.ApplyEnvironmentChecks(targetResult)
	plan.ApplyDefinerExecuteChecks(targetResult)
	stored, err := repo.SaveSchemaPlan(ctx, h.db, p.ID, p.RunID, p.ConfigVersion, plan)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, viewSchemaPlan(stored))
}
func (h *schemaPlanHandler) get(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	stored, err := repo.GetSchemaPlan(c.Request.Context(), h.db, p.ID, p.RunID)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, viewSchemaPlan(stored))
}
func (h *schemaPlanHandler) export(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	stored, err := repo.GetSchemaPlan(c.Request.Context(), h.db, p.ID, p.RunID)
	if err != nil {
		h.fail(c, err)
		return
	}
	ddl, err := stored.Plan.ExportDDL()
	if err != nil {
		h.fail(c, repo.ErrSchemaPlanBlocked)
		return
	}
	c.Header("Content-Disposition", `attachment; filename="schema-migration.sql"`)
	c.Data(http.StatusOK, "application/sql; charset=utf-8", []byte(ddl))
}
func (h *schemaPlanHandler) confirm(c *gin.Context) {
	p := h.pipeline(c)
	if p == nil {
		return
	}
	var request struct {
		Digest  string `json:"digest"`
		Confirm bool   `json:"confirm"`
	}
	if err := c.ShouldBindJSON(&request); err != nil || !request.Confirm {
		Fail(c, http.StatusBadRequest, "confirmation_required", "请明确确认当前摘要对应的迁移计划")
		return
	}
	userID, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		Fail(c, http.StatusUnauthorized, "unauthorized", "登录会话无效")
		return
	}
	stored, err := repo.ConfirmSchemaPlan(c.Request.Context(), h.db, p.ID, p.RunID, request.Digest, userID)
	if err != nil {
		h.fail(c, err)
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionSchemaPlanConfirm
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = p.ID.String()
	event.Changes, _ = json.Marshal(map[string]any{"digest": stored.Record.Digest, "runId": stored.Record.RunID})
	h.audit.Record(c.Request.Context(), event)
	syncPipelineBestEffort(c.Request.Context(), h.dispatcher, p.ID)
	OK(c, viewSchemaPlan(stored))
}
