// POST /endpoints/:id/probe 接收手动探测并返回 taskId；自动周期探测仍由
// background_tasks.endpoint_probe_enabled 单独控制。库表浏览使用实时只读探测。
//
// 路由清单：
//
// GET/POST         /endpoints
//
//	GET/PATCH/DELETE /endpoints/:id
//	POST             /endpoints/:id/probe
//	GET              /endpoints/:id/catalog
package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// serverUuid/version/lowerCaseTableNames 直接透传空字符串，不转 *string：
// 三者都是 not null default 空串 的 varchar，前端 `string | null` 联合
// 类型本身就接受空字符串，跟 internal/manager/http/centers.go 的 Region/Description 处理方式
// 一致。
type endpointView struct {
	ID                  string          `json:"id"`
	Name                string          `json:"name"`
	CenterID            string          `json:"centerId"`
	Host                string          `json:"host"`
	Port                uint16          `json:"port"`
	Username            string          `json:"username"`
	TLS                 json.RawMessage `json:"tls"`
	Params              json.RawMessage `json:"params"`
	Version             string          `json:"version"`
	ServerUUID          string          `json:"serverUuid"`
	LowerCaseTableNames string          `json:"lowerCaseTableNames"`
	ProbeNodeID         *string         `json:"probeNodeId"`
	LastProbeAt         *time.Time      `json:"lastProbeAt"`
	LastProbeResult     json.RawMessage `json:"lastProbeResult"`
	CreatedAt           time.Time       `json:"createdAt"`
}

// endpointUpdateView 是 PUT /endpoints/:id 专用的响应体。
type endpointUpdateView struct {
	endpointView

	AffectedPipelines []affectedPipelineView `json:"affectedPipelines"`

	// AffectedUnknown 为真表示上面那个数组**不可信**（查询失败），不是"没有"。
	AffectedUnknown bool `json:"affectedUnknown,omitempty"`
}

// affectedPipelineView 是受影响列表的元素，刻意不复用 pipelineView：
// toPipelineView 需要 ctx/db/nodeHeartbeats，每条还要额外的库访问与心跳聚合，
// 而这里要的只是"叫什么、现在跑没跑、要不要重启"。下面四个字段全部直接取自
// ListPipelinesByEndpoint 已经查回来的那一行，零额外查询。
type affectedPipelineView struct {
	ID                string `json:"id"`
	Name              string `json:"name"`
	DesiredState      string `json:"desiredState"`
	SourceActualState string `json:"sourceActualState"`
	TargetActualState string `json:"targetActualState"`
}

func toAffectedPipelineViews(ps []model.Pipeline) []affectedPipelineView {
	views := make([]affectedPipelineView, 0, len(ps))
	for _, p := range ps {
		views = append(views, affectedPipelineView{
			ID:                p.ID.String(),
			Name:              p.Name,
			DesiredState:      string(p.DesiredState),
			SourceActualState: string(p.SourceActualState),
			TargetActualState: string(p.TargetActualState),
		})
	}
	return views
}

// toEndpointView 要求调用方传入已经脱敏（Redacted()）的端点，本函数自己
// 不做脱敏——脱敏这一步在调用点做，方便审计代码走原始（未脱敏）值是否需要
// 额外校验时不必先转换回去。当前调用点全部先 Redacted() 再传入。
func toEndpointView(d model.Endpoint) endpointView {
	var probeNodeID *string
	if !d.ProbeNodeID.IsZero() {
		s := d.ProbeNodeID.String()
		probeNodeID = &s
	}
	return endpointView{
		ID:                  d.ID.String(),
		Name:                d.Name,
		CenterID:            d.CenterID.String(),
		Host:                d.Host,
		Port:                d.Port,
		Username:            d.Username,
		TLS:                 json.RawMessage(d.TLS),
		Params:              json.RawMessage(d.Params),
		Version:             d.Version,
		ServerUUID:          d.ServerUUID,
		LowerCaseTableNames: d.LowerCaseTableNames,
		ProbeNodeID:         probeNodeID,
		LastProbeAt:         d.LastProbeAt,
		LastProbeResult:     json.RawMessage(d.LastProbeResult),
		CreatedAt:           d.CreatedAt,
	}
}

type endpointCreateRequest struct {
	Name     string          `json:"name" binding:"required"`
	CenterID string          `json:"centerId" binding:"required"`
	Host     string          `json:"host" binding:"required"`
	Port     uint16          `json:"port" binding:"required"`
	Username string          `json:"username" binding:"required"`
	Password string          `json:"password" binding:"required"`
	TLS      json.RawMessage `json:"tls"`
	Params   json.RawMessage `json:"params"`
}

// endpointUpdateRequest 对齐端点的更新输入：不接受 centerId，换中心是专门
// 流程（repo.UpdateEndpoint 既有限制）；不含 role——原因同上。
//
// Password 用 `!= nil` 判断是否覆盖，不做"空字符串视为不修改"的特殊处理
// ——真实前端表单 web/src/features/endpoints/EndpointFormModal.tsx 的密码留空时
// 整个 key 都不发，不会发空字符串；用 `!= nil` 与 internal/manager/http/centers.go 的
// MQMgmtPassword 同一套判断方式，行为对真实前端完全等价，且不会把未来
// 某个客户端显式传的 "" 静默吞掉。
//
// TLS/Params 用 json.RawMessage：key 未出现时是 nil（零值，不修改），
// key 出现时是原始 JSON 字节（整体替换）——前端当前的类型定义（可选、
// 不含 null 变体）不会显式传 null，不需要三态区分。
type endpointUpdateRequest struct {
	Name     *string         `json:"name"`
	Host     *string         `json:"host"`
	Port     *uint16         `json:"port"`
	Username *string         `json:"username"`
	Password *string         `json:"password"`
	TLS      json.RawMessage `json:"tls"`
	Params   json.RawMessage `json:"params"`
}

type endpointHandler struct {
	db     *gorm.DB
	audit  IAuditRecorder
	prober IEndpointProber
}

// IEndpointProber 是 HTTP 层需要的手动探测接收能力。
type IEndpointProber interface {
	SubmitEndpointProbe(ctx context.Context, endpointID, nodeID uuidx.UUID) (string, error)
	GetEndpointCatalog(ctx context.Context, endpointID uuidx.UUID) ([]task.CatalogEntry, error)
}

// registerEndpointRoutes 挂载端点管理与手动探测路由。
func registerEndpointRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder, prober IEndpointProber) {
	h := &endpointHandler{db: db, audit: audit, prober: prober}
	api.GET("/endpoints", h.list)
	api.POST("/endpoints", h.create)
	api.GET("/endpoints/:id", h.get)
	api.PATCH("/endpoints/:id", h.update)
	api.DELETE("/endpoints/:id", h.delete)
	api.POST("/endpoints/:id/probe", h.probe)
	api.GET("/endpoints/:id/catalog", h.catalog)
	api.GET("/endpoints/:id/definitions", h.definitions)
}

type iEndpointDefinitionsProber interface {
	GetEndpointDefinitions(context.Context, uuidx.UUID, *iproto.DefsProbeArgs) (*iproto.DefsResult, error)
}

func (h *endpointHandler) definitions(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	prober, ok := h.prober.(iEndpointDefinitionsProber)
	if !ok {
		Fail(c, http.StatusServiceUnavailable, "definitions_unavailable", "结构定义查询服务暂不可用")
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	schemas := c.QueryArray("schema")
	if len(schemas) == 0 || len(schemas) > 100 {
		Fail(c, http.StatusBadRequest, "invalid_scope", "请通过 schema 参数提供明确的业务库范围")
		return
	}
	result, err := prober.GetEndpointDefinitions(c.Request.Context(), id, &iproto.DefsProbeArgs{Schemas: schemas})
	switch {
	case errors.Is(err, repo.ErrEndpointNotFound):
		Fail(c, http.StatusNotFound, "not_found", "端点不存在")
	case errors.Is(err, task.ErrCatalogNotProbed):
		Fail(c, http.StatusConflict, "not_probed", "请先完成一次成功的端点连接探测")
	case errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, http.StatusConflict, "node_offline", "端点所属中心没有在线节点")
	case errors.Is(err, task.ErrProbeUnavailable):
		Fail(c, http.StatusServiceUnavailable, "definitions_unavailable", "结构定义查询服务暂不可用")
	case errors.Is(err, task.ErrDefinitionsTooLarge):
		Fail(c, http.StatusUnprocessableEntity, "definitions_too_large", "对象数量或定义文本超过查询上限，请缩小范围")
	case errors.Is(err, task.ErrDefinitionsFailed):
		Fail(c, http.StatusBadGateway, "definitions_failed", "节点未能完整读取对象定义")
	case err != nil:
		FailInternal(c, err, "definitions_failed")
	default:
		// Explicit HTTP view keeps protobuf field naming out of the public API.
		entries := make([]struct {
			Schema     string `json:"schema"`
			Name       string `json:"name"`
			Kind       string `json:"kind"`
			Definition string `json:"definition"`
		}, 0, len(result.GetEntries()))
		for _, entry := range result.GetEntries() {
			entries = append(entries, struct {
				Schema     string `json:"schema"`
				Name       string `json:"name"`
				Kind       string `json:"kind"`
				Definition string `json:"definition"`
			}{entry.GetSchema(), entry.GetObjectName(), entry.GetKind().String(), entry.GetDefinition()})
		}
		OK(c, entries)
	}
}

func (h *endpointHandler) catalog(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	if h.prober == nil {
		Fail(c, http.StatusServiceUnavailable, "catalog_unavailable", "目录查询服务暂不可用")
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	entries, err := h.prober.GetEndpointCatalog(c.Request.Context(), id)
	switch {
	case errors.Is(err, repo.ErrEndpointNotFound):
		Fail(c, http.StatusNotFound, "not_found", "端点不存在")
	case errors.Is(err, task.ErrCatalogNotProbed):
		Fail(c, http.StatusConflict, "not_probed", "请先完成一次成功的端点连接探测")
	case errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, http.StatusConflict, "node_offline", "端点所属中心没有在线节点")
	case errors.Is(err, task.ErrProbeUnavailable):
		Fail(c, http.StatusServiceUnavailable, "catalog_unavailable", "目录查询服务暂不可用")
	case errors.Is(err, task.ErrCatalogTooLarge):
		Fail(c, http.StatusUnprocessableEntity, "catalog_too_large", "可见库表或索引列超过目录浏览上限，请缩小端点账号可见范围")
	case errors.Is(err, task.ErrCatalogFailed):
		Fail(c, http.StatusBadGateway, "catalog_failed", "节点未能完成库表查询")
	case err != nil:
		FailInternal(c, err, "catalog_failed")
	default:
		OK(c, entries)
	}
}

func (h *endpointHandler) probe(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	if h.prober == nil {
		Fail(c, http.StatusServiceUnavailable, "probe_unavailable", "探测服务暂不可用")
		return
	}
	endpointID, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var request struct {
		NodeID string `json:"nodeId" binding:"required"`
	}
	if err := c.ShouldBindJSON(&request); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	nodeID, err := uuidx.Parse(request.NodeID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_node_id", "nodeId 格式不合法")
		return
	}
	taskID, err := h.prober.SubmitEndpointProbe(c.Request.Context(), endpointID, nodeID)
	switch {
	case errors.Is(err, repo.ErrEndpointNotFound):
		Fail(c, http.StatusNotFound, "not_found", "端点不存在")
	case errors.Is(err, repo.ErrNodeNotFound):
		Fail(c, http.StatusNotFound, "node_not_found", "探测节点不存在")
	case errors.Is(err, task.ErrProbeCrossCenter):
		Fail(c, http.StatusConflict, "cross_center_rejected", "探测节点必须与端点同中心")
	case errors.Is(err, task.ErrProbeNodeOffline):
		Fail(c, http.StatusConflict, "node_offline", "探测节点当前不在线")
	case errors.Is(err, task.ErrProbeUnavailable), errors.Is(err, task.ErrProbeQueueFull):
		Fail(c, http.StatusServiceUnavailable, "probe_unavailable", "探测服务暂不可用")
	case err != nil:
		FailInternal(c, err, "probe_dispatch_failed")
	default:
		event := baseAuditEvent(c)
		event.Action = model.AuditActionEndpointChange
		event.TargetType = model.AuditTargetTypeEndpoint
		event.TargetID = endpointID.String()
		h.audit.Record(c.Request.Context(), event)
		OKStatus(c, http.StatusAccepted, gin.H{"taskId": taskID})
	}
}

func (h *endpointHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	centerID := uuidx.Nil
	if v := c.Query("centerId"); v != "" {
		parsed, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_center_id", "centerId 格式不合法")
			return
		}
		centerID = parsed
	}

	des, err := repo.ListEndpoints(c.Request.Context(), h.db, centerID)
	if err != nil {
		FailInternal(c, err, "list_endpoints_failed")
		return
	}

	// keyword 在内存过滤（先取全量再过滤再分页）：端点规模不需要 DB 层
	// 过滤，跟 internal/manager/http/nodes.go 的 list 是同一个理由。这个形状不适用于 logs/events
	// 那种量级的端点（那边已经是 DB 层过滤）。
	keyword := strings.ToLower(strings.TrimSpace(c.Query("keyword")))
	views := make([]endpointView, 0, len(des))
	for _, d := range des {
		if keyword != "" &&
			!strings.Contains(strings.ToLower(d.Name), keyword) &&
			!strings.Contains(strings.ToLower(d.Host), keyword) {
			continue
		}
		views = append(views, toEndpointView(d.Redacted()))
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	total := len(views)
	start := min((page-1)*pageSize, total)
	end := min(start+pageSize, total)

	OK(c, paginated[endpointView]{
		Items:    views[start:end],
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}

func (h *endpointHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	d, err := repo.GetEndpoint(c.Request.Context(), h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrEndpointNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "端点不存在")
			return
		}
		FailInternal(c, err, "get_endpoint_failed")
		return
	}
	OK(c, toEndpointView(d.Redacted()))
}

func (h *endpointHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req endpointCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	centerID, err := uuidx.Parse(req.CenterID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_center_id", "centerId 格式不合法")
		return
	}

	ctx := c.Request.Context()
	if _, err := repo.GetCenter(ctx, h.db, centerID); err != nil {
		if errors.Is(err, repo.ErrCenterNotFound) {
			Fail(c, http.StatusBadRequest, "center_not_found", "centerId 对应的中心不存在")
			return
		}
		FailInternal(c, err, "get_center_failed")
		return
	}

	d := &model.Endpoint{
		Name:     req.Name,
		CenterID: centerID,
		Host:     req.Host,
		Port:     req.Port,
		Username: req.Username,
		Password: req.Password,
		TLS:      model.JSON(req.TLS),
		Params:   model.JSON(req.Params),
	}
	if err := repo.CreateEndpoint(ctx, h.db, d); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		FailInternal(c, err, "create_endpoint_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionEndpointChange
	event.TargetType = model.AuditTargetTypeEndpoint
	event.TargetID = d.ID.String()
	h.audit.Record(ctx, event)

	OK(c, toEndpointView(d.Redacted()))
}

func (h *endpointHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req endpointUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	ctx := c.Request.Context()
	current, err := repo.GetEndpoint(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrEndpointNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "端点不存在")
			return
		}
		FailInternal(c, err, "get_endpoint_failed")
		return
	}

	original := *current
	if req.Name != nil {
		current.Name = *req.Name
	}
	if req.Host != nil {
		current.Host = *req.Host
	}
	if req.Port != nil {
		current.Port = *req.Port
	}
	if req.Username != nil {
		current.Username = *req.Username
	}
	if req.Password != nil {
		current.Password = *req.Password
	}
	if len(req.TLS) > 0 {
		current.TLS = model.JSON(req.TLS)
	}
	if len(req.Params) > 0 {
		current.Params = model.JSON(req.Params)
	}

	if err := repo.UpdateEndpoint(ctx, h.db, current); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		switch {
		case errors.Is(err, repo.ErrEndpointNotFound):
			Fail(c, http.StatusNotFound, "not_found", "端点不存在")
		case errors.Is(err, repo.ErrEndpointVersionConflict):
			Fail(c, http.StatusConflict, "endpoint_version_conflict", "端点已被并发修改，请刷新后重试")
		default:
			FailInternal(c, err, "update_endpoint_failed")
		}
		return
	}

	updated, err := repo.GetEndpoint(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_endpoint_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionEndpointChange
	event.TargetType = model.AuditTargetTypeEndpoint
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	view := endpointUpdateView{
		endpointView:      toEndpointView(updated.Redacted()),
		AffectedPipelines: []affectedPipelineView{},
	}
	if current.Host == original.Host && current.Port == original.Port &&
		current.Username == original.Username && current.Password == original.Password &&
		equalEndpointJSON(current.TLS, original.TLS) && equalEndpointJSON(current.Params, original.Params) {
		OK(c, view)
		return
	}
	affected, err := repo.ListPipelinesByEndpoint(ctx, h.db, id)
	if err != nil {
		// 端点已写库成功，这里不能把请求变成 5xx，见 endpointUpdateView 的
		// AffectedUnknown 字段说明。
		slog.Error("endpoint: list affected pipelines failed",
			slog.String("endpoint_id", id.String()), slog.Any("error", err))
		view.AffectedUnknown = true
		view.AffectedPipelines = []affectedPipelineView{}
	} else {
		view.AffectedPipelines = toAffectedPipelineViews(affected)
	}

	OK(c, view)
}

func equalEndpointJSON(left, right model.JSON) bool {
	var a, b any
	if len(left) == 0 && len(right) == 0 {
		return true
	}
	if json.Unmarshal(left, &a) != nil || json.Unmarshal(right, &b) != nil {
		return reflect.DeepEqual(left, right)
	}
	return reflect.DeepEqual(a, b)
}

func (h *endpointHandler) delete(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if err := repo.DeleteEndpoint(c.Request.Context(), h.db, id); err != nil {
		switch {
		case errors.Is(err, repo.ErrEndpointNotFound):
			Fail(c, http.StatusNotFound, "not_found", "端点不存在")
		case errors.Is(err, repo.ErrEndpointHasDependents):
			Fail(c, http.StatusConflict, "endpoint_in_use", err.Error())
		default:
			FailInternal(c, err, "delete_endpoint_failed")
		}
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionEndpointChange
	event.TargetType = model.AuditTargetTypeEndpoint
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}
