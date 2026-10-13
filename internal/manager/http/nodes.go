package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// GET /nodes 的分页：repo.ListNodes 本身不分页（数据集规模上不需要），
// 分页在这一层对结果切片实现，参数缺省/上限用 internal/manager/http/pagination.go 里全部资源
// 列表共用的常量。

// nodeRuntimeView 对齐 web/src/api/nodes.ts 的 NodeRuntimeInfo。
type nodeRuntimeView struct {
	CPUPercent *float64 `json:"cpuPercent"`

	// MemPercent 是内存占用百分比（0~100），**原样透传节点上报值，本层不算**。
	//
	// 节点的 sampleMemory() 同源返回 mem_percent / mem_rss_bytes /
	// mem_total_bytes 三项，分母的选择（有 cgroup 上限取该上限、否则取宿主机
	// MemTotal）只有节点做得了。
	//
	// **本层刻意不做"节点没报就用 rss/total 算"的回退**：这三项一并暴露的
	// 用意是让消费方能验算 mem_percent ≈ rss/total×100。一旦本层在某些情况
	// 下自己算出 memPercent，那个验算在**恰好是它派上用场的那一格**变成恒真
	// ——消费方无从分辨手里这个数是节点报的还是本层导出的，而两者正是验算
	// 要区分的对象。节点没报就是 null，消费方要推自己推。
	MemPercent *float64 `json:"memPercent"`

	// 与 MemBytes 不可互推：那是 runtime.MemStats.Sys（Go 向 OS 取得的
	// 地址空间总量），与 RSS 可以相差数倍且方向不固定。本视图今天不暴露
	// MemBytes，所以界面上不会出现这两个对不上的数并排显示。
	MemRssBytes   *uint64 `json:"memRssBytes"`
	MemTotalBytes *uint64 `json:"memTotalBytes"`

	StateDBSizeBytes *int64 `json:"stateDbSizeBytes"`

	// PipelineCount 是**三态**，所以是指针：
	//
	//	null → 节点尚未上报承载镜像，或该节点从未心跳
	//	0    → 上报了，承载 0 条
	//	2    → 承载 2 条
	//
	// 非指针的 int 没有能力表达"不知道"，会把一次契约漂移渲染成"0 条线路"
	// 这条可信的业务事实。model.NodeRuntime.Pipelines
	// 在结构上保住了 nil 与 [] 的区分，**这里一个 len(nil)==0 就能把它抹平**，
	// 所以判据是 `Pipelines != nil` 而不是 `len(Pipelines) > 0`。
	PipelineCount *int `json:"pipelineCount"`

	// UptimeSeconds 是节点进程已运行的秒数，原样透传
	// model.NodeRuntime.UptimeSeconds（顶层字段，**不在 resource 里**）。
	//
	// Manager 算不出它：本侧只能算"我第一次见到这个节点至今"，那是另一个
	// 语义，且 Manager 一重启就归零。节点没填就是 null。
	UptimeSeconds *int64 `json:"uptimeSeconds"`
}

func decodeNodeRuntime(raw model.JSON, nodeID uuidx.UUID) (nodeRuntimeView, error) {
	rt, err := model.DecodeNodeRuntime(raw)
	if err != nil {
		return nodeRuntimeView{}, err
	}
	if rt == nil {
		return nodeRuntimeView{}, nil
	}

	var v nodeRuntimeView
	if rt.Resource != nil {
		cpu := rt.Resource.CPUPercent
		v.CPUPercent = &cpu
		// 三项都是 *T，缺席即 nil 即 null。**逐个显式判 nil 复制值，
		// 不用非指针字段承接**：这四个字段的 0 都是合法观测值（占用 0%、
		// RSS 0、进程刚启动 0 秒），非指针会把"节点没报"渲染成"占用 0%"
		// ——一条不报错的错值。
		if r := rt.Resource.MemPercent; r != nil {
			c := *r
			v.MemPercent = &c
		}
		if r := rt.Resource.MemRssBytes; r != nil {
			c := *r
			v.MemRssBytes = &c
		}
		if r := rt.Resource.MemTotalBytes; r != nil {
			c := *r
			v.MemTotalBytes = &c
		}
	}
	if rt.UptimeSeconds != nil {
		up := *rt.UptimeSeconds
		v.UptimeSeconds = &up
	}
	if rt.LocalStore != nil {
		// uint64 → int64：本地状态库大小要超过 8 EiB 才会绕回负数，
		// 不设钳制（钳制分支无法被测试覆盖，等于加一段没人验过的代码）。
		size := int64(rt.LocalStore.StoreSizeBytes)
		v.StateDBSizeBytes = &size
	}
	v.PipelineCount = pipelineCountOf(rt, nodeID)
	return v, nil
}

type nodeSummaryView struct {
	ID              string     `json:"id"`
	Name            string     `json:"name"`
	CenterID        string     `json:"centerId"`
	Status          string     `json:"status"`
	HasPipelines    bool       `json:"hasPipelines"`
	Version         string     `json:"version"`
	Hostname        string     `json:"hostname"`
	Labels          []string   `json:"labels"`
	MaxPipelines    uint32     `json:"maxPipelines"`
	LastHeartbeatAt *time.Time `json:"lastHeartbeatAt"`
	RegisteredAt    time.Time  `json:"registeredAt"`
}

// nodePipelineRefView 对齐 web/src/api/nodes.ts 的 NodePipelineRef。
//
// 一个节点可能同时是同一条 pipeline 的源端与目标端（dispatch 的
// dispatchCommand 去重逻辑确认这种拓扑存在）。此时 role 只能显示一个
// 值，沿用既有的"target 优先"判断，actualState 跟着 role 走，两者始终
// 取自同一侧，不会出现"role 写 source、状态取 target"的错配。
type nodePipelineRefView struct {
	PipelineID   string `json:"pipelineId"`
	PipelineName string `json:"pipelineName"`
	Role         string `json:"role"`
	// SideActualState 只描述 Role 那一侧，见上方说明。
	SideActualState string `json:"sideActualState"`
}

// nodeDetailView 对齐 NodeDetail。
type nodeDetailView struct {
	nodeSummaryView
	IPs                 []string              `json:"ips"`
	OSArch              string                `json:"osArch"`
	DesiredLogLevel     string                `json:"desiredLogLevel"`
	DesiredLogShipLevel string                `json:"desiredLogShipLevel"`
	ActualLogLevel      string                `json:"actualLogLevel"`
	ActualLogShipLevel  string                `json:"actualLogShipLevel"`
	Runtime             nodeRuntimeView       `json:"runtime"`
	Pipelines           []nodePipelineRefView `json:"pipelines"`
}

const nodeLogLevelActualUnknown = "unknown"

func splitIPs(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return []string{}
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func toNodeSummaryView(n model.Node) (nodeSummaryView, error) {
	labels, err := decodeLabels(n.Labels)
	if err != nil {
		return nodeSummaryView{}, err
	}
	return nodeSummaryView{
		ID:              n.ID.String(),
		Name:            n.Name,
		CenterID:        n.CenterID.String(),
		Status:          string(n.Status),
		Version:         n.Version,
		Hostname:        n.Hostname,
		Labels:          labels,
		MaxPipelines:    n.MaxPipelines,
		LastHeartbeatAt: n.LastHeartbeatAt,
		RegisteredAt:    n.RegisteredAt,
	}, nil
}

// toNodeDetailView 组装节点详情。pipelines 由调用方查好传入（
// repo.ListPipelinesByNode）——转换函数本身不做数据库访问，保持和
// toNodeSummaryView 一样是纯函数，方便测试。
func toNodeDetailView(n model.Node, pipelines []model.Pipeline) (nodeDetailView, error) {
	summary, err := toNodeSummaryView(n)
	if err != nil {
		return nodeDetailView{}, err
	}
	runtime, err := decodeNodeRuntime(n.Runtime, n.ID)
	if err != nil {
		return nodeDetailView{}, err
	}
	// 一条 pipeline 在本节点上可能占**两个**角色：自环（源和目标是同一个
	// 节点）是受支持的真实场景，internal/manager/dispatch/ctl_config_pull.go 专门处理它。
	//
	// 同一节点可同时承载源端与目标端，按匹配的侧分别生成引用。
	pipelineRefs := make([]nodePipelineRefView, 0, len(pipelines))
	for _, p := range pipelines {
		type sideRef struct {
			side  model.Side
			state model.ActualState
		}
		sides := make([]sideRef, 0, 2)
		if p.SourceNodeID == n.ID {
			sides = append(sides, sideRef{model.SideSource, p.SourceActualState})
		}
		if p.TargetNodeID == n.ID {
			sides = append(sides, sideRef{model.SideTarget, p.TargetActualState})
		}
		if len(sides) == 0 {
			continue
		}
		for _, sr := range sides {
			pipelineRefs = append(pipelineRefs, nodePipelineRefView{
				PipelineID:      p.ID.String(),
				PipelineName:    p.Name,
				Role:            string(sr.side),
				SideActualState: string(sr.state),
			})
		}
	}
	return nodeDetailView{
		nodeSummaryView:     summary,
		IPs:                 splitIPs(n.IPs),
		OSArch:              n.OSArch,
		DesiredLogLevel:     string(n.LogLevel),
		DesiredLogShipLevel: string(n.LogShipLevel),
		ActualLogLevel:      nodeLogLevelActualUnknown,
		ActualLogShipLevel:  nodeLogLevelActualUnknown,
		Runtime:             runtime,
		Pipelines:           pipelineRefs,
	}, nil
}

// nodeUpdateRequest 对齐 web/src/api/nodes.ts 的 NodeUpdateInput：禁用与
// 启用共用 disabled 布尔（「PATCH /nodes/{id}」一行），不单开
// enable 端点。
type nodeUpdateRequest struct {
	Name         *string   `json:"name"`
	Labels       *[]string `json:"labels"`
	Disabled     *bool     `json:"disabled"`
	MaxPipelines *uint32   `json:"maxPipelines"`
}

// nodeLogLevelRequest 对齐 web/src/api/nodes.ts 的 NodeLogLevelInput：
// 两个级别都必填（不是 PATCH 语义的部分更新），与「POST
// /nodes/{id}/log-level | body {logLevel, logShipLevel}」一致。
type nodeLogLevelRequest struct {
	LogLevel     model.LogLevel `json:"logLevel" binding:"required"`
	LogShipLevel model.LogLevel `json:"logShipLevel" binding:"required"`
}

type nodeHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
}

// registerNodeRoutes 挂载节点资源路由。
func registerNodeRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder) {
	h := &nodeHandler{db: db, audit: audit}
	api.GET("/nodes", h.list)
	api.GET("/nodes/:id", h.get)
	api.PATCH("/nodes/:id", h.update)
	api.POST("/nodes/:id/revoke", h.revoke)
	api.POST("/nodes/:id/log-level", h.logLevel)
	api.DELETE("/nodes/:id", h.deregister)
}

func (h *nodeHandler) list(c *gin.Context) {
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

	nodes, err := repo.ListNodes(c.Request.Context(), h.db, centerID)
	if err != nil {
		FailInternal(c, err, "list_nodes_failed")
		return
	}

	status := strings.TrimSpace(c.Query("status"))
	keyword := strings.ToLower(strings.TrimSpace(c.Query("keyword")))

	views := make([]nodeSummaryView, 0, len(nodes))
	for _, n := range nodes {
		if status != "" && string(n.Status) != status {
			continue
		}
		// `n.Hostname` 这一半今天恒不命中：`model.Node.Hostname` 在生产上
		// 永远是空串（列默认值），所以 `strings.Contains("", keyword)` 对任何
		// 非空 keyword 都是 false，本条件退化成"只按 Name 搜"。
		//
		// 写 Hostname 的三个入口里唯一有生产调用方的是 `repo.UpdateNode`
		// （本文件 PATCH），而那个 handler 只改 Name/Labels、从不给 Hostname
		// 赋值，把读出来的值原样写回 ⇒ 空串进、空串出，无法由此变成非空。
		if keyword != "" &&
			!strings.Contains(strings.ToLower(n.Name), keyword) &&
			!strings.Contains(strings.ToLower(n.Hostname), keyword) {
			continue
		}
		view, err := toNodeSummaryView(n)
		if err != nil {
			FailInternal(c, err, "decode_node_failed")
			return
		}
		if !h.populateNodeReferences(c, &view) {
			return
		}
		views = append(views, view)
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	total := len(views)
	start := min((page-1)*pageSize, total)
	end := min(start+pageSize, total)

	OK(c, paginated[nodeSummaryView]{
		Items:    views[start:end],
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}

func (h *nodeHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	ctx := c.Request.Context()
	n, err := repo.GetNode(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrNodeNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "节点不存在")
			return
		}
		FailInternal(c, err, "get_node_failed")
		return
	}
	pipelines, err := repo.ListPipelinesByNode(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "list_node_pipelines_failed")
		return
	}
	owned, err := h.scopeRuntime(ctx, *n, pipelines)
	if err != nil {
		FailInternal(c, err, "scope_node_runtime_failed")
		return
	}
	view, err := toNodeDetailView(owned, pipelines)
	if err != nil {
		FailInternal(c, err, "decode_node_failed")
		return
	}
	if !h.populateNodeReferences(c, &view.nodeSummaryView) {
		return
	}
	OK(c, view)
}

func (h *nodeHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req nodeUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if req.MaxPipelines != nil && *req.MaxPipelines > repo.MaxNodePipelineQuota {
		Fail(c, http.StatusBadRequest, "invalid_node_quota", "最大 pipeline 数必须为 0 到 10000 的整数")
		return
	}
	ctx := c.Request.Context()
	changed := req.Name != nil || req.Labels != nil || req.MaxPipelines != nil
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Check and lock before changing any fields; a rejected disable rolls back the entire request.
		if req.Disabled != nil && *req.Disabled {
			if err := repo.DisableNode(ctx, tx, id); err != nil {
				return err
			}
		}
		current, err := repo.GetNode(ctx, tx, id)
		if err != nil {
			return err
		}
		if changed {
			if req.Name != nil {
				current.Name = *req.Name
			}
			if req.Labels != nil {
				current.Labels, err = encodeLabels(*req.Labels)
				if err != nil {
					return err
				}
			}
			if req.MaxPipelines != nil {
				current.MaxPipelines = *req.MaxPipelines
			}
			if err := repo.UpdateNode(ctx, tx, current); err != nil {
				return err
			}
		}
		if req.Disabled != nil && !*req.Disabled {
			return repo.EnableNode(ctx, tx, id)
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		if writeResourceNameError(c, err) || h.respondNodeMutationError(c, id, err) || writeNodeQuotaError(c, err) {
			return
		}
		if errors.Is(err, repo.ErrNodeVersionConflict) {
			Fail(c, http.StatusConflict, "node_version_conflict", "节点已被并发修改，请刷新后重试")
			return
		}
		FailInternal(c, err, "update_node_failed")
		return
	}
	record := func(action model.AuditAction) {
		event := baseAuditEvent(c)
		event.Action = action
		event.TargetType = model.AuditTargetTypeNode
		event.TargetID = id.String()
		h.audit.Record(ctx, event)
	}
	if changed {
		record(model.AuditActionNodeUpdate)
	}
	if req.Disabled != nil {
		if *req.Disabled {
			record(model.AuditActionNodeDisable)
		} else {
			record(model.AuditActionNodeEnable)
		}
	}
	n, err := repo.GetNode(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_node_failed")
		return
	}
	view, err := toNodeSummaryView(*n)
	if err != nil {
		FailInternal(c, err, "decode_node_failed")
		return
	}
	if !h.populateNodeReferences(c, &view) {
		return
	}
	OK(c, view)
}

func (h *nodeHandler) respondNodeMutationError(c *gin.Context, id uuidx.UUID, err error) bool {
	if errors.Is(err, repo.ErrNodeNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "节点不存在")
		return true
	}
	if errors.Is(err, repo.ErrNodeHasDependents) {
		blockers, queryErr := repo.PipelinesBlockingNodeDeletion(c.Request.Context(), h.db, id)
		if queryErr != nil {
			FailInternal(c, queryErr, "check_node_dependents_failed")
		} else {
			respondNodeDeregisterBlocked(c, blockers)
		}
		return true
	}
	return false
}

func (h *nodeHandler) populateNodeReferences(c *gin.Context, view *nodeSummaryView) bool {
	id, err := uuidx.Parse(view.ID)
	if err != nil {
		FailInternal(c, err, "decode_node_failed")
		return false
	}
	refs, err := repo.PipelinesBlockingNodeDeletion(c.Request.Context(), h.db, id)
	if err != nil {
		FailInternal(c, err, "check_node_dependents_failed")
		return false
	}
	view.HasPipelines = len(refs) > 0
	return true
}

// revoke retires the current token atomically and records the audit event.
// The old hash authenticates only node.hello retirement replies. The node
// verifies that reply, persists its retirement marker, and shuts down pipelines.
// A disconnected node learns retirement when its control connection recovers.
func (h *nodeHandler) revoke(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if err := repo.RevokeNodeToken(c.Request.Context(), h.db, id); err != nil {
		if h.respondNodeMutationError(c, id, err) {
			return
		}
		FailInternal(c, err, "revoke_node_token_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionNodeRevoke
	event.TargetType = model.AuditTargetTypeNode
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}

func (h *nodeHandler) logLevel(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req nodeLogLevelRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	if !req.LogLevel.Valid() {
		Fail(c, http.StatusBadRequest, "invalid_log_level", "logLevel 取值不合法")
		return
	}
	if !req.LogShipLevel.Valid() {
		Fail(c, http.StatusBadRequest, "invalid_log_ship_level", "logShipLevel 取值不合法")
		return
	}

	if err := repo.SetNodeLogLevel(c.Request.Context(), h.db, id, req.LogLevel, req.LogShipLevel); err != nil {
		if errors.Is(err, repo.ErrNodeNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "节点不存在")
			return
		}
		FailInternal(c, err, "set_node_log_level_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionNodeSetLogLevel
	event.TargetType = model.AuditTargetTypeNode
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{
		"desiredLogLevel":     string(req.LogLevel),
		"desiredLogShipLevel": string(req.LogShipLevel),
		"actualLogLevel":      nodeLogLevelActualUnknown,
		"actualLogShipLevel":  nodeLogLevelActualUnknown,
	})
}

// nodeDeregisterBlockerView 是 DELETE /nodes/{id} 被阻断时 409 响应体的
// 单条阻断项，对齐「body 为阻断者完整清单（
// PipelinesBlockingNodeDeletion 的返回），不是计数或布尔」的要求。只挑
// 界面用得上的字段，不整段透传 model.Pipeline（没有必要把 strategy_id、
// config_version 这类和"为什么挡住注销"无关的字段也搭进去）。
type nodeDeregisterBlockerView struct {
	PipelineID   string     `json:"pipelineId"`
	PipelineName string     `json:"pipelineName"`
	CleanupState string     `json:"cleanupState"`
	DeletedAt    *time.Time `json:"deletedAt"`
}

func toNodeDeregisterBlockerViews(pipelines []model.Pipeline) []nodeDeregisterBlockerView {
	views := make([]nodeDeregisterBlockerView, 0, len(pipelines))
	for _, p := range pipelines {
		var deletedAt *time.Time
		if p.DeletedAt.Valid {
			t := p.DeletedAt.Time
			deletedAt = &t
		}
		views = append(views, nodeDeregisterBlockerView{
			PipelineID:   p.ID.String(),
			PipelineName: p.Name,
			CleanupState: string(p.CleanupState),
			DeletedAt:    deletedAt,
		})
	}
	return views
}

// nodeDeregisterResultView 是 DELETE /nodes/{id} 成功时的响应体（202，
// 见 respondNodeDeregisterAccepted 的注释）。
type nodeDeregisterResultView struct {
	ID           string `json:"id"`
	CleanupState string `json:"cleanupState"`
}

func respondNodeDeregisterBlocked(c *gin.Context, blockers []model.Pipeline) {
	FailWithDetails(c, http.StatusConflict, "node_has_dependents",
		fmt.Sprintf("节点仍被 %d 条 pipeline 引用，不能禁用、吊销 token 或注销（含待清理未完成的）", len(blockers)),
		toNodeDeregisterBlockerViews(blockers))
}

func (h *nodeHandler) deregister(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	ctx := c.Request.Context()

	if err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.RevokeNodeToken(ctx, tx, id); err != nil {
			return err
		}
		return repo.SoftDeleteNode(ctx, tx, id, time.Now())
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		if h.respondNodeMutationError(c, id, err) {
			return
		}
		FailInternal(c, err, "deregister_node_failed")
		return
	}

	revokeEvent := baseAuditEvent(c)
	revokeEvent.Action = model.AuditActionNodeRevoke
	revokeEvent.TargetType = model.AuditTargetTypeNode
	revokeEvent.TargetID = id.String()
	h.audit.Record(ctx, revokeEvent)

	deleteEvent := baseAuditEvent(c)
	deleteEvent.Action = model.AuditActionNodeDelete
	deleteEvent.TargetType = model.AuditTargetTypeNode
	deleteEvent.TargetID = id.String()
	h.audit.Record(ctx, deleteEvent)

	OKStatus(c, http.StatusAccepted, nodeDeregisterResultView{
		ID:           id.String(),
		CleanupState: string(model.CleanupStatePending),
	})
}

// pipelineCountOf 算出 pipelineCount 的三态取值。
//
// PipelinesKnownCount 才是 presence 载体：proto3 的 repeated 没有 has 位，
// "确实承载 0 条"与"镜像链路又断了"在 wire 上是同一串零字节，这个 optional
// 是唯一能分开它们的东西。
//
// PipelinesKnownCount 是校验和不是显示值（见 internal/manager/ingest/ctl_report.go 那段
// "绝不能用 uint32(len(pipelines)) 算"的说明）。两者分工：前者决定知不知道，
// 后者给出是多少。
//
// 按 proto/report.proto 的既定口径，校验和对不上的结论是"这组数不可信"——不可信
// 就不能渲染成任何一个具体数字（渲染 len 或渲染 claimed 都是在一个已知不可信
// 的基础上挑一个），null 是唯一诚实的答案。warn 带 node_id/claimed/actual，
// 否则运维只看到界面上少了一格、不知道为什么。
func pipelineCountOf(rt *model.NodeRuntime, nodeID uuidx.UUID) *int {
	if rt.PipelinesKnownCount == nil {
		return nil
	}
	actual := len(rt.Pipelines)
	if uint64(*rt.PipelinesKnownCount) != uint64(actual) {
		slog.Warn("http: node runtime pipelines_known_count disagrees with pipelines length",
			slog.String("node_id", nodeID.String()),
			slog.Uint64("claimed", uint64(*rt.PipelinesKnownCount)),
			slog.Int("actual", actual))
		return nil
	}
	return &actual
}
