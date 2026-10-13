// Pipeline HTTP 层。
//
// 前端 PipelinesPage/PipelineCreatePage/PipelineDetailPage 与
// web/src/api/pipelines.ts 消费本文件的 DTO；位点详情和确认消费
// positionHandler 的独立契约。
//
// 路由清单：
//
// GET/POST         /pipelines
//
//	GET/PATCH/DELETE /pipelines/:id
//	POST             /pipelines/:id/start
//	POST             /pipelines/:id/pause
//	POST             /pipelines/:id/resume
//	POST             /pipelines/:id/stop
//	POST             /pipelines/:id/retry
//	POST             /pipelines/:id/migrate-node
package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// maxBinlogServerIDAllocAttempts 是单次分配冲突时的重试上限（见
// allocateBinlogServerID）。默认号段跨度约 2.9 亿，即使已有 100 万条
// pipeline 占满号段内 100 万个值，单次随机命中已占用值的概率也只有约
// 0.34%，连续 10 次都命中冲突的概率低到可以忽略——真出现这种情况，更合理
// 的解释是号段配置得太小，应该让调用方看到明确失败去调整
// config.PipelineConfig，而不是无限重试掩盖号段不够用的问题。
const maxBinlogServerIDAllocAttempts = 10

// randomBinlogServerIDCandidate 是 pipelineHandler.pickBinlogServerIDCandidate
// 的生产实现：在 [min, max] 闭区间内均匀取一个候选 server_id。
//
// 用 uint64 计算跨度（span）：min=0、max=MaxUint32 时 max-min+1 在 uint32
// 里会整数溢出成 0，若直接传给按 uint32 取模/取余的随机函数会导致除零
// panic；uint64 装得下"uint32 全域跨度 + 1"，不存在这个问题。
// rand.Uint64N(span) 的返回值落在 [0, span)，span<=2^32 保证转回 uint32
// 不会截断；min + 该值落在 [min, max]，不会超过 max 造成 uint32 加法溢出。
func randomBinlogServerIDCandidate(minID, maxID uint32) uint32 {
	span := uint64(maxID) - uint64(minID) + 1
	return minID + uint32(rand.Uint64N(span))
}

// pipelineView projects persisted fields and derived display state.
// Source and target observations remain independent; unknown denotes an absent
// current observation, while last errors retain their previous diagnostics.
// Cleanup progress and run positions use their dedicated views.
type pipelineView struct {
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	// SourceNodeLastHeartbeatAt/TargetNodeLastHeartbeatAt 是两侧节点最后
	// 一次心跳的时间，nil 表示该节点从未心跳过（刚注册）或查不到该节点。
	//
	// 这里给的是**节点心跳时间**，不是"这一侧最后一次上报 pipeline 状态
	// 的时间"——后者今天不存在这份数据，要它就要新开存储。心跳时间零
	// 新存储（nodes.last_heartbeat_at 本来就在），而且语义更准：
	// **离线本来就是按节点判的**（internal/manager/ingest/ctl_offline.go 用心跳超时判定，
	// 再调 MarkPipelineRoleUnknown 把该侧标 unknown），所以配合
	// {side}ActualState == "unknown" 一起看，运维拿到的是"哪一侧、
	// 从什么时候起失联"这一对完整信息。
	SourceNodeLastHeartbeatAt *time.Time `json:"sourceNodeLastHeartbeatAt"`
	TargetNodeLastHeartbeatAt *time.Time `json:"targetNodeLastHeartbeatAt"`
	ID                        string     `json:"id"`
	Name                      string     `json:"name"`
	TopologyID                string     `json:"topologyId"`
	SourceEndpointID          string     `json:"sourceEndpointId"`
	SourceNodeID              string     `json:"sourceNodeId"`
	TargetEndpointID          string     `json:"targetEndpointId"`
	TargetNodeID              string     `json:"targetNodeId"`
	StrategyID                string     `json:"strategyId"`
	InitMode                  string     `json:"initMode"`
	DesiredState              string     `json:"desiredState"`
	// DisplayState/DisplayLastError 是两侧聚合出来的展示值，
	// **只能用于展示，不得用于任何调度/编排决策**——名字里的 Display
	// 就是这条约束唯一的强制手段，见本类型上方的说明。
	DisplayState     string `json:"displayState"`
	DisplayLastError string `json:"displayLastError,omitempty"`
	// DisplayStateSide 是上面两个值取自哪一侧（"source" / "target"）。
	DisplayStateSide string `json:"displayStateSide"`

	SourceActualState string          `json:"sourceActualState"`
	SourceLastError   string          `json:"sourceLastError,omitempty"`
	TargetActualState string          `json:"targetActualState"`
	TargetLastError   string          `json:"targetLastError,omitempty"`
	StartPosition     json.RawMessage `json:"startPosition,omitempty"`
	ConfigVersion     uint64          `json:"configVersion"`
	PositionRevision  uint64          `json:"positionRevision"`
	StrategyVersion   uint32          `json:"strategyVersion"`
	// 取值由 dispatch.RecoveryExhausted 计算，本层不重写那条比较
	// （同 DeriveDisplayState"只在一处计算"的理由）。前端据此渲染
	// "已停止自动恢复、等待人工"，并把 POST /pipelines/:id/retry
	// 做成可点的出口——注意 ACTUAL_STATE_COLOR 那一套配色分辨不了
	// 它："仍在自动重试"与"已放弃、等人工"的 actualState 都是
	// error，逐像素相同，必须靠这个字段区分。
	RecoveryExhausted bool `json:"recoveryExhausted"`
}

// displayStateSideOf 回答 DeriveDisplayState 这一次的返回值取自哪一侧。
//
// 两侧状态不同时只有一侧能匹配上；两侧完全相同时先匹配到源侧，与
// DeriveDisplayState 的选择一致。
func displayStateSideOf(p model.Pipeline, state model.ActualState, lastError string) model.Side {
	if p.SourceActualState == state && p.SourceLastError == lastError {
		return model.SideSource
	}
	return model.SideTarget
}

// nodeHeartbeats 是一次请求内的"节点 ID → 最后心跳时间"缓存。
//
// 为什么是缓存而不是一条批量查询：repo 层今天没有"按 ID 集合批量取
// 节点"的原语，而 internal/manager/repo 不归本模块可写；在 http 层直接
// 写 gorm 查询会破坏"http 只经 repo 访问数据库"这条本包至今零例外的
// 分层（全包生产代码里没有一处 db.Model/db.Where）。逐个 repo.GetNode
// 加一层按 ID 去重的缓存，把查询次数从"pipeline 条数 × 2"降到"涉及的
// 不同节点数"——列表页通常是少数几个节点承载很多条 pipeline，这个量级
// 与列表本身"先取全量再内存过滤"的既有取舍一致。
type nodeHeartbeats map[uuidx.UUID]*time.Time

// lookup 取 nodeID 的最后心跳时间，查不到或出错时返回 nil。
//
// **查不到不算请求失败**：心跳时间是辅助展示信息，为了它把整个
// pipeline 列表变成 500，代价与收益完全不成比例。节点行不存在（被清理
// 过）对应的就是"没有心跳信息"，如实给 null。
func (h nodeHeartbeats) lookup(ctx context.Context, db *gorm.DB, nodeID uuidx.UUID) *time.Time {
	if nodeID.IsZero() {
		return nil
	}
	if ts, ok := h[nodeID]; ok {
		return ts
	}
	var ts *time.Time
	n, err := repo.GetNode(ctx, db, nodeID)
	switch {
	case err == nil:
		ts = n.LastHeartbeatAt
	case errors.Is(err, repo.ErrNodeNotFound):
		// pipeline 引用了一个已经不存在的节点行，如实给 null。
	default:
		slog.Warn("http: 取节点心跳时间失败，pipeline 视图该侧心跳留空",
			slog.String("node_id", nodeID.String()), slog.Any("error", err))
	}
	h[nodeID] = ts
	return ts
}

// toPipelineView 组装对外视图。hb 可以为 nil（此时两个心跳字段都是
// null）；调用方传一个共享的 nodeHeartbeats 就能在列表场景下复用查询。
func toPipelineView(ctx context.Context, db *gorm.DB, p model.Pipeline, hb nodeHeartbeats) pipelineView {
	displayState, displayLastError := p.DeriveDisplayState()
	return pipelineView{
		ID:               p.ID.String(),
		Name:             p.Name,
		TopologyID:       p.TopologyID.String(),
		SourceEndpointID: p.SourceEndpointID.String(),
		SourceNodeID:     p.SourceNodeID.String(),
		TargetEndpointID: p.TargetEndpointID.String(),
		TargetNodeID:     p.TargetNodeID.String(),
		StrategyID:       p.StrategyID.String(),
		StrategyVersion:  p.StrategyVersion,
		InitMode:         string(p.InitMode),
		StartPosition:    json.RawMessage(p.StartPosition),
		DesiredState:     string(p.DesiredState),
		ConfigVersion:    p.ConfigVersion,
		PositionRevision: p.PositionRevision,

		DisplayState:     string(displayState),
		DisplayLastError: displayLastError,
		DisplayStateSide: string(displayStateSideOf(p, displayState, displayLastError)),

		SourceActualState: string(p.SourceActualState),
		SourceLastError:   p.SourceLastError,
		TargetActualState: string(p.TargetActualState),
		TargetLastError:   p.TargetLastError,
		RecoveryExhausted: dispatch.RecoveryExhausted(p),

		SourceNodeLastHeartbeatAt: hb.lookup(ctx, db, p.SourceNodeID),
		TargetNodeLastHeartbeatAt: hb.lookup(ctx, db, p.TargetNodeID),

		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}
}

// pipelineCreateRequest 对应 MGR-PL-01：选择源端点+源节点、目标端点+目标
// 节点、策略版本、初始化方式与起始位点。desiredState/actualState 不接受
// 调用方指定，创建时固定为 Stopped/Draft，见 create 的注释。
type pipelineCreateRequest struct {
	Rules            json.RawMessage `json:"rules,omitempty"`
	Name             string          `json:"name" binding:"required"`
	TopologyID       string          `json:"topologyId"`
	SourceEndpointID string          `json:"sourceEndpointId" binding:"required"`
	SourceNodeID     string          `json:"sourceNodeId" binding:"required"`
	TargetEndpointID string          `json:"targetEndpointId" binding:"required"`
	TargetNodeID     string          `json:"targetNodeId" binding:"required"`
	StrategyID       string          `json:"strategyId"`
	StrategyVersion  uint32          `json:"strategyVersion"`
	InitMode         string          `json:"initMode" binding:"required"`
	StartPosition    json.RawMessage `json:"startPosition"`
}

type pipelineUpdateRequest struct {
	Name            *string `json:"name"`
	StrategyID      *string `json:"strategyId"`
	StrategyVersion *uint32 `json:"strategyVersion"`
	InitMode        *string `json:"initMode"`
}

type pipelineNodeMigrationRequest struct {
	Role          string  `json:"role" binding:"required"`
	NodeID        string  `json:"nodeId" binding:"required"`
	ConfigVersion *uint64 `json:"configVersion" binding:"required"`
}

type pipelineHandler struct {
	db         *gorm.DB
	audit      IAuditRecorder
	dispatcher dispatch.IPipelineDispatcher

	binlogServerIDRangeMin uint32
	binlogServerIDRangeMax uint32
	// pickBinlogServerIDCandidate 默认指向 randomBinlogServerIDCandidate
	// （registerPipelineRoutes 里设置）；同包测试可以替换成确定性闭包，
	// 构造"第一次返回一个已占用的值、第二次返回空闲值"之类的序列，从而
	// 在不需要真实并发 goroutine 的前提下验证 allocateBinlogServerID 的
	// 冲突重试逻辑——与本仓库"可测试逻辑通过参数/注入点接收非确定性输入"
	// 的既有约定同一原则（例如 dispatch 包对 now 的注入），只是这里注入
	// 的是候选值序列而不是时间。
	pickBinlogServerIDCandidate func(minID, maxID uint32) uint32
}

// registerPipelineRoutes 挂载 pipeline CRUD 与生命周期端点。
//
// dispatcher 只在 update（策略引用变化可能改变 assembled 内容，name-only
// 编辑交给 dispatch 自己的摘要比对过滤成 no-op，不在这里单独判断）与
// delete（推 config.changed{deleted:true}，见 delete 的注释）两处被调用。
// create 与 start/pause/resume/stop 故意不调用，理由见各自 handler 上的
// 注释——已经在 internal/manager/dispatch 清单测试的伴生说明里逐条记录，
// 与本文件保持一份口径。
func registerPipelineRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder, dispatcher dispatch.IPipelineDispatcher, binlogServerIDRangeMin, binlogServerIDRangeMax uint32) {
	h := &pipelineHandler{
		db:                          db,
		audit:                       audit,
		dispatcher:                  dispatcher,
		binlogServerIDRangeMin:      binlogServerIDRangeMin,
		binlogServerIDRangeMax:      binlogServerIDRangeMax,
		pickBinlogServerIDCandidate: randomBinlogServerIDCandidate,
	}
	api.GET("/pipelines", h.list)
	api.POST("/pipelines", h.create)
	api.POST("/pipelines/bidirectional", RequireRole(auth.RoleAdmin), h.createBidirectional)
	api.POST("/pipelines/batch", RequireRole(auth.RoleAdmin), h.batch)
	api.POST("/pipelines/:id/clone", RequireRole(auth.RoleAdmin), h.clone)
	api.GET("/pipelines/:id", RequirePipelineScope(db, false), h.get)
	api.GET("/pipelines/:id/risk", RequirePipelineScope(db, false), h.getRisk)
	api.POST("/pipelines/:id/risk/confirm", RequirePipelineScope(db, true), h.confirmRisk)
	api.GET("/pipelines/:id/rules", RequirePipelineScope(db, false), h.getRules)
	api.PUT("/pipelines/:id/rules", RequirePipelineScope(db, true), h.saveRules)
	api.POST("/pipelines/:id/rules/apply", RequirePipelineScope(db, true), h.applyRules)
	api.GET("/pipelines/:id/releases", RequirePipelineScope(db, false), h.listReleases)
	api.PATCH("/pipelines/:id", RequirePipelineScope(db, true), h.update)
	api.DELETE("/pipelines/:id", RequirePipelineScope(db, true), h.delete)
	api.POST("/pipelines/:id/start", RequirePipelineScope(db, true), h.start)
	api.POST("/pipelines/:id/pause", RequirePipelineScope(db, true), h.pause)
	api.POST("/pipelines/:id/resume", RequirePipelineScope(db, true), h.resume)
	api.POST("/pipelines/:id/stop", RequirePipelineScope(db, true), h.stop)
	api.POST("/pipelines/:id/retry", RequirePipelineScope(db, true), h.retry)
	api.POST("/pipelines/:id/migrate-node", RequireRole(auth.RoleAdmin), h.migrateNode)
}

// pipelineValidationError 是创建/更新 pipeline 时引用校验（存在性 + C-19/
// C-16 同中心一致性）失败的错误类型，与"仓储层内部错误"区分开——前者一律
// 映射成 400，后者走 FailInternal。用 errors.As 在 handler 里识别。
type pipelineValidationError struct {
	code    string
	message string
}

func (e *pipelineValidationError) Error() string { return e.message }

// writePipelineValidationError 把 validatePipelineEndpoints/
// validateStrategyVersionExists 返回的 error 映射成 HTTP 响应：
// *pipelineValidationError 一律 400，其余当内部错误处理。
func writePipelineValidationError(c *gin.Context, err error) {
	if verr, ok := errors.AsType[*pipelineValidationError](err); ok {
		Fail(c, http.StatusBadRequest, verr.code, verr.message)
		return
	}
	FailInternal(c, err, "validate_pipeline_reference_failed")
}

// validatePipelineEndpoints 校验 source/target 端点与节点的引用存在性
// （含已软删），并执行 C-19/C-16 的同中心一致性校验：源节点必须与源端点
// 同中心，目标节点必须与目标端点同中心，不满足一律拒绝。
func validatePipelineEndpoints(ctx context.Context, db *gorm.DB, sourceEndpointID, sourceNodeID, targetEndpointID, targetNodeID uuidx.UUID) error {
	sourceEndpoint, err := repo.GetEndpoint(ctx, db, sourceEndpointID)
	if err != nil {
		if errors.Is(err, repo.ErrEndpointNotFound) {
			return &pipelineValidationError{"source_endpoint_not_found", "sourceEndpointId 对应的端点不存在"}
		}
		return err
	}
	sourceNode, err := repo.GetNode(ctx, db, sourceNodeID)
	if err != nil {
		if errors.Is(err, repo.ErrNodeNotFound) {
			return &pipelineValidationError{"source_node_not_found", "sourceNodeId 对应的节点不存在"}
		}
		return err
	}
	if sourceNode.CenterID != sourceEndpoint.CenterID {
		return &pipelineValidationError{"source_center_mismatch", "源节点与源端点不在同一中心（C-19/C-16）"}
	}

	targetEndpoint, err := repo.GetEndpoint(ctx, db, targetEndpointID)
	if err != nil {
		if errors.Is(err, repo.ErrEndpointNotFound) {
			return &pipelineValidationError{"target_endpoint_not_found", "targetEndpointId 对应的端点不存在"}
		}
		return err
	}
	targetNode, err := repo.GetNode(ctx, db, targetNodeID)
	if err != nil {
		if errors.Is(err, repo.ErrNodeNotFound) {
			return &pipelineValidationError{"target_node_not_found", "targetNodeId 对应的节点不存在"}
		}
		return err
	}
	if targetNode.CenterID != targetEndpoint.CenterID {
		return &pipelineValidationError{"target_center_mismatch", "目标节点与目标端点不在同一中心（C-19/C-16）"}
	}
	return nil
}

// validateStrategyVersionExists 校验 (strategyId, strategyVersion) 组合
// 确实存在——理由同上：裸 UUID + uint32 组合，没有外键，不校验就是允许
// pipeline 引用一个不存在的策略版本。
func validateStrategyVersionExists(ctx context.Context, db *gorm.DB, strategyID uuidx.UUID, version uint32) error {
	if _, err := repo.GetStrategyVersion(ctx, db, strategyID, version); err != nil {
		if errors.Is(err, repo.ErrStrategyVersionNotFound) {
			return &pipelineValidationError{"strategy_version_not_found", "strategyId/strategyVersion 对应的策略版本不存在"}
		}
		return err
	}
	return nil
}

func (h *pipelineHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	topologyID := uuidx.Nil
	if v := c.Query("topologyId"); v != "" {
		parsed, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_topology_id", "topologyId 格式不合法")
			return
		}
		topologyID = parsed
	}

	pipelines, err := repo.ListPipelines(c.Request.Context(), h.db, topologyID)
	if err != nil {
		FailInternal(c, err, "list_pipelines_failed")
		return
	}
	if role, ok := RoleFromContext(c); !ok || role != auth.RoleAdmin {
		userID, parseErr := uuidx.Parse(UserIDFromContext(c))
		if parseErr != nil {
			Fail(c, http.StatusForbidden, "forbidden", "没有权限访问 pipeline")
			return
		}
		pipelines, err = repo.FilterPipelinesByUser(c.Request.Context(), h.db, userID, pipelines)
		if err != nil {
			FailInternal(c, err, "filter_pipelines_by_scope_failed")
			return
		}
	}

	// keyword 在内存过滤（先取全量再过滤再分页）：与 endpoints.go/
	// internal/manager/http/nodes.go 的 list 同一个理由，pipeline 现阶段的规模不需要 DB 层过滤。
	keyword := strings.ToLower(strings.TrimSpace(c.Query("keyword")))
	// 整个列表共用一份节点心跳缓存：少数几个节点承载很多条 pipeline 时，
	// 查询次数是"不同节点数"而不是"pipeline 条数 × 2"。
	hb := nodeHeartbeats{}
	views := make([]pipelineView, 0, len(pipelines))
	for _, p := range pipelines {
		if keyword != "" && !strings.Contains(strings.ToLower(p.Name), keyword) {
			continue
		}
		views = append(views, toPipelineView(c.Request.Context(), h.db, p, hb))
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	total := len(views)
	start := min((page-1)*pageSize, total)
	end := min(start+pageSize, total)

	OK(c, paginated[pipelineView]{
		Items:    views[start:end],
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}

func (h *pipelineHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	ctx := c.Request.Context()
	p, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		FailInternal(c, err, "get_pipeline_failed")
		return
	}
	OK(c, toPipelineView(ctx, h.db, *p, nodeHeartbeats{}))
}

// C-19/C-16 同中心校验 + 六类引用存在性校验（source/target 端点与
// 节点、策略版本、拓扑）由请求处理入口完成，见 validatePipelineEndpoints/
// validateStrategyVersionExists。
func (h *pipelineHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req pipelineCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	h.createFromRequest(c, req, "")
}

// createFromRequest keeps cloning on the normal configuration validation,
// scope/quota checks and independent run/server identity allocation path.
func (h *pipelineHandler) createFromRequest(c *gin.Context, req pipelineCreateRequest, clonedFrom string) {
	initMode := model.InitMode(req.InitMode)
	if !initMode.Valid() {
		Fail(c, http.StatusBadRequest, "invalid_init_mode", "initMode 取值不合法")
		return
	}

	if req.TopologyID == "" {
		req.TopologyID = repo.AutomaticTopologyID
	}
	topologyID, err := uuidx.Parse(req.TopologyID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_topology_id", "topologyId 格式不合法")
		return
	}
	sourceEndpointID, err := uuidx.Parse(req.SourceEndpointID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_source_endpoint_id", "sourceEndpointId 格式不合法")
		return
	}
	sourceNodeID, err := uuidx.Parse(req.SourceNodeID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_source_node_id", "sourceNodeId 格式不合法")
		return
	}
	targetEndpointID, err := uuidx.Parse(req.TargetEndpointID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_target_endpoint_id", "targetEndpointId 格式不合法")
		return
	}
	targetNodeID, err := uuidx.Parse(req.TargetNodeID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_target_node_id", "targetNodeId 格式不合法")
		return
	}
	strategyID := uuidx.Nil
	var canonicalRules []byte
	if hasPipelineRules(req.Rules) {
		if req.StrategyID != "" || req.StrategyVersion != 0 {
			Fail(c, http.StatusBadRequest, "invalid_rules", "rules 与策略引用不能同时提供")
			return
		}
		canonicalRules, err = canonicalPipelineRules(req.Rules)
	} else {
		strategyID, err = uuidx.Parse(req.StrategyID)
	}
	if err != nil {
		if hasPipelineRules(req.Rules) {
			writePipelineValidationError(c, err)
			return
		}
		Fail(c, http.StatusBadRequest, "invalid_strategy_id", "strategyId 格式不合法")
		return
	}

	ctx := c.Request.Context()

	if _, err := repo.GetTopology(ctx, h.db, topologyID); err != nil {
		if errors.Is(err, repo.ErrTopologyNotFound) {
			Fail(c, http.StatusBadRequest, "topology_not_found", "历史线路分组不存在")
			return
		}
		FailInternal(c, err, "get_topology_failed")
		return
	}

	if err := validatePipelineEndpoints(ctx, h.db, sourceEndpointID, sourceNodeID, targetEndpointID, targetNodeID); err != nil {
		writePipelineValidationError(c, err)
		return
	}

	if len(canonicalRules) == 0 {
		if err := validateStrategyVersionExists(ctx, h.db, strategyID, req.StrategyVersion); err != nil {
			writePipelineValidationError(c, err)
			return
		}
	}

	p := &model.Pipeline{
		Name:             req.Name,
		TopologyID:       topologyID,
		SourceEndpointID: sourceEndpointID,
		SourceNodeID:     sourceNodeID,
		TargetEndpointID: targetEndpointID,
		TargetNodeID:     targetNodeID,
		StrategyID:       strategyID,
		StrategyVersion:  req.StrategyVersion,
		InitMode:         initMode,
		StartPosition:    model.JSON(req.StartPosition),
		DesiredState:     model.DesiredStateStopped,
		// New pipelines begin with independent unknown source and target observations.
		SourceActualState: model.ActualStateDraft,
		TargetActualState: model.ActualStateDraft,
	}
	userID, err := uuidx.Parse(UserIDFromContext(c))
	if len(canonicalRules) > 0 && (err != nil || userID.IsZero()) {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限执行此操作")
		return
	}
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		handler := *h
		handler.db = tx
		if len(canonicalRules) > 0 {
			if err := repo.LockTopology(ctx, tx, p.TopologyID); err != nil {
				return err
			}
			p.ID = uuidx.New()
			version, err := repo.CreatePipelineRulesVersion(ctx, tx, p.ID, model.JSON(canonicalRules), userID)
			if err != nil {
				return err
			}
			p.StrategyID, p.StrategyVersion = version.StrategyID, version.Version
		}
		return handler.createPipelineWithBinlogServerID(ctx, p)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		if errors.Is(err, repo.ErrNodeDisabledOrRevoked) {
			Fail(c, http.StatusConflict, "node_unavailable", "节点已禁用、吊销或注销，请重新选择节点")
			return
		}
		if writeNodeQuotaError(c, err) {
			return
		}
		if writeResourceNameError(c, err) || writePipelineOverlapError(c, err) {
			return
		}
		if errors.Is(err, topologycheck.ErrBidirectionalInitialization) {
			Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "双向初始化只能由一个方向执行全量或结构迁移，反方向需使用仅增量：当前最新")
			return
		}
		if errors.Is(err, repo.ErrPipelineNameConflict) {
			Fail(c, http.StatusConflict, "pipeline_name_conflict", "pipeline 名称已存在")
			return
		}
		FailInternal(c, err, "create_pipeline_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineCreate
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = p.ID.String()
	if clonedFrom != "" {
		event.Changes, _ = json.Marshal(map[string]string{"cloned_from": clonedFrom})
	}
	h.audit.Record(ctx, event)

	OK(c, toPipelineView(ctx, h.db, *p, nodeHeartbeats{}))
}

// createPipelineWithBinlogServerID 在同一个事务里创建 pipeline 并分配
// server_id（MGR-PL-06），保证二者要么一起成功、要么一起回滚：重试预算
// 耗尽导致分配彻底失败时，不会留下一条已经落库、但没有合法 server_id 的
// pipeline——那样的 pipeline 永远无法真正启动（dump 阶段建立复制协议连接
// 需要一个合法 server_id），却已经在列表/详情接口里可见，是比"创建直接
// 失败"更差的结果。
//
// 保存事务同时校验初始化与目标范围重叠，源端点锁序列化跨拓扑保存。
// ReadCommitted 保证等待锁之后读取最新配置；事务继承请求超时和取消。
func (h *pipelineHandler) createPipelineWithBinlogServerID(ctx context.Context, p *model.Pipeline) error {
	return h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		if err := repo.CheckBidirectionalInitialization(ctx, tx, p, false); err != nil {
			return err
		}
		if err := h.validateScopeOverlap(ctx, tx, p); err != nil {
			return err
		}
		if err := repo.CreatePipeline(ctx, tx, p); err != nil {
			return err
		}
		if err := h.allocateBinlogServerID(ctx, tx, p); err != nil {
			return err
		}
		if err := repo.EnsurePipelineSnapshotRun(ctx, tx, p.ID); err != nil {
			return err
		}
		updated, err := repo.GetPipeline(ctx, tx, p.ID)
		if err == nil {
			*p = *updated
		}
		return err
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

// allocateBinlogServerID 在 [h.binlogServerIDRangeMin, h.binlogServerIDRangeMax]
// 闭区间内重试分配一个未被占用的 server_id，最多尝试
// maxBinlogServerIDAllocAttempts 次；由 createPipelineWithBinlogServerID
// 在同一个事务里紧跟 repo.CreatePipeline 之后调用。
func (h *pipelineHandler) allocateBinlogServerID(ctx context.Context, tx *gorm.DB, p *model.Pipeline) error {
	var lastErr error
	for range maxBinlogServerIDAllocAttempts {
		candidate := h.pickBinlogServerIDCandidate(h.binlogServerIDRangeMin, h.binlogServerIDRangeMax)
		err := repo.AllocatePipelineBinlogServerID(ctx, tx, p.ID, candidate)
		if err == nil {
			p.BinlogServerID = &candidate
			return nil
		}
		if !errors.Is(err, repo.ErrPipelineBinlogServerIDConflict) {
			return err
		}
		lastErr = err
	}
	return fmt.Errorf("http: 分配 binlog_server_id 连续 %d 次冲突（号段 [%d, %d]），已放弃重试: %w",
		maxBinlogServerIDAllocAttempts, h.binlogServerIDRangeMin, h.binlogServerIDRangeMax, lastErr)
}

// source/target 端点与节点、topologyId 不可变（改端点等同于创建一条新
// pipeline，与 repo.PipelineConfigUpdate 的字段集合一致），因此不重复
// C-19/C-16 校验；strategyId/strategyVersion 有变化时重新校验该组合存在。
func (h *pipelineHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req pipelineUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	ctx := c.Request.Context()
	current, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	if req.Name != nil && req.StrategyID == nil && req.StrategyVersion == nil && req.InitMode == nil {
		h.rename(c, id, *req.Name)
		return
	}

	update := repo.PipelineConfigUpdate{
		Name:            current.Name,
		StrategyID:      current.StrategyID,
		StrategyVersion: current.StrategyVersion,
		InitMode:        current.InitMode,
		StartPosition:   current.StartPosition,
	}
	if req.Name != nil {
		update.Name = *req.Name
	}
	strategyRefChanged := false
	if req.StrategyID != nil {
		strategyID, err := uuidx.Parse(*req.StrategyID)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_strategy_id", "strategyId 格式不合法")
			return
		}
		update.StrategyID = strategyID
		strategyRefChanged = true
	}
	if req.StrategyVersion != nil {
		update.StrategyVersion = *req.StrategyVersion
		strategyRefChanged = true
	}
	if req.InitMode != nil {
		initMode := model.InitMode(*req.InitMode)
		if !initMode.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_init_mode", "initMode 取值不合法")
			return
		}
		update.InitMode = initMode
	}

	if strategyRefChanged {
		if err := validateStrategyVersionExists(ctx, h.db, update.StrategyID, update.StrategyVersion); err != nil {
			writePipelineValidationError(c, err)
			return
		}
	}

	if err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.LockTopology(ctx, tx, current.TopologyID); err != nil {
			return err
		}
		if strategyRefChanged {
			candidate := *current
			candidate.StrategyID, candidate.StrategyVersion = update.StrategyID, update.StrategyVersion
			if err := h.validateScopeOverlap(ctx, tx, &candidate); err != nil {
				return err
			}
		}
		return repo.UpdatePipelineConfig(ctx, tx, id, current.ConfigVersion, update)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted}); err != nil {
		if writeResourceNameError(c, err) || writePipelineOverlapError(c, err) {
			return
		}
		switch {
		case errors.Is(err, topologycheck.ErrBidirectionalInitialization):
			Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "双向初始化只能由一个方向执行全量或结构迁移，反方向需使用仅增量：当前最新")
		case errors.Is(err, repo.ErrPipelineNotFound):
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		case errors.Is(err, repo.ErrPipelineNameConflict):
			Fail(c, http.StatusConflict, "pipeline_name_conflict", "pipeline 名称已存在")
		case errors.Is(err, repo.ErrPipelineConfigVersionConflict):
			Fail(c, http.StatusConflict, "pipeline_config_version_conflict", "pipeline 配置已被并发修改，请刷新后重试")
		default:
			FailInternal(c, err, "update_pipeline_failed")
		}
		return
	}

	updated, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineUpdate
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	// dispatch 自己会比较组装摘要，name-only 编辑（内容没变）在那一层会
	// 被判定为 no-op，这里不需要先判断 strategyRefChanged 再决定要不要
	// 调用——见 dispatch.SyncPipeline 文档。这次编辑如果同时命中了真正
	// 影响内容的字段，config_version 可能跳两格而不是一格（上面
	// repo.UpdatePipelineConfig 已经无条件递增过一次，dispatch 判定内容
	// 确实变了会再独立递增一次）——这是权衡过两种失败模式之后接受的
	// 已知行为，不是 bug，完整论证见 internal/manager/dispatch 包文档
	// "为什么 internal/manager/http/pipelines.go 的 PATCH 会让 config_version 跳两格，以及为
	// 什么不修"一节，不要在这里重复展开，也不要凭直觉去"优化"掉它。
	syncPipelineBestEffort(ctx, h.dispatcher, id)

	OK(c, toPipelineView(ctx, h.db, *updated, nodeHeartbeats{}))
}

// The committed deletion increments config_version and last_cmd_seq. Notify
// carries that committed revision; background cleanup repeats the notification
// until both authenticated owners acknowledge the matching UNLOAD.
func (h *pipelineHandler) delete(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}

	ctx := c.Request.Context()
	before, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	if err := h.db.Transaction(func(tx *gorm.DB) error {
		if err := repo.LockTopology(ctx, tx, before.TopologyID); err != nil {
			return err
		}
		return repo.SoftDeletePipeline(ctx, tx, id, time.Now())
	}); err != nil {
		if errors.Is(err, repo.ErrPipelineDeleteRequiresStopped) {
			Fail(c, http.StatusConflict, "pipeline_delete_requires_stopped", "请先停止线路并等待两端停止后再删除")
			return
		}
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		FailInternal(c, err, "delete_pipeline_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineDelete
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	deleted, err := repo.GetPipelineIncludingDeleted(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_cleanup_failed")
		return
	}
	notifyPipelineDeletedBestEffort(ctx, h.dispatcher, id, deleted.ConfigVersion, deleted.SourceNodeID, deleted.TargetNodeID)

	OK(c, gin.H{})
}

func (h *pipelineHandler) start(c *gin.Context) {
	h.setDesiredState(c, model.DesiredStateRunning, model.AuditActionPipelineStart)
}

func (h *pipelineHandler) pause(c *gin.Context) {
	h.setDesiredState(c, model.DesiredStatePaused, model.AuditActionPipelinePause)
}

func (h *pipelineHandler) resume(c *gin.Context) {
	h.setDesiredState(c, model.DesiredStateRunning, model.AuditActionPipelineResume)
}

func (h *pipelineHandler) stop(c *gin.Context) {
	h.setDesiredState(c, model.DesiredStateStopped, model.AuditActionPipelineStop)
}

// migrateNode changes one side's node within the endpoint's center. The
// pipeline must be stopped; the repository keeps run/checkpoint identity and
// advances config_version atomically, then dispatch notifies both old and new
// nodes so the old role unloads and the new role pulls its projection.
func (h *pipelineHandler) migrateNode(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req pipelineNodeMigrationRequest
	if err := c.ShouldBindJSON(&req); err != nil || req.ConfigVersion == nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "role、nodeId、configVersion 为必填参数")
		return
	}
	var side model.Side
	switch req.Role {
	case string(model.SideSource):
		side = model.SideSource
	case string(model.SideTarget):
		side = model.SideTarget
	default:
		Fail(c, http.StatusBadRequest, "invalid_role", "role 必须是 source 或 target")
		return
	}
	newNodeID, err := uuidx.Parse(req.NodeID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_node_id", "nodeId 格式不合法")
		return
	}
	ctx := c.Request.Context()
	migrator, ok := h.dispatcher.(dispatch.IPipelineNodeMigrator)
	if !ok {
		Fail(c, http.StatusServiceUnavailable, "dispatcher_unavailable", "节点迁移下发器未配置")
		return
	}
	var bundle *iproto.PipelineStateBundle
	p, err := repo.GetPipeline(ctx, h.db, id)
	if err == nil && p.LastCmdSeq > 0 && (p.InitMode == model.InitModeFullAndIncremental || p.InitMode == model.InitModeFullOnly) {
		if exporter, ok := h.dispatcher.(dispatch.IPipelineStateExporter); ok {
			bundle, err = exporter.ExportPipelineState(ctx, id, side, newNodeID, *req.ConfigVersion)
		} else {
			err = repo.ErrPipelineMigrationSnapshotState
		}
	}
	var migration *repo.PipelineNodeMigration
	if err == nil {
		migration, err = repo.MigratePipelineNodeWithState(ctx, h.db, id, side, newNodeID, *req.ConfigVersion, bundle)
	}
	if err != nil {
		switch {
		case errors.Is(err, repo.ErrPipelineNotFound):
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		case errors.Is(err, repo.ErrPipelineConfigVersionConflict):
			Fail(c, http.StatusConflict, "pipeline_config_version_conflict", "pipeline 配置已被并发修改，请刷新后重试")
		case errors.Is(err, repo.ErrPipelineMigrationRequiresStopped):
			Fail(c, http.StatusConflict, "pipeline_not_stopped", "pipeline 必须停止后才能迁移节点")
		case errors.Is(err, repo.ErrPipelineMigrationSameNode):
			Fail(c, http.StatusBadRequest, "same_node", "目标节点与当前节点相同")
		case errors.Is(err, repo.ErrPipelineMigrationCenterMismatch):
			Fail(c, http.StatusBadRequest, "center_mismatch", "目标节点与端点不在同一中心")
		case errors.Is(err, repo.ErrPipelineMigrationNodeUnavailable):
			Fail(c, http.StatusConflict, "node_unavailable", "目标节点当前不可用于迁移")
		case errors.Is(err, repo.ErrPipelineMigrationCapability):
			Fail(c, http.StatusConflict, "node_migration_unsupported", "原节点和新节点均须支持 node_migration_v1，请先升级节点")
		case errors.Is(err, repo.ErrPipelineMigrationSnapshotState):
			Fail(c, http.StatusConflict, "snapshot_migration_unavailable", "无法取得原节点停止态的可信全量存档，请检查节点在线状态和能力后重试")
		case errors.Is(err, repo.ErrSourceRecoveryUnavailable):
			Fail(c, http.StatusConflict, "source_recovery_unavailable", "缺少当前运行的有效源检查点，不能迁移源节点")
		case errors.Is(err, repo.ErrNodePipelineQuota):
			Fail(c, http.StatusConflict, "node_pipeline_quota_exceeded", "目标节点已达到 pipeline 配额")
		case errors.Is(err, repo.ErrNodeNotFound), errors.Is(err, repo.ErrEndpointNotFound):
			Fail(c, http.StatusBadRequest, "invalid_reference", "目标节点或端点不存在")
		default:
			FailInternal(c, err, "migrate_pipeline_node_failed")
		}
		return
	}
	if err := migrator.NotifyNodeMigration(ctx, migration.PipelineID, migration.ConfigVersion, migration.Side, migration.OldNodeID, migration.NewNodeID); err != nil {
		// The database change is committed and remains authoritative. Surface the
		// dispatch failure so operators can retry the migration notification.
		FailInternal(c, err, "migrate_pipeline_node_dispatch_failed")
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineNodeMigrate
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	event.Changes, _ = json.Marshal(map[string]string{"role": string(side), "oldNodeId": migration.OldNodeID.String(), "newNodeId": migration.NewNodeID.String()})
	h.audit.Record(ctx, event)
	p, err = repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}
	OK(c, toPipelineView(ctx, h.db, *p, nodeHeartbeats{}))
}

func (h *pipelineHandler) setDesiredState(c *gin.Context, state model.DesiredState, action model.AuditAction) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	ctx := c.Request.Context()

	if err := h.writePipelineIntent(ctx, id, state, false); err != nil {
		h.failPipelineIntent(c, id, err)
		return
	}

	p, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = action
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	OK(c, toPipelineView(ctx, h.db, *p, nodeHeartbeats{}))
}

// Explicit retry advances command identity even when desired state is unchanged,
// resetting exhaustion and allowing another lifecycle attempt.
func (h *pipelineHandler) retry(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	ctx := c.Request.Context()
	if err := h.writePipelineIntent(ctx, id, "", true); err != nil {
		h.failPipelineIntent(c, id, err)
		return
	}

	p, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineRetry
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	OK(c, toPipelineView(ctx, h.db, *p, nodeHeartbeats{}))
}

// writePipelineIntent keeps authorization, cmd_seq and desired_state in one
// transaction. Graph mutations and confirmation take the same topology lock.
func (h *pipelineHandler) writePipelineIntent(ctx context.Context, id uuidx.UUID, state model.DesiredState, retry bool) error {
	return h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := repo.LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		// Re-read after waiting for graph edits and other lifecycle requests.
		p, err = repo.LockPipelineIncludingDeleted(ctx, tx, id)
		if err != nil {
			return err
		}
		want := state
		if retry {
			want = p.DesiredState
		}
		if p.DeletedAt.Valid {
			return repo.ErrPipelineNotFound
		}
		if err := p.ValidateDesiredTransition(want); err != nil {
			return err
		}
		if want == model.DesiredStateRunning {
			if err := pipelinespec.ValidateFullOnlyTarget(ctx, tx, p); err != nil {
				return err
			}
			if err := repo.CheckTimestampStart(ctx, tx, p); err != nil {
				return err
			}
			if err := repo.CheckTopologyStart(ctx, tx, p.TopologyID); err != nil {
				return err
			}
			if err := repo.CheckPrecheckStartTx(ctx, tx, p.ID, time.Now()); err != nil {
				return err
			}
		}
		transactionHandler := *h
		transactionHandler.db = tx
		if retry {
			return transactionHandler.bumpCmdSeqUnconditionally(ctx, id)
		}
		// The new command sequence must precede its desired state.
		if err := transactionHandler.bumpCmdSeqIfNewIntent(ctx, id, state); err != nil {
			return err
		}
		return repo.SetPipelineDesiredState(ctx, tx, id, state)
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}

func (h *pipelineHandler) failPipelineIntent(c *gin.Context, id uuidx.UUID, err error) {
	if errors.Is(err, pipelinespec.ErrFullOnlyTargetCapability) {
		Fail(c, http.StatusConflict, "full_only_target_capability_required", "目标节点需支持仅全量在 H 边界停止，请先升级目标节点")
		return
	}
	if errors.Is(err, model.ErrPipelineTransition) {
		Fail(c, http.StatusConflict, "invalid_pipeline_transition", "当前状态不允许此操作，未启动线路请先完成预检查并启动")
		return
	}

	if errors.Is(err, repo.ErrPrecheckNotFound) {
		Fail(c, http.StatusConflict, "precheck_required", "请先完成当前配置的预检查")
		return
	}
	if code := precheckErrorCode(err); code != "precheck_failed" && code != "" {
		Fail(c, http.StatusConflict, code, "当前预检查结果不允许启动，请刷新并处理检查项")
		return
	}
	if errors.Is(err, repo.ErrTimestampConfirmationRequired) {
		Fail(c, http.StatusConflict, "timestamp_confirmation_required", "请先预览并确认时间点对应的实际事务边界")
		return
	}
	if errors.Is(err, topologycheck.ErrBidirectionalInitialization) {
		Fail(c, http.StatusConflict, "bidirectional_initialization_conflict", "双向初始化只能由一个方向执行全量或结构迁移，反方向需使用仅增量：当前最新")
		return
	}
	if errors.Is(err, repo.ErrPipelineNotFound) || errors.Is(err, repo.ErrTopologyNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "pipeline 配置不存在")
		return
	}
	if errors.Is(err, repo.ErrTopologyConfirmationRequired) {
		p, readErr := repo.GetPipeline(c.Request.Context(), h.db, id)
		if readErr != nil {
			FailInternal(c, readErr, "get_pipeline_failed")
			return
		}
		analysis, readErr := repo.GetTopologyAnalysis(c.Request.Context(), h.db, p.TopologyID)
		if readErr != nil {
			FailInternal(c, readErr, "topology_analysis_failed")
			return
		}
		FailWithDetails(c, http.StatusConflict, "topology_confirmation_required", "回环或重复到达告警需管理员确认后才能启动", analysis)
		return
	}
	FailInternal(c, err, "pipeline_intent_failed")
}

// bumpCmdSeqUnconditionally 推进 cmd_seq，不判断意图是否变化——与
// bumpCmdSeqIfNewIntent 的唯一区别就是少了那个判断（见 retry 的文档）。
// CAS 冲突的处理方式一致：重新读当前值再试，不吞错。
func (h *pipelineHandler) bumpCmdSeqUnconditionally(ctx context.Context, id uuidx.UUID) error {
	for range maxCmdSeqBumpRetries {
		p, err := repo.GetPipeline(ctx, h.db, id)
		if err != nil {
			return err
		}
		if _, err := repo.BumpPipelineCmdSeq(ctx, h.db, id, p.LastCmdSeq); err != nil {
			if errors.Is(err, repo.ErrPipelineCmdSeqConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("http: 推进 pipeline cmd_seq 连续 %d 次 CAS 冲突，放弃", maxCmdSeqBumpRetries)
}

// maxCmdSeqBumpRetries 是 bumpCmdSeqIfNewIntent 在 repo.ErrPipelineCmdSeqConflict
// 上的最大重试次数。这里的冲突只来自"同一个 pipeline 上多个并发请求同时
// 触发新意图判定"这种预期内的良性竞争（见该函数文档），几次重试就应该
// 收敛；用一个有限上限而不是无限重试，是不让一个异常放大的并发场景把
// 单次 HTTP 请求拖成无限循环。
const maxCmdSeqBumpRetries = 5

// 判定逻辑本身：只有当
// 这次请求的目标状态与 pipeline 当前的 desired_state 不同（也就是真的
// 出现了新意图）时，才用 repo.BumpPipelineCmdSeq 推进一次 cmd_seq；目标
// 与当前一致（重复点击、幂等重放）不算新意图，不推进。
//
// CAS 冲突（repo.ErrPipelineCmdSeqConflict）时重新读取当前状态再判断一次
// ——不能吞掉这个错误直接放弃：那会让这次真正的新意图完全没有对应的
// cmd_seq 推进，对账循环会一直复用旧序号，等价于这次操作在命令面不可见
// 不需要把"读旧值判断变化 + CAS 递增"包进一个数据库
// 事务：重试循环里重新 GetPipeline 依然能正确判断"目标是否变化"，多余的
// 判断轮次只是浪费一点查询，不是错误，与 SyncPipeline 双递增无害论同一个
// 道理。
func (h *pipelineHandler) bumpCmdSeqIfNewIntent(ctx context.Context, id uuidx.UUID, wantState model.DesiredState) error {
	for range maxCmdSeqBumpRetries {
		p, err := repo.GetPipeline(ctx, h.db, id)
		if err != nil {
			return err
		}
		if p.DesiredState == wantState {
			return nil
		}
		if _, err := repo.BumpPipelineCmdSeq(ctx, h.db, id, p.LastCmdSeq); err != nil {
			if errors.Is(err, repo.ErrPipelineCmdSeqConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("http: 推进 pipeline cmd_seq 连续 %d 次 CAS 冲突，放弃", maxCmdSeqBumpRetries)
}
