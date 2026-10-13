package http

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// centerView 对齐 web/src/api/centers.ts 的 CenterSummary。**不含**
// mqMgmtPassword：「中心」一行明确"列表与详情不返回
// mq_mgmt_password（只写不读）"，model.Center 本身也用 json:"-" 防止意外
// 携带，这里的 view 结构体从类型层面再确认一次不存在这个字段。
//
// region/description/mqMgmtUrl/mqMgmtUser 在 model.Center 里都是非空
// varchar（未设置时是空字符串，不是 SQL NULL），前端类型声明的
// `string | null` 只是允许 null，不要求；空字符串同样满足这个联合类型，
// 不需要额外转换成指针。inspectorNodeId 是例外：uuidx.Nil（未指定）如果
// 直接 .String() 会输出全零 UUID 的字符串形式，看起来像一个真实存在的
// 节点引用，必须显式转换成 nil 才对应前端的 null。
type centerView struct {
	ID              string    `json:"id"`
	Code            string    `json:"code"`
	Name            string    `json:"name"`
	Region          string    `json:"region"`
	Description     string    `json:"description"`
	IsManagerCenter bool      `json:"isManagerCenter"`
	InspectorNodeID *string   `json:"inspectorNodeId"`
	MQMgmtURL       string    `json:"mqMgmtUrl"`
	MQMgmtUser      string    `json:"mqMgmtUser"`
	OriginUUIDCount int       `json:"originUuidCount"`
	CreatedAt       time.Time `json:"createdAt"`
}

func toCenterView(c model.Center, originUUIDCount int) centerView {
	var inspectorID *string
	if !c.InspectorNodeID.IsZero() {
		s := c.InspectorNodeID.String()
		inspectorID = &s
	}
	return centerView{
		ID:              c.ID.String(),
		Code:            c.Code,
		Name:            c.Name,
		Region:          c.Region,
		Description:     c.Description,
		IsManagerCenter: c.IsManagerCenter,
		InspectorNodeID: inspectorID,
		MQMgmtURL:       c.MQMgmtURL,
		MQMgmtUser:      c.MQMgmtUser,
		OriginUUIDCount: originUUIDCount,
		CreatedAt:       c.CreatedAt,
	}
}

// originUUIDView 对齐 web/src/api/centers.ts 的 OriginUuid。
type originUUIDView struct {
	ID        string    `json:"id"`
	CenterID  string    `json:"centerId"`
	UUID      string    `json:"uuid"`
	Kind      string    `json:"kind"`
	RefID     string    `json:"refId"`
	RefName   string    `json:"refName,omitempty"`
	RefType   string    `json:"refType,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
}

func toOriginUUIDView(rec model.CenterOriginUUID) originUUIDView {
	return originUUIDView{
		ID:        rec.ID.String(),
		CenterID:  rec.CenterID.String(),
		UUID:      rec.UUID.String(),
		Kind:      string(rec.Kind),
		RefID:     rec.RefID,
		CreatedAt: rec.CreatedAt,
	}
}

type centerCreateRequest struct {
	Code            string `json:"code" binding:"required"`
	Name            string `json:"name" binding:"required"`
	Region          string `json:"region"`
	Description     string `json:"description"`
	IsManagerCenter bool   `json:"isManagerCenter"`
}

// centerUpdateRequest 对齐 CenterUpdateInput & Partial<CenterBrokerInput>：
// 全部字段可选，PATCH 语义（只覆盖请求里出现的字段），用指针区分「未提供」
// 与「显式设为零值」。
type centerUpdateRequest struct {
	Code            *string `json:"code"`
	Name            *string `json:"name"`
	Region          *string `json:"region"`
	Description     *string `json:"description"`
	IsManagerCenter *bool   `json:"isManagerCenter"`
	MQMgmtURL       *string `json:"mqMgmtUrl"`
	MQMgmtUser      *string `json:"mqMgmtUser"`
	MQMgmtPassword  *string `json:"mqMgmtPassword"`
}

// originUUIDCreateRequest 对齐 OriginUuidInput：只接受 uuid，服务端固定按
// kind='manual' 建（「POST /centers/{id}/origin-uuids」一行），
// 不接受调用方指定 kind——这个结构体从类型层面确认不会意外接受它。
type originUUIDCreateRequest struct {
	UUID string `json:"uuid" binding:"required"`
}

// setInspectorRequest 对齐 setInspectorNode：nodeId 为 null 表示恢复自动
// 选择（「PUT /centers/{id}/inspector」一行）。
type setInspectorRequest struct {
	NodeID *string `json:"nodeId"`
}

type centerHandler struct {
	db         *gorm.DB
	audit      IAuditRecorder
	dispatcher dispatch.IPipelineDispatcher
}

// registerCenterRoutes 挂载中心 CRUD、来源身份 UUID 集合管理、巡检节点
// 指派。
func registerCenterRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder, dispatcher dispatch.IPipelineDispatcher) {
	h := &centerHandler{db: db, audit: audit, dispatcher: dispatcher}
	api.GET("/centers", h.list)
	api.POST("/centers", h.create)
	api.GET("/centers/:id", h.get)
	api.PATCH("/centers/:id", h.update)
	api.DELETE("/centers/:id", h.delete)
	api.GET("/centers/:id/origin-uuids", h.listOriginUUIDs)
	api.POST("/centers/:id/origin-uuids", h.addOriginUUID)
	api.DELETE("/centers/:id/origin-uuids/:uuidId", h.deleteOriginUUID)
	api.PUT("/centers/:id/inspector", h.setInspector)
}

// originUUIDCountForCenter 是 originUuidCount 字段的取值方式：目前 repo
// 没有专门的计数函数，中心数量天然很少（每套部署通常几个到十几个），逐个
// 中心查一次来源身份列表取长度足够，不值得为这一个统计字段新增仓储方法。
func originUUIDCountForCenter(c *gin.Context, db *gorm.DB, id uuidx.UUID) (int, error) {
	recs, err := repo.ListCenterOriginUUIDs(c.Request.Context(), db, id)
	if err != nil {
		return 0, err
	}
	return len(recs), nil
}

// 应当只有一行、且落在本函数体内；出现第二行就说明本注释已经过期。
//
// 中心
// 本身不直接被任何 pipeline 引用（model.Center 没有 topology_id 字段，
// repo.GetTopologyGraph 反过来是从 pipeline 的 source/target 端点各自的
// center_id 推出"这条拓扑涉及哪些中心"，见该函数实现），所以"哪些
// pipeline 受这个中心影响"要经过它名下的端点这一跳：先
// repo.ListEndpoints(centerID) 拿到该中心全部端点，再对每个端点调
// repo.ListPipelinesByEndpoint（MGR-DS-05 已经在用的同一个查询），去重
// 后逐条调用 SyncPipeline。
func (h *centerHandler) syncPipelinesForCenter(ctx context.Context, centerID uuidx.UUID) {
	endpoints, err := repo.ListEndpoints(ctx, h.db, centerID)
	if err != nil {
		slog.Error("dispatch: list endpoints for center cascade failed",
			slog.String("center_id", centerID.String()), slog.Any("error", err))
		return
	}

	affected := make(map[uuidx.UUID]struct{})
	for _, ep := range endpoints {
		pipelines, err := repo.ListPipelinesByEndpoint(ctx, h.db, ep.ID)
		if err != nil {
			slog.Error("dispatch: list pipelines by endpoint failed",
				slog.String("endpoint_id", ep.ID.String()), slog.Any("error", err))
			continue
		}
		for _, p := range pipelines {
			affected[p.ID] = struct{}{}
		}
	}
	for pipelineID := range affected {
		syncPipelineBestEffort(ctx, h.dispatcher, pipelineID)
	}
}

func (h *centerHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	centers, err := repo.ListCenters(c.Request.Context(), h.db)
	if err != nil {
		FailInternal(c, err, "list_centers_failed")
		return
	}
	views := make([]centerView, 0, len(centers))
	for _, center := range centers {
		count, err := originUUIDCountForCenter(c, h.db, center.ID)
		if err != nil {
			FailInternal(c, err, "count_origin_uuids_failed")
			return
		}
		views = append(views, toCenterView(center, count))
	}
	OK(c, views)
}

func (h *centerHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	center, err := repo.GetCenter(c.Request.Context(), h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrCenterNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "中心不存在")
			return
		}
		FailInternal(c, err, "get_center_failed")
		return
	}
	count, err := originUUIDCountForCenter(c, h.db, id)
	if err != nil {
		FailInternal(c, err, "count_origin_uuids_failed")
		return
	}
	OK(c, toCenterView(*center, count))
}

func (h *centerHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req centerCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	center := &model.Center{
		Code:            req.Code,
		Name:            req.Name,
		Region:          req.Region,
		Description:     req.Description,
		IsManagerCenter: req.IsManagerCenter,
	}
	if err := repo.CreateCenter(c.Request.Context(), h.db, center); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		if errors.Is(err, repo.ErrCenterCodeConflict) {
			Fail(c, http.StatusConflict, "center_code_conflict", "中心编码已存在")
			return
		}
		FailInternal(c, err, "create_center_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterCreate
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = center.ID.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, toCenterView(*center, 0))
}

func (h *centerHandler) update(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req centerUpdateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	ctx := c.Request.Context()
	current, err := repo.GetCenter(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrCenterNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "中心不存在")
			return
		}
		FailInternal(c, err, "get_center_failed")
		return
	}

	if req.Code != nil {
		current.Code = *req.Code
	}
	if req.Name != nil {
		current.Name = *req.Name
	}
	if req.Region != nil {
		current.Region = *req.Region
	}
	if req.Description != nil {
		current.Description = *req.Description
	}
	if req.IsManagerCenter != nil {
		current.IsManagerCenter = *req.IsManagerCenter
	}
	if req.MQMgmtURL != nil {
		current.MQMgmtURL = *req.MQMgmtURL
	}
	if req.MQMgmtUser != nil {
		current.MQMgmtUser = *req.MQMgmtUser
	}
	if req.MQMgmtPassword != nil {
		current.MQMgmtPassword = *req.MQMgmtPassword
	}

	if err := repo.UpdateCenter(ctx, h.db, current); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		switch {
		case errors.Is(err, repo.ErrCenterCodeConflict):
			Fail(c, http.StatusConflict, "center_code_conflict", "中心编码已存在")
		case errors.Is(err, repo.ErrCenterVersionConflict):
			Fail(c, http.StatusConflict, "center_version_conflict", "中心已被并发修改，请刷新后重试")
		case errors.Is(err, repo.ErrCenterNotFound):
			Fail(c, http.StatusNotFound, "not_found", "中心不存在")
		default:
			FailInternal(c, err, "update_center_failed")
		}
		return
	}

	count, err := originUUIDCountForCenter(c, h.db, id)
	if err != nil {
		FailInternal(c, err, "count_origin_uuids_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterUpdate
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	// Region/Description/MQMgmt* 三类字段与 assembled 内容无关，级联到的
	// pipeline 会在 dispatch 自己的摘要比对里被正确判定为 no-op，这里不
	// 逐字段判断"这次改的是不是 Code/IsManagerCenter"——判断逻辑已经统一
	// 在 dispatch 里，见 syncPipelinesForCenter 的注释。
	h.syncPipelinesForCenter(ctx, id)

	OK(c, toCenterView(*current, count))
}

// 删除中心时校验其端点、节点、流水线等引用；存在引用时拒绝删除。
func (h *centerHandler) delete(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if err := repo.DeleteCenter(c.Request.Context(), h.db, id); err != nil {
		switch {
		case errors.Is(err, repo.ErrCenterNotFound):
			Fail(c, http.StatusNotFound, "not_found", "中心不存在")
		case errors.Is(err, repo.ErrCenterHasDependents):
			Fail(c, http.StatusConflict, "center_has_dependents", err.Error())
		default:
			FailInternal(c, err, "delete_center_failed")
		}
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterDelete
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}

func (h *centerHandler) listOriginUUIDs(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	centerID, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	recs, err := repo.ListCenterOriginUUIDs(c.Request.Context(), h.db, centerID)
	if err != nil {
		FailInternal(c, err, "list_origin_uuids_failed")
		return
	}
	views := make([]originUUIDView, 0, len(recs))
	for _, rec := range recs {
		views = append(views, toOriginUUIDView(rec))
	}
	if err := h.resolveOriginReferences(c.Request.Context(), centerID, views); err != nil {
		FailInternal(c, err, "list_origin_references_failed")
		return
	}
	OK(c, views)
}

func (h *centerHandler) addOriginUUID(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	centerID, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req originUUIDCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	uuidVal, err := uuidx.Parse(req.UUID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_uuid", "uuid 格式不合法")
		return
	}

	rec := &model.CenterOriginUUID{
		CenterID: centerID,
		UUID:     uuidVal,
		Kind:     model.OriginKindManual,
	}
	if err := repo.CreateCenterOriginUUID(c.Request.Context(), h.db, rec); err != nil {
		if errors.Is(err, repo.ErrOriginUUIDConflict) {
			Fail(c, http.StatusConflict, "origin_uuid_conflict", "该 UUID 已被登记为来源身份")
			return
		}
		FailInternal(c, err, "create_origin_uuid_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterOriginUUIDAdd
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = centerID.String()
	h.audit.Record(c.Request.Context(), event)

	h.syncPipelinesForCenter(c.Request.Context(), centerID)

	OK(c, toOriginUUIDView(*rec))
}

func (h *centerHandler) deleteOriginUUID(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	// Delete by the UUID record identity. This route does not impose center-scoped
	// authorization; adding it requires checking the record against the parent center.
	uuidID, err := uuidx.Parse(c.Param("uuidId"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}

	ctx := c.Request.Context()
	// 删除前先读出这条记录归属的 center_id——删除后就没有别的办法知道
	// 该级联哪个中心了（这里仍然不用它做请求路径校验，只用来做下面的
	// dispatch 级联，与本函数一贯"不校验 centerId"的取舍不冲突）。
	before, err := repo.GetCenterOriginUUID(ctx, h.db, uuidID)
	if err != nil {
		if errors.Is(err, repo.ErrOriginUUIDNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "来源身份记录不存在")
			return
		}
		FailInternal(c, err, "get_origin_uuid_failed")
		return
	}

	if err := repo.DeleteCenterOriginUUID(ctx, h.db, uuidID); err != nil {
		if errors.Is(err, repo.ErrOriginUUIDNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "来源身份记录不存在")
			return
		}
		FailInternal(c, err, "delete_origin_uuid_failed")
		return
	}

	// TargetID 用来源身份记录自身的主键（uuidId），而不是父中心 id——
	// 上面的注释已经说明这个 handler 故意不解析/校验 centerId，审计的
	// 目标对象应该是「被删掉的是哪条记录」，这个信息只有 uuidID 能精确
	// 表达。
	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterOriginUUIDRemove
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = uuidID.String()
	h.audit.Record(ctx, event)

	h.syncPipelinesForCenter(ctx, before.CenterID)

	OK(c, gin.H{})
}

// setInspector 处理 PUT /centers/{id}/inspector。repo 没有专门的原子
// setter，UpdateCenter 的白名单里已经包含 inspector_node_id，复用它——读取当前记录、只改
// InspectorNodeID 一个字段、按乐观锁写回，语义与其它 PATCH 字段更新完全
// 一致，没有必要为这一个字段单独写一条 UPDATE 语句。
//
// 不调用 dispatch：InspectorNodeID 只影响巡检探测派发到哪个节点
// （task.InspectionLoop 的选址逻辑），pipelinespec.Assemble 全程不读这个
// 字段，与任何 pipeline 的 assembled 内容无关。
func (h *centerHandler) setInspector(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req setInspectorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	ctx := c.Request.Context()

	current, err := repo.GetCenter(ctx, h.db, id)
	if err != nil {
		if errors.Is(err, repo.ErrCenterNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "中心不存在")
			return
		}
		FailInternal(c, err, "get_center_failed")
		return
	}

	nodeID := uuidx.Nil
	if req.NodeID != nil {
		parsed, err := uuidx.Parse(*req.NodeID)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_node_id", "nodeId 格式不合法")
			return
		}
		node, err := repo.GetNode(ctx, h.db, parsed)
		if err != nil {
			if errors.Is(err, repo.ErrNodeNotFound) {
				Fail(c, http.StatusBadRequest, "node_not_found", "nodeId 对应的节点不存在")
				return
			}
			FailInternal(c, err, "get_node_failed")
			return
		}
		if node.CenterID != id {
			Fail(c, http.StatusBadRequest, "inspector_center_mismatch", "节点与中心不属于同一中心，不能指派为巡检节点（C-19）")
			return
		}
		nodeID = parsed
	}

	current.InspectorNodeID = nodeID

	if err := repo.UpdateCenter(ctx, h.db, current); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		if errors.Is(err, repo.ErrCenterVersionConflict) {
			Fail(c, http.StatusConflict, "center_version_conflict", "中心已被并发修改，请刷新后重试")
			return
		}
		FailInternal(c, err, "set_inspector_failed")
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionCenterInspectorAssign
	event.TargetType = model.AuditTargetTypeCenter
	event.TargetID = id.String()
	h.audit.Record(ctx, event)

	OK(c, gin.H{})
}
