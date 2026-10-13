package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// 注册码列表隐藏已消费的凭据，避免继续显示可注册的误导状态。
const defaultRegisterCodeValidity = 24 * time.Hour

// registerCodeView 是 GET/POST /register-codes 响应体里单条注册码的对外
// 形状，字段与 web/src/api/nodes.ts 的 RegisterCode 类型逐一对齐（camelCase
// JSON key；model.NodeRegisterCode 的 gorm/json 标签是 snake_case，供落库
// 用，不能直接拿去序列化给前端）。不含 CodeHash——model.NodeRegisterCode
// 本身也用 json:"-" 防止意外携带，这里的 view 结构体从类型层面再确认一次
// 不存在这个字段。
type registerCodeView struct {
	ID           string     `json:"id"`
	CenterID     string     `json:"centerId"`
	PresetName   string     `json:"presetName"`
	PresetLabels []string   `json:"presetLabels"`
	ExpiresAt    time.Time  `json:"expiresAt"`
	UsedCount    int        `json:"usedCount"`
	CreatedBy    string     `json:"createdBy"`
	CreatedAt    time.Time  `json:"createdAt"`
	RevokedAt    *time.Time `json:"revokedAt"`
}

// registerCodeCreatedView 是 POST /register-codes 的响应体：在
// registerCodeView 基础上多一个明文 Code 字段，只在这一次响应里出现
// （web/src/api/nodes.ts 的 RegisterCodeCreated 注释：调用方只应在内存态
// 短暂持有，不得写 localStorage/日志/URL）。本文件里任何 slog 调用都不会
// 涉及这个字段，仅通过 OK() 直接写回响应体一次。
type registerCodeCreatedView struct {
	registerCodeView
	Code string `json:"code"`
}

// toRegisterCodeView 把落库记录转成对外视图；PresetLabels 需要从
// model.JSON 解码成 []string。
func toRegisterCodeView(rec model.NodeRegisterCode) (registerCodeView, error) {
	labels, err := decodeLabels(rec.PresetLabels)
	if err != nil {
		return registerCodeView{}, err
	}
	return registerCodeView{
		ID:           rec.ID.String(),
		CenterID:     rec.CenterID.String(),
		PresetName:   rec.PresetName,
		PresetLabels: labels,
		ExpiresAt:    rec.ExpiresAt,
		UsedCount:    rec.UsedCount,
		CreatedBy:    rec.CreatedBy.String(),
		CreatedAt:    rec.CreatedAt,
		RevokedAt:    rec.RevokedAt,
	}, nil
}

// registerCodeCreateRequest 对齐 web/src/api/nodes.ts 的
// RegisterCodeCreateInput。**不接受 maxUses**：「注册码创建」
// 一行明确「无 max_uses 参数，服务端固定为 1（需求 O-04）」，
// repo.GenerateNodeRegisterCode 本身也不接受这个参数——这里的请求结构体
// 从类型层面确认不会意外接受它。
type registerCodeCreateRequest struct {
	CenterID     string     `json:"centerId" binding:"required"`
	ExpiresAt    *time.Time `json:"expiresAt"`
	PresetName   string     `json:"presetName"`
	PresetLabels []string   `json:"presetLabels"`
}

type registerCodeHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
}

func registerRegisterCodeRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder) {
	h := &registerCodeHandler{db: db, audit: audit}
	api.GET("/register-codes", h.list)
	api.POST("/register-codes", RequireRole(auth.RoleAdmin), h.create)
	api.DELETE("/register-codes/:id", h.revoke)
}

// list 处理 GET /register-codes?centerId=...（centerId 可选，省略时不按
// 中心过滤，对齐 repo.ListNodeRegisterCodes 用 uuidx.Nil 表示不过滤的
// 约定）。不分页：web/src/api/nodes.ts 的 listRegisterCodes 声明的返回类型
// 就是 RegisterCode[]，不是 Paginated<RegisterCode>——注册码数量天然很少
// （每次注册后即耗尽），没有分页的必要。
func (h *registerCodeHandler) list(c *gin.Context) {
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

	recs, err := repo.ListNodeRegisterCodes(c.Request.Context(), h.db, centerID)
	if err != nil {
		FailInternal(c, err, "list_register_codes_failed")
		return
	}

	views := make([]registerCodeView, 0, len(recs))
	for _, rec := range recs {
		view, err := toRegisterCodeView(rec)
		if err != nil {
			FailInternal(c, err, "decode_register_code_failed")
			return
		}
		views = append(views, view)
	}
	OK(c, views)
}

// create 处理 POST /register-codes（需求 MGR-NODE-01）。明文注册码只在这
// 一次响应里出现，之后无法再从任何接口取回（库中只存哈希）——调用方必须
// 当次展示给运维、由运维自行妥善保存。
func (h *registerCodeHandler) create(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	var req registerCodeCreateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}

	centerID, err := uuidx.Parse(req.CenterID)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_center_id", "centerId 格式不合法")
		return
	}

	createdBy, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		FailInternal(c, err, "invalid_session_user_id")
		return
	}

	expiresAt := time.Now().Add(defaultRegisterCodeValidity)
	if req.ExpiresAt != nil {
		expiresAt = *req.ExpiresAt
	}

	presetLabels, err := encodeLabels(req.PresetLabels)
	if err != nil {
		FailInternal(c, err, "encode_preset_labels_failed")
		return
	}

	ctx := c.Request.Context()
	plainCode, rec, err := repo.GenerateNodeRegisterCode(ctx, h.db, repo.NewNodeRegisterCodeParams{
		CenterID:     centerID,
		ExpiresAt:    expiresAt,
		PresetName:   req.PresetName,
		PresetLabels: presetLabels,
		CreatedBy:    createdBy,
	})
	if err != nil {
		FailInternal(c, err, "generate_register_code_failed")
		return
	}

	view, err := toRegisterCodeView(*rec)
	if err != nil {
		FailInternal(c, err, "decode_register_code_failed")
		return
	}

	username, _ := c.Get(ctxKeyUsername)
	usernameStr, _ := username.(string)
	h.audit.Record(ctx, AuditEvent{
		UserID:     UserIDFromContext(c),
		Username:   usernameStr,
		Action:     model.AuditActionRegisterCodeCreate,
		TargetType: model.AuditTargetTypeNodeRegisterCode,
		TargetID:   rec.ID.String(),
		Success:    true,
		ClientIP:   c.ClientIP(),
		UserAgent:  c.Request.UserAgent(),
	})

	OK(c, registerCodeCreatedView{registerCodeView: view, Code: plainCode})
}

// revoke 处理 DELETE /register-codes/{id}（需求 MGR-NODE-02）。作废只阻止
// 后续用它注册新节点，不影响已经用该码注册成功的节点（口令与注册状态
// 解耦，repo.RevokeNodeRegisterCode 的注释里也是这个约定）。幂等：重复
// 作废同一个 id 不报错。
func (h *registerCodeHandler) revoke(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}

	ctx := c.Request.Context()
	if err := repo.RevokeNodeRegisterCode(ctx, h.db, id, time.Now()); err != nil {
		if errors.Is(err, repo.ErrRegisterCodeNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "注册码不存在")
			return
		}
		FailInternal(c, err, "revoke_register_code_failed")
		return
	}

	username, _ := c.Get(ctxKeyUsername)
	usernameStr, _ := username.(string)
	h.audit.Record(ctx, AuditEvent{
		UserID:     UserIDFromContext(c),
		Username:   usernameStr,
		Action:     model.AuditActionRegisterCodeRevoke,
		TargetType: model.AuditTargetTypeNodeRegisterCode,
		TargetID:   id.String(),
		Success:    true,
		ClientIP:   c.ClientIP(),
		UserAgent:  c.Request.UserAgent(),
	})

	OK(c, gin.H{})
}
