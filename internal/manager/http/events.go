package http

// GET /events。

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// Payload 解码成 map[string]any 而不是原样透传 model.JSON 原始字节：
// 本包其它地方处理 model.JSON 列（decodeLabels、decodeNodeRuntime）也都
// 是解码成具体 Go 类型再放进 DTO，不直接暴露 model.JSON 本身，这里保持
// 同一个约定。Payload 的 schema 随事件类型而不同（model.Event 的注释），
// 没有像 model.NodeRuntime 那样的固定形状可以解码，map[string]any 是"结构
// 可变、但仍是具体解码结果"这一约束下最接近的选择。
type eventView struct {
	ID         string    `json:"id"`
	Ts         time.Time `json:"ts"`
	Type       string    `json:"type"`
	Level      string    `json:"level"`
	NodeID     *string   `json:"nodeId"`
	PipelineID *string   `json:"pipelineId"`

	AckedBy *string `json:"ackedBy"`
	// 存量形状：`ackedBy` 非 nil 而 `ackedAt` 为 nil，是真实且永久的。
	// v43 迁移（`internal/manager/repo/ddl_event_acked_at.go`）只加列、不回填——回填只能
	// 编造时间。所以不要写任何"有 ackedBy 就有 ackedAt"的断言，前端对这种
	// 行的表示法是「已确认（时间不详）」。
	AckedAt *time.Time     `json:"ackedAt"`
	Payload map[string]any `json:"payload,omitempty"`
}

func toEventView(e model.Event) (eventView, error) {
	var nodeID *string
	if !e.NodeID.IsZero() {
		s := e.NodeID.String()
		nodeID = &s
	}
	var pipelineID *string
	if !e.PipelineID.IsZero() {
		s := e.PipelineID.String()
		pipelineID = &s
	}
	var ackedBy *string
	if !e.AckedBy.IsZero() {
		s := e.AckedBy.String()
		ackedBy = &s
	}
	var payload map[string]any
	if len(e.Payload) > 0 {
		if err := json.Unmarshal(e.Payload, &payload); err != nil {
			return eventView{}, fmt.Errorf("解析事件 payload 失败: %w", err)
		}
	}
	return eventView{
		ID:         e.ID.String(),
		Ts:         e.Ts,
		Type:       string(e.Type),
		Level:      string(e.Level),
		NodeID:     nodeID,
		PipelineID: pipelineID,
		AckedBy:    ackedBy,
		AckedAt:    e.AckedAt,
		Payload:    payload,
	}, nil
}

// eventListView 是 GET /events 的响应体，形状对齐 logListView（见其注释：
// 因为要携带 since/until，不复用共享的 paginated[T]）。
type eventListView struct {
	Items    []eventView `json:"items"`
	Total    int         `json:"total"`
	Page     int         `json:"page"`
	PageSize int         `json:"pageSize"`
	Since    time.Time   `json:"since"`
	Until    time.Time   `json:"until"`
}

type eventHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
}

const (
	// defaultEventWindow 对齐 defaultLogWindow：观测类端点默认展示"最近一
	// 天"，两个端点的界面预期一致。
	defaultEventWindow = 24 * time.Hour
	// maxEventFetchRows 对齐 maxLogFetchRows 的理由：repo.ListEvents 同样
	// 只提供游标分页（Limit + BeforeTs/BeforeID），这一层为了支持页码翻页
	// 退化成"取前 page*pageSize 条、再切出这一页"，需要一个上限防止深翻页
	// 把单次查询行数放大到不合理的程度。
	maxEventFetchRows = 5000
)

func registerEventRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder) {
	h := &eventHandler{db: db, audit: audit}
	api.GET("/events", h.list)
	api.POST("/events/:id/ack", h.ack)
}

// 事件确认允许请求重试：自己重复确认成功，他人已确认返回冲突，记录不存在返回 404。
func (h *eventHandler) ack(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	// 会话鉴权中间件已在 /api/v1 上游拦截未认证请求，走到这里取不到用户
	// 是装配错误而不是客户端错误，所以是 500 而不是 401。
	userID, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil {
		FailInternal(c, err, "ack_event_no_authenticated_user")
		return
	}
	// 挡在这里是为了诊断分得开，不是为了改状态码：两条都是 500，
	// 因为走到这里取不到合法用户是装配错误，不是客户端错误。
	if userID.IsZero() {
		FailInternal(c, errors.New("会话里的用户 ID 是零值"), "ack_event_zero_user_id")
		return
	}

	if err := repo.AckEvent(c.Request.Context(), h.db, id, userID, time.Now()); err != nil {
		switch {
		case errors.Is(err, repo.ErrEventNotFound):
			Fail(c, http.StatusNotFound, "not_found", "事件不存在")
		case errors.Is(err, repo.ErrEventAlreadyAcked):
			Fail(c, http.StatusConflict, "event_already_acked", "事件已被他人确认")
		default:
			FailInternal(c, err, "ack_event_failed")
		}
		return
	}

	event := baseAuditEvent(c)
	event.Action = model.AuditActionEventAck
	event.TargetType = model.AuditTargetTypeEvent
	event.TargetID = id.String()
	h.audit.Record(c.Request.Context(), event)

	OK(c, gin.H{})
}

func (h *eventHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	// maxWindow 传 0：只用来补默认窗口，不设跨度上限（文件顶部注释 1）。
	since, until, err := resolveTimeRangeQuery(c.Query("since"), c.Query("until"), time.Now(),
		defaultEventWindow, 0, "")
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_time_range", err.Error())
		return
	}

	q := repo.EventQuery{Since: since, Until: until}
	if v := c.Query("nodeId"); v != "" {
		nodeID, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_node_id", "nodeId 格式不合法")
			return
		}
		q.NodeID = nodeID
	}
	if v := c.Query("pipelineId"); v != "" {
		pipelineID, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_pipeline_id", "pipelineId 格式不合法")
			return
		}
		q.PipelineID = pipelineID
	}
	if v := strings.TrimSpace(c.Query("type")); v != "" {
		eventType := model.EventType(v)
		if !eventType.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_type", "type 取值不合法")
			return
		}
		q.Type = eventType
	}
	if v := strings.TrimSpace(c.Query("level")); v != "" {
		level := model.EventLevel(v)
		if !level.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_level", "level 取值不合法")
			return
		}
		q.Level = level
	}
	if v := strings.TrimSpace(c.Query("onlyUnacked")); v != "" {
		onlyUnacked, err := strconv.ParseBool(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_only_unacked", "onlyUnacked 取值不合法，必须是 true/false")
			return
		}
		q.OnlyUnacked = onlyUnacked
	}

	// 与 internal/manager/http/logs.go 的 list 同一个"取前 page*pageSize 条、再切出这一页"退化
	// 方案，理由见 maxEventFetchRows 的注释。
	fetchRows := page * pageSize
	if fetchRows > maxEventFetchRows {
		Fail(c, http.StatusBadRequest, "page_too_deep", "翻页跨度过大，请缩小时间范围或增加过滤条件后重试")
		return
	}
	q.Limit = fetchRows

	ctx := c.Request.Context()
	rows, err := repo.ListEvents(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "list_events_failed")
		return
	}

	start := (page - 1) * pageSize
	var pageRows []model.Event
	if start < len(rows) {
		end := min(start+pageSize, len(rows))
		pageRows = rows[start:end]
	}

	total, err := repo.CountEvents(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "count_events_failed")
		return
	}

	views := make([]eventView, 0, len(pageRows))
	for _, row := range pageRows {
		view, err := toEventView(row)
		if err != nil {
			FailInternal(c, err, "decode_event_failed")
			return
		}
		views = append(views, view)
	}

	OK(c, eventListView{
		Items:    views,
		Total:    int(total),
		Page:     page,
		PageSize: pageSize,
		Since:    since,
		Until:    until,
	})
}
