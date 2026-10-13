package http

// GET /conflicts。
//
// 只读端点，不接审计（同 GET /logs、GET /events）。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type conflictView struct {
	ID           string         `json:"id"`
	Ts           time.Time      `json:"ts"`
	PipelineID   string         `json:"pipelineId"`
	SchemaName   string         `json:"schemaName"`
	Table        string         `json:"table"`
	PK           map[string]any `json:"pk"`
	ConflictType string         `json:"conflictType"`
	Resolution   string         `json:"resolution"`
	SourceRow    map[string]any `json:"sourceRow,omitempty"`
	TargetRow    map[string]any `json:"targetRow,omitempty"`
}

// decodeConflictJSONField 把 Conflict 的 PK/SourceRow/TargetRow 三个
// model.JSON 列解码成 map[string]any，空值返回 nil（不是空 map），配合
// SourceRow/TargetRow 字段的 omitempty 让"没有这份镜像"和"镜像是个空
// 对象"在响应里能区分开。
func decodeConflictJSONField(raw model.JSON, fieldName string) (map[string]any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var m map[string]any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&m); err != nil {
		return nil, fmt.Errorf("解析冲突记录 %s 失败: %w", fieldName, err)
	}
	return m, nil
}

func toConflictView(c model.Conflict) (conflictView, error) {
	pk, err := decodeConflictJSONField(c.PK, "pk")
	if err != nil {
		return conflictView{}, err
	}
	sourceRow, err := decodeConflictJSONField(c.SourceRow, "source_row")
	if err != nil {
		return conflictView{}, err
	}
	targetRow, err := decodeConflictJSONField(c.TargetRow, "target_row")
	if err != nil {
		return conflictView{}, err
	}
	return conflictView{
		ID:           c.ID.String(),
		Ts:           c.Ts,
		PipelineID:   c.PipelineID.String(),
		SchemaName:   c.SchemaName,
		Table:        c.Table,
		PK:           pk,
		ConflictType: string(c.ConflictType),
		Resolution:   string(c.Resolution),
		SourceRow:    sourceRow,
		TargetRow:    targetRow,
	}, nil
}

type conflictHandler struct {
	db *gorm.DB
}

// maxConflictFetchRows 理由同 maxLogFetchRows/maxEventFetchRows：
// repo.ListConflicts 只提供游标分页（Limit + BeforeTs/BeforeID），这一层
// 为了支持页码翻页退化成"取前 page*pageSize 条、再切出这一页"，需要一个
// 上限防止深翻页把单次查询行数放大到不合理的程度。
const maxConflictFetchRows = 5000

// registerConflictRoutes 挂载 GET /conflicts，查询复制目标端上报的冲突记录。
func registerConflictRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &conflictHandler{db: db}
	api.GET("/conflicts", h.list)
}

func (h *conflictHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	var q repo.ConflictQuery
	if v := c.Query("since"); v != "" {
		since, err := time.Parse(time.RFC3339, v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_time_range", "since 格式不合法，需为 RFC3339")
			return
		}
		q.Since = since
	}
	if v := c.Query("until"); v != "" {
		until, err := time.Parse(time.RFC3339, v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_time_range", "until 格式不合法，需为 RFC3339")
			return
		}
		q.Until = until
	}
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		Fail(c, http.StatusBadRequest, "invalid_time_range", "until 不能早于 since")
		return
	}
	if v := c.Query("pipelineId"); v != "" {
		pipelineID, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_pipeline_id", "pipelineId 格式不合法")
			return
		}
		q.PipelineID = pipelineID
	}
	if v := strings.TrimSpace(c.Query("conflictType")); v != "" {
		conflictType := model.ConflictType(v)
		if !conflictType.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_conflict_type", "conflictType 取值不合法")
			return
		}
		q.ConflictType = conflictType
	}
	if v := strings.TrimSpace(c.Query("resolution")); v != "" {
		resolution := model.ConflictResolution(v)
		if !resolution.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_resolution", "resolution 取值不合法")
			return
		}
		q.Resolution = resolution
	}

	// 与 internal/manager/http/logs.go 与 internal/manager/http/events.go 的 list 同一个"取前 page*pageSize 条、再切出
	// 这一页"退化方案，理由见 maxConflictFetchRows 的注释。
	fetchRows := page * pageSize
	if fetchRows > maxConflictFetchRows {
		Fail(c, http.StatusBadRequest, "page_too_deep", "翻页跨度过大，请缩小时间范围或增加过滤条件后重试")
		return
	}
	q.Limit = fetchRows

	ctx := c.Request.Context()
	rows, err := repo.ListConflicts(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "list_conflicts_failed")
		return
	}

	start := (page - 1) * pageSize
	var pageRows []model.Conflict
	if start < len(rows) {
		end := min(start+pageSize, len(rows))
		pageRows = rows[start:end]
	}

	// 真正的总数——不是像 countLogsForWindow/countEventsForWindow 那样对
	// model.Conflict 的绕过查询，repo.CountConflicts 是仓储层已经交付的
	// 能力，且与 ListConflicts 共用同一份 applyConflictFilters，不会出现
	// "列表用一套过滤、总数用另一套过滤"的漂移风险。
	total, err := repo.CountConflicts(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "count_conflicts_failed")
		return
	}

	views := make([]conflictView, 0, len(pageRows))
	for _, row := range pageRows {
		view, err := toConflictView(row)
		if err != nil {
			FailInternal(c, err, "decode_conflict_failed")
			return
		}
		views = append(views, view)
	}

	OK(c, paginated[conflictView]{
		Items:    views,
		Total:    int(total),
		Page:     page,
		PageSize: pageSize,
	})
}
