package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// logEntryView 对齐 web/src/api/logs.ts 的 LogEntry。
type logEntryView struct {
	ID         string    `json:"id"`
	Ts         time.Time `json:"ts"`
	Level      string    `json:"level"`
	Module     string    `json:"module"`
	Message    string    `json:"message"`
	NodeID     *string   `json:"nodeId"`
	PipelineID *string   `json:"pipelineId"`
}

func toLogEntryView(l model.Log) logEntryView {
	var nodeID *string
	if !l.NodeID.IsZero() {
		s := l.NodeID.String()
		nodeID = &s
	}
	var pipelineID *string
	if !l.PipelineID.IsZero() {
		s := l.PipelineID.String()
		pipelineID = &s
	}
	return logEntryView{
		ID:         l.ID.String(),
		Ts:         l.Ts,
		Level:      string(l.Level),
		Module:     l.Module,
		Message:    l.Message,
		NodeID:     nodeID,
		PipelineID: pipelineID,
	}
}

// Since/Until 必须回传：调用方未显式指定时间范围时，这一层会补一个默认
// 窗口（见 resolveLogTimeRange）再查询；如果不把生效范围回传出去，用户在
// 默认窗口之外搜不到日志时会误以为"这条日志不存在"，而真实原因是它落在
// 一个界面上完全不可见的窗口之外——这是「GET /logs 的时间范围」
// 专条明确列出的硬性字段，不是可选的友好提示。
type logListView struct {
	Items            []logEntryView `json:"items"`
	Total            int            `json:"total"`
	Page             int            `json:"page"`
	PageSize         int            `json:"pageSize"`
	Since            time.Time      `json:"since"`
	Until            time.Time      `json:"until"`
	MaxWindowSeconds int64          `json:"maxWindowSeconds"`
}

type logHandler struct {
	db *gorm.DB

	logRetention    time.Duration
	runtimeSettings bool
}

const (
	defaultLogWindow = 24 * time.Hour

	// logMaxWindowCostCap bounds the observation query window independently of retention.
	logMaxWindowCostCap = 15 * 24 * time.Hour

	// maxLogFetchRows 限制单次请求允许触达的最大行数（见 list 里
	// page*pageSize 的用法）：repo.ListLogs 只提供游标分页（BeforeTs/
	// BeforeID），没有 Offset，这一层要支持前端现有的按页码翻页
	// （web/src/features/nodes/NodeLogSearchPanel.tsx 用的是 AntD Table 的页码控件，不是"加载更
	// 多"），只能退化成"取前 page*pageSize 条、再切出这一页"；深翻页会让
	// 单次查询取的行数线性增长，设一个上限防止这个退化方案被滥用成一次
	// 查几万行，超出时提示调用方缩小时间范围或加过滤条件，而不是继续放大
	// 查询。
	maxLogFetchRows = 5000
)

// 约束            依据                  超过它的后果
//
//	retention.logs  运维配置，决定数据能活多久   超出部分本来就没有数据
//	查询成本硬上限   代码常量，决定单次查询扫多少分区  拖慢管理库，影响其它请求
//
// 有效上限取两者的较小值；400 错误文案（resolveLogTimeRange 的调用方）
// 必须指出这次生效的是哪一条，运维配了 90 天却被成本上限挡在 15 天时，
// 需要知道这是成本限制、不是他的配置没生效——否则会反复去改一个不起
// 作用的配置项（与 PRECONDITION_FAILED 诊断同一个原则："错误信息要指向
// 根因，不要让人去查错方向"）。
//
// retention <= 0 视为未配置（防御性兜底，正常路径不会出现，见
// Options.LogRetention 的注释），只用成本上限单独生效。
func effectiveLogMaxWindow(retention time.Duration) (window time.Duration, reason string) {
	if retention > 0 && retention < logMaxWindowCostCap {
		return retention, "retention.logs 配置的日志保留期——超过这个跨度的日志已经不存在，不是被限制"
	}
	// 这里刻意不出现"保留期"三个字（哪怕是"不是保留期"这种否定语气）：
	// 运维排查时通常是搜关键字定位错误类别，一句话里同时出现"保留期"和
	// "不是"，搜索/扫读时很容易只捕捉到"保留期"就转去改 retention.logs，
	// 反而复现了这条注释想避免的误导——文案要让"这次生效的是哪一条约束"
	// 从字面上就没有歧义，不能依赖读者仔细看完否定词。
	return logMaxWindowCostCap, "查询成本上限（避免一次扫描过多分区拖慢管理库，跟日志能存多久无关）"
}

// registerLogRoutes 挂载 GET /logs。
func registerLogRoutes(api *gin.RouterGroup, db *gorm.DB, logRetention time.Duration, runtimeSettings bool) {
	h := &logHandler{db: db, logRetention: logRetention, runtimeSettings: runtimeSettings}
	api.GET("/logs", h.list)
}

func (h *logHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	retention := h.logRetention
	if h.runtimeSettings {
		settings, err := repo.ReadRuntimeSettings(c.Request.Context(), h.db)
		if err != nil {
			FailInternal(c, err, "read_runtime_settings_failed")
			return
		}
		retention = time.Duration(settings.LogRetentionSeconds) * time.Second
	}
	window, reason := effectiveLogMaxWindow(retention)
	since, until, err := resolveTimeRangeQuery(c.Query("since"), c.Query("until"), time.Now(), defaultLogWindow, window, reason)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_time_range", err.Error())
		return
	}

	q := repo.LogQuery{Since: since, Until: until}
	if v := c.Query("nodeId"); v != "" {
		nodeID, err := uuidx.Parse(v)
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_node_id", "nodeId 格式不合法")
			return
		}
		q.NodeID = nodeID
	}
	if v := strings.TrimSpace(c.Query("level")); v != "" {
		level := model.LogLevel(v)
		if !level.Valid() {
			Fail(c, http.StatusBadRequest, "invalid_level", "level 取值不合法")
			return
		}
		q.Level = level
	}
	if v := strings.TrimSpace(c.Query("keyword")); v != "" {
		q.Keyword = v
	}

	// repo.ListLogs 只支持游标分页，见 maxLogFetchRows 的注释：这里退化成
	// "取前 page*pageSize 条、再切出这一页"来支持前端现有的页码翻页。
	fetchRows := page * pageSize
	if fetchRows > maxLogFetchRows {
		Fail(c, http.StatusBadRequest, "page_too_deep", "翻页跨度过大，请缩小时间范围或增加过滤条件后重试")
		return
	}
	q.Limit = fetchRows

	ctx := c.Request.Context()
	rows, err := repo.ListLogs(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "list_logs_failed")
		return
	}

	start := (page - 1) * pageSize
	var pageRows []model.Log
	if start < len(rows) {
		end := min(start+pageSize, len(rows))
		pageRows = rows[start:end]
	}

	total, err := repo.CountLogs(ctx, h.db, q)
	if err != nil {
		FailInternal(c, err, "count_logs_failed")
		return
	}

	views := make([]logEntryView, 0, len(pageRows))
	for _, row := range pageRows {
		views = append(views, toLogEntryView(row))
	}

	effectiveWindow, _ := effectiveLogMaxWindow(h.logRetention)
	OK(c, logListView{
		Items:            views,
		Total:            int(total),
		Page:             page,
		PageSize:         pageSize,
		Since:            since,
		Until:            until,
		MaxWindowSeconds: int64(effectiveWindow.Seconds()),
	})
}
