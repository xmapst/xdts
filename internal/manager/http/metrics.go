package http

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type metricsHandler struct{ db *gorm.DB }

type progressObservationView struct {
	NodeID     string               `json:"nodeId"`
	Role       model.Side           `json:"role"`
	RunID      uint64               `json:"runId"`
	ObservedAt time.Time            `json:"observedAt"`
	Values     model.ProgressValues `json:"values"`
}

type metricHistoryView struct {
	ObservedAt  time.Time             `json:"observedAt"`
	Role        *model.Side           `json:"role"`
	RunID       *uint64               `json:"runId"`
	NodeID      *string               `json:"nodeId"`
	DelayMS     int64                 `json:"delayMs"`
	RowsPerSec  float64               `json:"rowsPerSec"`
	TxnsPerSec  *float64              `json:"txnsPerSec"`
	BytesPerSec float64               `json:"bytesPerSec"`
	QueueDepth  int64                 `json:"queueDepth"`
	Errors      int64                 `json:"errors"`
	Conflicts   *int64                `json:"conflicts"`
	Values      *model.ProgressValues `json:"values"`
}

type pipelineMetricsView struct {
	PipelineID            string                       `json:"pipelineId"`
	RunID                 uint64                       `json:"runId"`
	From                  time.Time                    `json:"from"`
	To                    time.Time                    `json:"to"`
	SampleIntervalSeconds int                          `json:"sampleIntervalSeconds"`
	Latest                []progressObservationView    `json:"latest"`
	History               paginated[metricHistoryView] `json:"history"`
}

func registerMetricsRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &metricsHandler{db: db}
	api.GET("/metrics/pipelines/:id", RequirePipelineScope(db, false), h.get)
}

func metricsTime(raw string, fallback time.Time) (time.Time, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || value.Year() < 1970 || value.Year() > 9999 {
		return time.Time{}, errors.New("invalid metrics time")
	}
	return value.UTC(), nil
}

func metricsPage(raw string, fallback int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return 0, errors.New("invalid metrics page")
	}
	return n, nil
}

func (h *metricsHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	to, err := metricsTime(c.Query("to"), time.Now().UTC())
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_range", "时间范围不合法")
		return
	}
	from, err := metricsTime(c.Query("from"), to.Add(-time.Hour))
	if err != nil || !to.After(from) || to.Sub(from) > 31*24*time.Hour {
		Fail(c, http.StatusBadRequest, "invalid_range", "时间范围必须在 31 天以内")
		return
	}
	page, err := metricsPage(c.Query("page"), 1)
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_page", "页码不合法")
		return
	}
	size, err := metricsPage(c.Query("pageSize"), defaultListPageSize)
	if err != nil || size > maxListPageSize || page-1 > int(^uint(0)>>1)/size {
		Fail(c, http.StatusBadRequest, "invalid_page", "分页范围不合法")
		return
	}
	activeOnly := false
	if c.Request.URL.Query().Has("activeOnly") {
		raw := c.Query("activeOnly")
		if raw != "true" && raw != "false" {
			Fail(c, http.StatusBadRequest, "invalid_filter", "activeOnly 必须为 true 或 false")
			return
		}
		activeOnly = raw == "true"
	}
	ctx := c.Request.Context()
	var result pipelineMetricsView
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		latest, err := repo.ListPipelineProgress(ctx, tx, id, p.RunID)
		if err != nil {
			return err
		}
		rows, total, err := repo.ListPipelineMetricHistory(ctx, tx, id, p.RunID, from, to, page, size, activeOnly)
		if err != nil {
			return err
		}
		result = pipelineMetricsView{PipelineID: id.String(), RunID: p.RunID, From: from, To: to, SampleIntervalSeconds: int(repo.ProgressSampleInterval / time.Second), Latest: []progressObservationView{}, History: paginated[metricHistoryView]{Items: []metricHistoryView{}, Total: int(total), Page: page, PageSize: size}}
		for _, row := range latest {
			var values model.ProgressValues
			if err := json.Unmarshal(row.Values, &values); err != nil {
				return err
			}
			result.Latest = append(result.Latest, progressObservationView{NodeID: row.NodeID.String(), Role: row.Side, RunID: row.RunID, ObservedAt: row.ObservedAt, Values: values})
		}
		for _, row := range rows {
			var node *string
			if row.NodeID != nil {
				value := row.NodeID.String()
				node = &value
			}
			var values *model.ProgressValues
			if len(row.Values) != 0 {
				values = &model.ProgressValues{}
				if err := json.Unmarshal(row.Values, values); err != nil {
					return err
				}
			}
			result.History.Items = append(result.History.Items, metricHistoryView{ObservedAt: row.Ts, Role: row.Side, RunID: row.RunID, NodeID: node, DelayMS: row.DelayMs, RowsPerSec: row.RPS, TxnsPerSec: row.TPS, BytesPerSec: row.BPS, QueueDepth: row.QueueDepth, Errors: row.Errors, Conflicts: row.Conflicts, Values: values})
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if errors.Is(err, repo.ErrPipelineNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		return
	}
	if err != nil {
		FailInternal(c, err, "metrics_query_failed")
		return
	}
	OK(c, result)
}
