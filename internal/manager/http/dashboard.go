package http

import (
	"context"
	"database/sql"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

const dashboardMetricFreshness = 5 * time.Minute

type dashboardHandler struct{ db *gorm.DB }

type dashboardLagView struct {
	PipelineID string    `json:"pipelineId"`
	Name       string    `json:"name"`
	RunID      uint64    `json:"runId"`
	LagMS      float64   `json:"lagMs"`
	ObservedAt time.Time `json:"observedAt"`
}

type dashboardErrorView struct {
	ID         string          `json:"id"`
	Ts         time.Time       `json:"ts"`
	Type       model.EventType `json:"type"`
	PipelineID *string         `json:"pipelineId"`
	NodeID     *string         `json:"nodeId"`
}

type dashboardSnapshotView struct {
	PipelineID           string               `json:"pipelineId"`
	Name                 string               `json:"name"`
	RunID                uint64               `json:"runId"`
	Status               model.SnapshotStatus `json:"status"`
	TotalChunks          uint64               `json:"totalChunks"`
	DoneChunks           uint64               `json:"doneChunks"`
	CatchupRemainingTxns *uint64              `json:"catchupRemainingTxns"`
	UpdatedAt            time.Time            `json:"updatedAt"`
}

type dashboardView struct {
	AsOf                   time.Time               `json:"asOf"`
	OnlineNodes            int64                   `json:"onlineNodes"`
	RunningPipelines       int64                   `json:"runningPipelines"`
	UnknownLagPipelines    int64                   `json:"unknownLagPipelines"`
	ErrorEvents24H         int64                   `json:"errorEvents24h"`
	FiringAlerts           int64                   `json:"firingAlerts"`
	MetricFreshnessSeconds int64                   `json:"metricFreshnessSeconds"`
	TotalSnapshots         int64                   `json:"totalSnapshots"`
	LagTop                 []dashboardLagView      `json:"lagTop"`
	RecentErrors           []dashboardErrorView    `json:"recentErrors"`
	Snapshots              []dashboardSnapshotView `json:"snapshots"`
}

func registerDashboardRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &dashboardHandler{db: db}
	api.GET("/dashboard", h.get)
}

func dashboardRunningScope(tx *gorm.DB) *gorm.DB {
	return repo.ReadResourceScope(tx.Statement.Context, tx.Model(&model.Pipeline{}).Table("pipelines AS p"), repo.ReadDashboardPipeline).
		Joins("JOIN nodes AS source ON source.id=p.source_node_id AND source.deleted_at IS NULL AND source.status=?", model.NodeStatusOnline).
		Joins("JOIN nodes AS target ON target.id=p.target_node_id AND target.deleted_at IS NULL AND target.status=?", model.NodeStatusOnline).
		Where("p.source_actual_state=? AND p.target_actual_state=?", model.ActualStateRunning, model.ActualStateRunning)
}

func dashboardLagScope(tx *gorm.DB, now time.Time) *gorm.DB {
	// JSON null/missing/string observations are unknown. Both report identity and
	// metric observation time are required; receiving an old metric again does
	// not make it fresh. SQL applies the same conditions before count and TopN.
	return dashboardRunningScope(tx).
		Joins("JOIN pipeline_progress_latest AS g ON g.pipeline_id=p.id AND g.run_id=p.run_id AND g.node_id=p.target_node_id AND g.side=?", model.SideTarget).
		Where("JSON_TYPE(JSON_EXTRACT(g.values_json, '$.metricsObservedTsMs')) IN ('INTEGER','UNSIGNED INTEGER')").
		Where("CAST(JSON_UNQUOTE(JSON_EXTRACT(g.values_json, '$.metricsObservedTsMs')) AS SIGNED) BETWEEN ? AND ?", now.Add(-dashboardMetricFreshness).UnixMilli(), now.UnixMilli()).
		Where("CAST(JSON_UNQUOTE(JSON_EXTRACT(g.values_json, '$.metricsObservedTsMs')) AS SIGNED) <= TIMESTAMPDIFF(MICROSECOND, '1970-01-01', g.observed_at)/1000").
		Where("g.observed_at <= ?", now).
		Where("JSON_TYPE(JSON_EXTRACT(g.values_json, '$.lagMs')) IN ('INTEGER','UNSIGNED INTEGER','DOUBLE')").
		Where("CAST(JSON_UNQUOTE(JSON_EXTRACT(g.values_json, '$.lagMs')) AS DECIMAL(30,6)) BETWEEN 0 AND 9007199254740991")
}

func (h *dashboardHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	now := time.Now().UTC()
	result := dashboardView{AsOf: now, MetricFreshnessSeconds: int64(dashboardMetricFreshness / time.Second), LagTop: []dashboardLagView{}, RecentErrors: []dashboardErrorView{}, Snapshots: []dashboardSnapshotView{}}
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := repo.ReadResourceScope(ctx, tx.Model(&model.Node{}), repo.ReadNode).Where("status=?", model.NodeStatusOnline).Count(&result.OnlineNodes).Error; err != nil {
			return err
		}
		if err := dashboardRunningScope(tx).Count(&result.RunningPipelines).Error; err != nil {
			return err
		}
		var known int64
		if err := dashboardLagScope(tx, now).Count(&known).Error; err != nil {
			return err
		}
		result.UnknownLagPipelines = result.RunningPipelines - known
		var progress []struct {
			PipelineID uuidx.UUID
			Name       string
			RunID      uint64
			LagMS      float64
			ObservedMS int64
		}
		if err := dashboardLagScope(tx, now).Select("p.id AS pipeline_id, p.name, p.run_id, CAST(JSON_UNQUOTE(JSON_EXTRACT(g.values_json, '$.lagMs')) AS DECIMAL(30,6)) AS lag_ms, CAST(JSON_UNQUOTE(JSON_EXTRACT(g.values_json, '$.metricsObservedTsMs')) AS SIGNED) AS observed_ms").Order("lag_ms DESC").Order("p.id ASC").Limit(10).Scan(&progress).Error; err != nil {
			return err
		}
		for _, row := range progress {
			result.LagTop = append(result.LagTop, dashboardLagView{PipelineID: row.PipelineID.String(), Name: row.Name, RunID: row.RunID, LagMS: row.LagMS, ObservedAt: time.UnixMilli(row.ObservedMS).UTC()})
		}
		errorScope := func() *gorm.DB {
			return repo.ReadResourceScope(ctx, tx.Model(&model.Event{}), repo.ReadEvent).Where("level=? AND ts>=? AND ts<=?", model.EventLevelError, now.Add(-24*time.Hour), now)
		}
		if err := errorScope().Count(&result.ErrorEvents24H).Error; err != nil {
			return err
		}
		var events []model.Event
		if err := errorScope().Select("id, ts, type, pipeline_id, node_id").Order("ts DESC, id DESC").Limit(10).Find(&events).Error; err != nil {
			return err
		}
		for _, event := range events {
			view := dashboardErrorView{ID: event.ID.String(), Ts: event.Ts, Type: event.Type}
			if !event.PipelineID.IsZero() {
				id := event.PipelineID.String()
				view.PipelineID = &id
			}
			if !event.NodeID.IsZero() {
				id := event.NodeID.String()
				view.NodeID = &id
			}
			result.RecentErrors = append(result.RecentErrors, view)
		}
		if err := repo.ReadResourceScope(ctx, tx.Model(&model.Alert{}), repo.ReadAlert).Where("status=?", model.AlertStatusFiring).Count(&result.FiringAlerts).Error; err != nil {
			return err
		}
		snapshotScope := func() *gorm.DB {
			return repo.ReadResourceScope(ctx, tx.Model(&model.PipelineSnapshot{}).Table("pipeline_snapshots AS s"), repo.ReadDashboardSnapshot).Joins("JOIN pipelines AS p ON p.id=s.pipeline_id AND p.run_id=s.run_id AND p.deleted_at IS NULL")
		}
		if err := snapshotScope().Count(&result.TotalSnapshots).Error; err != nil {
			return err
		}
		var snapshots []struct {
			model.PipelineSnapshot
			Name string
		}
		if err := snapshotScope().Select("s.id, s.pipeline_id, s.run_id, s.status, s.total_chunks, s.done_chunks, s.catchup_remaining_txns, s.updated_at, p.name").Order("s.updated_at DESC, s.id DESC").Limit(20).Scan(&snapshots).Error; err != nil {
			return err
		}
		for _, row := range snapshots {
			result.Snapshots = append(result.Snapshots, dashboardSnapshotView{PipelineID: row.PipelineID.String(), Name: row.Name, RunID: row.RunID, Status: row.Status, TotalChunks: row.TotalChunks, DoneChunks: row.DoneChunks, CatchupRemainingTxns: row.CatchupRemainingTxns, UpdatedAt: row.UpdatedAt})
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		Fail(c, http.StatusInternalServerError, "dashboard_unavailable", "读取仪表盘失败")
		return
	}
	OK(c, result)
}
