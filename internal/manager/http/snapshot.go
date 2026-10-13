package http

import (
	"database/sql"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type snapshotHandler struct{ db *gorm.DB }
type snapshotTableView struct {
	Schema        string                    `json:"schema"`
	Table         string                    `json:"table"`
	EstimatedRows int64                     `json:"estimatedRows"`
	DoneRows      int64                     `json:"doneRows"`
	Status        model.SnapshotTableStatus `json:"status"`
	TotalChunks   *uint64                   `json:"totalChunks"`
	DoneChunks    *uint64                   `json:"doneChunks"`
	RowsPerSec    *float64                  `json:"rowsPerSec"`
	ObservedAt    *time.Time                `json:"observedAt"`
}
type snapshotView struct {
	PipelineID           string                       `json:"pipelineId"`
	RunID                uint64                       `json:"runId"`
	SnapshotUUID         string                       `json:"snapshotUuid"`
	Status               model.SnapshotStatus         `json:"status"`
	TotalChunks          uint64                       `json:"totalChunks"`
	DoneChunks           uint64                       `json:"doneChunks"`
	CatchupRemainingTxns *uint64                      `json:"catchupRemainingTxns"`
	StartedAt            *time.Time                   `json:"startedAt"`
	FinishedAt           *time.Time                   `json:"finishedAt"`
	UpdatedAt            time.Time                    `json:"updatedAt"`
	Tables               paginated[snapshotTableView] `json:"tables"`
}

func registerSnapshotRoutes(api *gin.RouterGroup, db *gorm.DB) {
	h := &snapshotHandler{db: db}
	api.GET("/pipelines/:id/snapshot", RequirePipelineScope(db, false), h.get)
}
func (h *snapshotHandler) get(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	size := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)
	if page-1 > int(^uint(0)>>1)/size {
		Fail(c, http.StatusBadRequest, "invalid_page", "分页范围过大")
		return
	}
	ctx := c.Request.Context()
	var result snapshotView
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		run, err := repo.GetPipelineSnapshotRun(ctx, tx, id, p.RunID)
		if err != nil {
			return err
		}
		tables, total, err := repo.ListSnapshotTables(ctx, tx, id, p.RunID, page, size)
		if err != nil {
			return err
		}
		result = snapshotView{PipelineID: id.String(), RunID: p.RunID, SnapshotUUID: run.SnapshotUUID.String(), Status: run.Status, TotalChunks: run.TotalChunks, DoneChunks: run.DoneChunks, CatchupRemainingTxns: run.CatchupRemainingTxns, StartedAt: run.StartedAt, FinishedAt: run.FinishedAt, UpdatedAt: run.UpdatedAt, Tables: paginated[snapshotTableView]{Items: []snapshotTableView{}, Total: int(total), Page: page, PageSize: size}}
		for _, table := range tables {
			var observed *time.Time
			if table.ObservedAtMS > 0 {
				value := time.UnixMilli(table.ObservedAtMS)
				observed = &value
			}
			result.Tables.Items = append(result.Tables.Items, snapshotTableView{Schema: table.SchemaName, Table: table.Table, EstimatedRows: table.EstRows, DoneRows: table.DoneRows, Status: table.Status, TotalChunks: table.TotalChunks, DoneChunks: table.DoneChunks, RowsPerSec: table.RowsPerSec, ObservedAt: observed})
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if errors.Is(err, repo.ErrPipelineNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		return
	}
	if errors.Is(err, repo.ErrSnapshotNotFound) {
		Fail(c, http.StatusNotFound, "snapshot_not_found", "本次运行没有全量记录")
		return
	}
	if err != nil {
		FailInternal(c, err, "snapshot_query_failed")
		return
	}
	OK(c, result)
}
