package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type pipelineCleanupStepView struct {
	State string `json:"state"`
	Error string `json:"error"`
}

type pipelineCleanupView struct {
	ID            string                  `json:"id"`
	Name          string                  `json:"name"`
	State         model.CleanupState      `json:"state"`
	DeletedAt     time.Time               `json:"deletedAt"`
	Attempts      int                     `json:"attempts"`
	LastAttemptAt *time.Time              `json:"lastAttemptAt"`
	LastError     string                  `json:"lastError"`
	Source        pipelineCleanupStepView `json:"source"`
	Target        pipelineCleanupStepView `json:"target"`
	Queue         pipelineCleanupStepView `json:"queue"`
	ServerID      pipelineCleanupStepView `json:"serverId"`
}

type pipelineCleanupHandler struct {
	db    *gorm.DB
	audit IAuditRecorder
	wake  func()
}

func registerPipelineCleanupRoutes(api *gin.RouterGroup, db *gorm.DB, audit IAuditRecorder, wake func()) {
	h := &pipelineCleanupHandler{db: db, audit: audit, wake: wake}
	api.GET("/pipelines/cleanup", h.list)
	api.POST("/pipelines/:id/cleanup/retry", RequireRole(auth.RoleAdmin), h.retry)
}

func toPipelineCleanupView(p model.Pipeline) pipelineCleanupView {
	roleStep := func(confirmed *time.Time, message string) pipelineCleanupStepView {
		if confirmed != nil {
			return pipelineCleanupStepView{State: "done"}
		}
		if message != "" {
			return pipelineCleanupStepView{State: "failed", Error: redact.PotentialCredentials(message)}
		}
		return pipelineCleanupStepView{State: "pending"}
	}
	var detail model.PipelineCleanupDetail
	_ = json.Unmarshal(p.CleanupDetail, &detail)
	resourceStep := func(r model.CleanupResourceDetail) pipelineCleanupStepView {
		state := string(r.State)
		if state == "" {
			state = "pending"
		}
		return pipelineCleanupStepView{State: state, Error: redact.PotentialCredentials(r.LastError)}
	}
	return pipelineCleanupView{ID: p.ID.String(), Name: p.Name, State: p.CleanupState, DeletedAt: p.DeletedAt.Time, Attempts: p.CleanupAttempts, LastAttemptAt: p.CleanupLastAttemptAt, LastError: redact.PotentialCredentials(p.CleanupLastError), Source: roleStep(p.SourceCleanupConfirmedAt, p.SourceCleanupError), Target: roleStep(p.TargetCleanupConfirmedAt, p.TargetCleanupError), Queue: resourceStep(detail.Queue), ServerID: resourceStep(detail.ServerID)}
}

func (h *pipelineCleanupHandler) list(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	size := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)
	q := h.db.WithContext(c.Request.Context()).Unscoped().Model(&model.Pipeline{}).Where("deleted_at IS NOT NULL AND cleanup_state <> ?", model.CleanupStateDone)
	var total int64
	if err := q.Count(&total).Error; err != nil {
		FailInternal(c, err, "list_pipeline_cleanup_failed")
		return
	}
	var ps []model.Pipeline
	if err := q.Order("deleted_at ASC, id ASC").Offset((page - 1) * size).Limit(size).Find(&ps).Error; err != nil {
		FailInternal(c, err, "list_pipeline_cleanup_failed")
		return
	}
	items := make([]pipelineCleanupView, 0, len(ps))
	for _, p := range ps {
		items = append(items, toPipelineCleanupView(p))
	}
	OK(c, paginated[pipelineCleanupView]{Items: items, Total: int(total), Page: page, PageSize: size})
}

func (h *pipelineCleanupHandler) retry(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	if err := repo.RequestPipelineCleanupRetry(c.Request.Context(), h.db, id); err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		if errors.Is(err, repo.ErrPipelineConfigVersionConflict) {
			Fail(c, http.StatusConflict, "cleanup_conflict", "线路尚未删除")
			return
		}
		FailInternal(c, err, "retry_pipeline_cleanup_failed")
		return
	}
	if h.wake != nil {
		h.wake()
	}
	event := baseAuditEvent(c)
	event.Action, event.TargetType, event.TargetID = model.AuditActionPipelineDelete, model.AuditTargetTypePipeline, id.String()
	h.audit.Record(context.WithoutCancel(c.Request.Context()), event)
	OK(c, gin.H{"scheduled": true})
}
