package http

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/datacheck"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

type iDataCheckRunner interface {
	SubmitDataCheck(context.Context, uuidx.UUID, uuidx.UUID, datacheck.Config, uint64) (*model.DataCheckRun, error)
}
type dataCheckHandler struct {
	db     *gorm.DB
	runner iDataCheckRunner
	audit  IAuditRecorder
}

func registerDataCheckRoutes(api *gin.RouterGroup, db *gorm.DB, prober IEndpointProber, audit IAuditRecorder) {
	h := &dataCheckHandler{db: db, audit: audit}
	h.runner, _ = prober.(iDataCheckRunner)
	api.GET("/pipelines/:id/data-checks", RequirePipelineScope(db, false), h.list)
	api.POST("/pipelines/:id/data-checks", RequirePipelineScope(db, true), h.create)
	api.GET("/pipelines/:id/data-checks/:checkId", RequirePipelineScope(db, false), h.get)
	api.POST("/pipelines/:id/data-checks/:checkId/cancel", RequirePipelineScope(db, true), h.cancel)
}

func (h *dataCheckHandler) ids(c *gin.Context) (uuidx.UUID, uuidx.UUID, bool) {
	if !requireDB(c, h.db) {
		return uuidx.Nil, uuidx.Nil, false
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "线路 id 不合法")
		return uuidx.Nil, uuidx.Nil, false
	}
	var check uuidx.UUID
	if c.Param("checkId") != "" {
		check, err = uuidx.Parse(c.Param("checkId"))
		if err != nil {
			Fail(c, http.StatusBadRequest, "invalid_id", "校验 id 不合法")
			return uuidx.Nil, uuidx.Nil, false
		}
	}
	if _, err := repo.GetPipeline(c.Request.Context(), h.db, id); err != nil {
		h.fail(c, err)
		return uuidx.Nil, uuidx.Nil, false
	}
	return id, check, true
}

func (h *dataCheckHandler) fail(c *gin.Context, err error) {
	switch {
	case errors.Is(err, repo.ErrDataCheckNotFound), errors.Is(err, repo.ErrPipelineNotFound):
		Fail(c, http.StatusNotFound, "not_found", "线路或校验任务不存在")
	case errors.Is(err, repo.ErrDataCheckBusy):
		Fail(c, http.StatusConflict, "data_check_busy", "线路已有未完成校验任务")
	case errors.Is(err, datacheck.ErrConfig):
		Fail(c, http.StatusUnprocessableEntity, "data_check_invalid", "请检查校验范围、列映射及抽样配置；校验范围须符合当前线路规则")
	case errors.Is(err, task.ErrProbeUnavailable):
		Fail(c, http.StatusServiceUnavailable, "data_check_unavailable", "校验服务尚未就绪")
	case errors.Is(err, task.ErrProbeNodeOffline), errors.Is(err, task.ErrDataCheckNodeUnsupported), errors.Is(err, task.ErrDataCheckConnectionUnsupported):
		Fail(c, http.StatusConflict, "data_check_failed", err.Error())
	default:
		FailInternal(c, err, "data_check_failed")
	}
}

func (h *dataCheckHandler) list(c *gin.Context) {
	id, _, ok := h.ids(c)
	if !ok {
		return
	}
	runs, err := repo.ListDataChecks(c.Request.Context(), h.db, id)
	if err != nil {
		FailInternal(c, err, "data_check_list_failed")
		return
	}
	OK(c, runs)
}
func (h *dataCheckHandler) get(c *gin.Context) {
	id, check, ok := h.ids(c)
	if !ok {
		return
	}
	run, err := repo.GetDataCheck(c.Request.Context(), h.db, id, check)
	if err != nil {
		h.fail(c, err)
		return
	}
	OK(c, run)
}
func (h *dataCheckHandler) create(c *gin.Context) {
	id, _, ok := h.ids(c)
	if !ok {
		return
	}
	if h.runner == nil {
		h.fail(c, task.ErrProbeUnavailable)
		return
	}
	var input struct {
		Config       datacheck.Config `json:"config"`
		SampleModulo uint64           `json:"sampleModulo"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(c.Writer, c.Request.Body, 1<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&input) != nil || decoder.Decode(new(any)) != io.EOF {
		Fail(c, http.StatusBadRequest, "data_check_invalid", "校验参数不合法")
		return
	}
	user, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || user.IsZero() {
		Fail(c, http.StatusForbidden, "forbidden", "用户身份无效")
		return
	}
	run, err := h.runner.SubmitDataCheck(c.Request.Context(), id, user, input.Config, input.SampleModulo)
	if err != nil {
		h.fail(c, err)
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineUpdate
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	event.Changes = []byte(`{"operation":"data_check_create"}`)
	if h.audit != nil {
		h.audit.Record(c.Request.Context(), event)
	}
	OK(c, run)
}
func (h *dataCheckHandler) cancel(c *gin.Context) {
	id, check, ok := h.ids(c)
	if !ok {
		return
	}
	if err := repo.CancelDataCheck(c.Request.Context(), h.db, id, check); err != nil {
		h.fail(c, err)
		return
	}
	event := baseAuditEvent(c)
	event.Action = model.AuditActionPipelineUpdate
	event.TargetType = model.AuditTargetTypePipeline
	event.TargetID = id.String()
	event.Changes = []byte(`{"operation":"data_check_cancel"}`)
	if h.audit != nil {
		h.audit.Record(c.Request.Context(), event)
	}
	h.get(c)
}
