package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func (h *pipelineHandler) rename(c *gin.Context, id uuidx.UUID, name string) {
	ctx := c.Request.Context()
	if err := repo.RenamePipeline(ctx, h.db, id, name); err != nil {
		if writeResourceNameError(c, err) {
			return
		}
		switch {
		case errors.Is(err, repo.ErrPipelineNotFound):
			Fail(c, http.StatusNotFound, "not_found", "线路不存在")
		case errors.Is(err, repo.ErrPipelineNameConflict):
			Fail(c, http.StatusConflict, "pipeline_name_conflict", "线路名称已存在")
		default:
			FailInternal(c, err, "rename_pipeline_failed")
		}
		return
	}
	updated, err := repo.GetPipeline(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}
	event := baseAuditEvent(c)
	event.Action, event.TargetType, event.TargetID = model.AuditActionPipelineUpdate, model.AuditTargetTypePipeline, id.String()
	h.audit.Record(ctx, event)
	OK(c, toPipelineView(ctx, h.db, *updated, nodeHeartbeats{}))
}
