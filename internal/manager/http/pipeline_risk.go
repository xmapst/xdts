package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type pipelineRiskView struct {
	Digest     string                  `json:"digest"`
	Confirmed  bool                    `json:"confirmed"`
	Warnings   []topologycheck.Warning `json:"warnings"`
	Incomplete bool                    `json:"incomplete"`
}

func (h *pipelineHandler) riskView(c *gin.Context) (*pipelineRiskView, error) {
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		return nil, err
	}
	if _, err := repo.GetPipeline(c.Request.Context(), h.db, id); err != nil {
		return nil, err
	}
	analysis, err := repo.GetTopologyAnalysis(c.Request.Context(), h.db, uuidx.MustParse(repo.AutomaticTopologyID))
	if err != nil {
		return nil, err
	}
	confirmed, err := repo.TopologyConfirmed(c.Request.Context(), h.db, analysis)
	if err != nil {
		return nil, err
	}
	return &pipelineRiskView{Digest: analysis.Digest, Confirmed: confirmed, Warnings: analysis.Warnings, Incomplete: analysis.Incomplete}, nil
}

func (h *pipelineHandler) getRisk(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	view, err := h.riskView(c)
	if err != nil {
		h.writeRiskError(c, err)
		return
	}
	OK(c, view)
}

func (h *pipelineHandler) confirmRisk(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var input struct {
		Digest string `json:"digest" binding:"required"`
	}
	if c.ShouldBindJSON(&input) != nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	view, err := h.riskView(c)
	if err != nil {
		h.writeRiskError(c, err)
		return
	}
	userID, err := uuidx.Parse(UserIDFromContext(c))
	role, ok := RoleFromContext(c)
	if err != nil || userID.IsZero() || !ok || role != auth.RoleAdmin {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限执行此操作")
		return
	}
	if view.Incomplete {
		Fail(c, http.StatusConflict, "pipeline_risk_changed", "线路风险分析未完成，请刷新后重新确认")
		return
	}
	if err := repo.ConfirmTopology(c.Request.Context(), h.db, uuidx.MustParse(repo.AutomaticTopologyID), input.Digest, userID, UsernameFromContext(c), time.Now()); err != nil {
		if errors.Is(err, repo.ErrTopologyAnalysisChanged) {
			Fail(c, http.StatusConflict, "pipeline_risk_changed", "线路风险分析已变化，请刷新后重新确认")
			return
		}
		FailInternal(c, err, "pipeline_risk_confirmation_failed")
		return
	}
	h.getRisk(c)
}

func (h *pipelineHandler) writeRiskError(c *gin.Context, err error) {
	if errors.Is(err, repo.ErrPipelineNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		return
	}
	if _, parseErr := uuidx.Parse(c.Param("id")); parseErr != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	FailInternal(c, err, "pipeline_risk_analysis_failed")
}
