package http

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type pipelineCloneRequest struct {
	Name             string          `json:"name" binding:"required"`
	TopologyID       string          `json:"topologyId"`
	SourceEndpointID string          `json:"sourceEndpointId"`
	SourceNodeID     string          `json:"sourceNodeId"`
	TargetEndpointID string          `json:"targetEndpointId"`
	TargetNodeID     string          `json:"targetNodeId"`
	StrategyID       string          `json:"strategyId"`
	StrategyVersion  *uint32         `json:"strategyVersion"`
	InitMode         string          `json:"initMode"`
	StartPosition    json.RawMessage `json:"startPosition"`
}

func (h *pipelineHandler) clone(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var req pipelineCloneRequest
	if err := c.ShouldBindJSON(&req); err != nil || strings.TrimSpace(req.Name) == "" {
		Fail(c, http.StatusBadRequest, "invalid_request", "请填写克隆线路的新名称")
		return
	}
	p, err := repo.GetPipeline(c.Request.Context(), h.db, id)
	if errors.Is(err, repo.ErrPipelineNotFound) {
		Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		return
	}
	if err != nil {
		FailInternal(c, err, "get_pipeline_failed")
		return
	}
	input := pipelineCreateRequest{Name: strings.TrimSpace(req.Name), TopologyID: repo.AutomaticTopologyID, SourceEndpointID: p.SourceEndpointID.String(), SourceNodeID: p.SourceNodeID.String(), TargetEndpointID: p.TargetEndpointID.String(), TargetNodeID: p.TargetNodeID.String(), StrategyID: p.StrategyID.String(), StrategyVersion: p.StrategyVersion, InitMode: string(p.InitMode), StartPosition: append(json.RawMessage(nil), p.StartPosition...)}
	for _, field := range []struct {
		override string
		target   *string
	}{
		{req.TopologyID, &input.TopologyID}, {req.SourceEndpointID, &input.SourceEndpointID}, {req.SourceNodeID, &input.SourceNodeID},
		{req.TargetEndpointID, &input.TargetEndpointID}, {req.TargetNodeID, &input.TargetNodeID}, {req.StrategyID, &input.StrategyID}, {req.InitMode, &input.InitMode},
	} {
		if field.override != "" {
			*field.target = field.override
		}
	}
	if req.StrategyVersion != nil {
		if *req.StrategyVersion == 0 {
			Fail(c, http.StatusBadRequest, "invalid_strategy_version", "strategyVersion 须大于0")
			return
		}
		input.StrategyVersion = *req.StrategyVersion
	}
	if req.StartPosition != nil {
		input.StartPosition = req.StartPosition
	} else if req.InitMode != "" && req.InitMode != string(p.InitMode) {
		input.StartPosition = nil
	}
	if req.StrategyID == "" && req.StrategyVersion == nil {
		version, err := repo.GetStrategyVersion(c.Request.Context(), h.db, p.StrategyID, p.StrategyVersion)
		if err != nil {
			FailInternal(c, err, "get_pipeline_rules_failed")
			return
		}
		input.Rules, input.StrategyID, input.StrategyVersion = json.RawMessage(version.Rules), "", 0
	}
	h.createFromRequest(c, input, id.String())
}

type pipelineBatchRequest struct {
	Action string   `json:"action" binding:"required"`
	IDs    []string `json:"ids" binding:"required"`
}

type pipelineBatchResult struct {
	ID      string `json:"id"`
	Success bool   `json:"success"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *pipelineHandler) batch(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	var req pipelineBatchRequest
	if err := c.ShouldBindJSON(&req); err != nil || len(req.IDs) == 0 || len(req.IDs) > 100 {
		Fail(c, http.StatusBadRequest, "invalid_request", "请选择1至100条不同线路")
		return
	}
	var state model.DesiredState
	var action model.AuditAction
	switch req.Action {
	case "start":
		state, action = model.DesiredStateRunning, model.AuditActionPipelineStart
	case "resume":
		state, action = model.DesiredStateRunning, model.AuditActionPipelineResume
	case "pause":
		state, action = model.DesiredStatePaused, model.AuditActionPipelinePause
	case "stop":
		state, action = model.DesiredStateStopped, model.AuditActionPipelineStop
	default:
		Fail(c, http.StatusBadRequest, "invalid_action", "批量操作仅支持启动、暂停、恢复和停止")
		return
	}
	ids := make([]uuidx.UUID, len(req.IDs))
	seen := make(map[uuidx.UUID]bool, len(ids))
	for i, text := range req.IDs {
		id, err := uuidx.Parse(text)
		if err != nil || id == (uuidx.UUID{}) || seen[id] {
			Fail(c, http.StatusBadRequest, "invalid_ids", "线路ID须为不同的有效UUID")
			return
		}
		ids[i], seen[id] = id, true
	}
	results := make([]pipelineBatchResult, 0, len(ids))
	for _, id := range ids {
		result := pipelineBatchResult{ID: id.String(), Success: true, Code: "ok", Message: "操作已接受"}
		if err := h.writePipelineIntent(c.Request.Context(), id, state, false); err != nil {
			result.Success = false
			result.Code, result.Message = pipelineBatchError(err)
			if result.Code == "pipeline_intent_failed" {
				slog.Error("pipeline batch intent failed", slog.String("pipeline_id", id.String()), slog.Any("error", err))
			}
		} else {
			event := baseAuditEvent(c)
			event.Action, event.TargetType, event.TargetID = action, model.AuditTargetTypePipeline, id.String()
			h.audit.Record(c.Request.Context(), event)
		}
		results = append(results, result)
	}
	OK(c, gin.H{"results": results})
}

func pipelineBatchError(err error) (string, string) {
	switch {
	case errors.Is(err, model.ErrPipelineTransition):
		return "invalid_pipeline_transition", "当前状态不允许此操作，未启动线路请先完成预检查并启动"
	case errors.Is(err, repo.ErrPrecheckNotFound):
		return "precheck_required", "请先完成当前配置的预检查"
	case errors.Is(err, repo.ErrTimestampConfirmationRequired):
		return "timestamp_confirmation_required", "请先预览并确认时间点对应的实际事务边界"
	case errors.Is(err, topologycheck.ErrBidirectionalInitialization):
		return "bidirectional_initialization_conflict", "双向初始化只能由一个方向执行全量或结构迁移"
	case errors.Is(err, repo.ErrPipelineNotFound), errors.Is(err, repo.ErrTopologyNotFound):
		return "not_found", "pipeline 或拓扑不存在"
	case errors.Is(err, repo.ErrTopologyConfirmationRequired):
		return "topology_confirmation_required", "回环或重复到达告警需管理员确认后才能启动"
	}
	if code := precheckErrorCode(err); code != "precheck_failed" && code != "" {
		return code, "当前预检查结果不允许启动，请刷新并处理检查项"
	}
	return "pipeline_intent_failed", "服务器内部错误"
}
