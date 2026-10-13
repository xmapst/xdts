package http

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

type pipelineRulesView struct {
	ConfigVersion     uint64                   `json:"configVersion"`
	Version           uint32                   `json:"version"`
	Rules             json.RawMessage          `json:"rules"`
	Editable          bool                     `json:"editable"`
	EditBlockedReason string                   `json:"editBlockedReason,omitempty"`
	Application       pipelineRulesApplication `json:"application"`
}

type pipelineRulesApplication struct {
	Source pipelineRulesApplyStatus `json:"source"`
	Target pipelineRulesApplyStatus `json:"target"`
}

type pipelineRulesApplyStatus struct {
	Status  model.ReleaseStatus `json:"status"`
	Message string              `json:"message,omitempty"`
}

func pipelineRulesEditReason(p *model.Pipeline) string {
	if p.ReinitPending {
		return "pipeline_reinitializing"
	}
	return ""
}

func validateDynamicPipelineRules(p *model.Pipeline, before, after model.JSON) error {
	old, err := pipelinespec.ParseStrategyRules(before)
	if err != nil {
		return err
	}
	next, err := pipelinespec.ParseStrategyRules(after)
	if err != nil {
		return err
	}
	if old.Source == nil {
		old.Source = &pipelinespec.StrategySourceRules{}
	}
	if next.Source == nil {
		next.Source = &pipelinespec.StrategySourceRules{}
	}
	if old.Target == nil {
		old.Target = &pipelinespec.StrategyTargetRules{}
	}
	if next.Target == nil {
		next.Target = &pipelinespec.StrategyTargetRules{}
	}
	if !p.SchemaUUID.IsZero() {
		selection := func(doc *pipelinespec.StrategyRulesDoc) []byte {
			raw, _ := json.Marshal(struct {
				Objects []*iproto.ObjectFilterRule `json:"objects,omitempty"`
				Shadows []*iproto.ShadowTableRule  `json:"shadows,omitempty"`
				Columns []*iproto.ColumnFilterRule `json:"columns,omitempty"`
			}{doc.Source.ObjectFilters, doc.Source.ShadowTables, doc.Source.ColumnFilters})
			return raw
		}
		if string(selection(old)) != string(selection(next)) || !proto.Equal(old.Target.Mappings, next.Target.Mappings) {
			return &pipelineValidationError{"pipeline_migration_confirmed", "对象选择、列过滤、影子表或映射变更需要新的结构迁移计划，请克隆线路后配置新规则"}
		}
	}
	if p.LastCmdSeq > 0 && (old.Source.Snapshot.GetReadFromReplica() != next.Source.Snapshot.GetReadFromReplica() || old.Source.Snapshot.GetReplicaEndpointId() != next.Source.Snapshot.GetReplicaEndpointId()) {
		return &pipelineValidationError{"pipeline_snapshot_source_changed", "更换快照读取端点需要克隆线路并执行预检查"}
	}
	return nil
}

func (h *pipelineHandler) pipelineRulesApplication(ctx context.Context, tx *gorm.DB, p *model.Pipeline) (pipelineRulesApplication, error) {
	result := pipelineRulesApplication{Source: pipelineRulesApplyStatus{Status: model.ReleaseStatusPending}, Target: pipelineRulesApplyStatus{Status: model.ReleaseStatusPending}}
	for _, role := range []struct {
		side   model.Side
		node   uuidx.UUID
		status *pipelineRulesApplyStatus
	}{
		{model.SideSource, p.SourceNodeID, &result.Source}, {model.SideTarget, p.TargetNodeID, &result.Target},
	} {
		var release model.PipelineRelease
		err := tx.WithContext(ctx).Where("pipeline_id=? AND config_version=? AND side=? AND node_id=?", p.ID, p.ConfigVersion, role.side, role.node).Order("created_at DESC, id DESC").First(&release).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			continue
		}
		if err != nil {
			return result, err
		}
		role.status.Status, role.status.Message = release.Status, release.Message
	}
	return result, nil
}

type pipelineRulesUpdateRequest struct {
	ConfigVersion *uint64         `json:"configVersion" binding:"required"`
	Rules         json.RawMessage `json:"rules" binding:"required"`
}

func (h *pipelineHandler) pipelineRules(ctx context.Context, id uuidx.UUID) (*pipelineRulesView, error) {
	var view *pipelineRulesView
	err := h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		v, err := repo.GetStrategyVersion(ctx, tx, p.StrategyID, p.StrategyVersion)
		if err != nil {
			return err
		}
		reason := pipelineRulesEditReason(p)
		application, err := h.pipelineRulesApplication(ctx, tx, p)
		if err != nil {
			return err
		}
		view = &pipelineRulesView{ConfigVersion: p.ConfigVersion, Version: v.Version, Rules: json.RawMessage(v.Rules), Editable: reason == "", EditBlockedReason: reason, Application: application}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return view, err
}

func (h *pipelineHandler) getRules(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	view, err := h.pipelineRules(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
		} else {
			FailInternal(c, err, "get_pipeline_rules_failed")
		}
		return
	}
	OK(c, view)
}

func (h *pipelineHandler) saveRules(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var input pipelineRulesUpdateRequest
	if c.ShouldBindJSON(&input) != nil || input.ConfigVersion == nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	raw, err := canonicalPipelineRules(input.Rules)
	if err != nil {
		writePipelineValidationError(c, err)
		return
	}
	userID, err := uuidx.Parse(UserIDFromContext(c))
	if err != nil || userID.IsZero() {
		Fail(c, http.StatusForbidden, "forbidden", "没有权限执行此操作")
		return
	}
	ctx := c.Request.Context()
	err = h.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		p, err := repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if err := repo.LockTopology(ctx, tx, p.TopologyID); err != nil {
			return err
		}
		p, err = repo.GetPipeline(ctx, tx, id)
		if err != nil {
			return err
		}
		if p.ConfigVersion != *input.ConfigVersion {
			return repo.ErrPipelineConfigVersionConflict
		}
		if pipelineRulesEditReason(p) != "" {
			return &pipelineValidationError{"pipeline_reinitializing", "线路正在重新初始化，请等待完成后再修改规则"}
		}
		var previousPolicy string
		if p.DesiredState == model.DesiredStateRunning {
			analysis, err := repo.GetTopologyAnalysis(ctx, tx, uuidx.MustParse(repo.AutomaticTopologyID))
			if err != nil {
				return err
			}
			previousPolicy = analysis.PolicyDigest
		}
		previous, err := repo.GetStrategyVersion(ctx, tx, p.StrategyID, p.StrategyVersion)
		if err != nil {
			return err
		}
		if err := validateDynamicPipelineRules(p, previous.Rules, model.JSON(raw)); err != nil {
			return err
		}
		version, err := repo.CreatePipelineRulesVersion(ctx, tx, p.ID, model.JSON(raw), userID)
		if err != nil {
			return err
		}
		candidate := *p
		candidate.StrategyID, candidate.StrategyVersion = version.StrategyID, version.Version
		doc, err := pipelinespec.ParseStrategyRules(raw)
		if err != nil {
			return err
		}
		if err := pipelinespec.RequireETLCapabilities(ctx, tx, &candidate, doc); err != nil {
			return &pipelineValidationError{"pipeline_etl_node_unsupported", err.Error()}
		}
		if err := h.validateScopeOverlap(ctx, tx, &candidate); err != nil {
			return err
		}
		if err := repo.UpdatePipelineRules(ctx, tx, id, *input.ConfigVersion, version.StrategyID, version.Version); err != nil {
			return err
		}
		if p.DesiredState == model.DesiredStateRunning {
			analysis, err := repo.GetTopologyAnalysis(ctx, tx, uuidx.MustParse(repo.AutomaticTopologyID))
			if err != nil {
				return err
			}
			if analysis.PolicyDigest == previousPolicy {
				return nil
			}
			if err := repo.CheckTopologyStart(ctx, tx, p.TopologyID); err != nil {
				if errors.Is(err, repo.ErrTopologyConfirmationRequired) {
					return &pipelineValidationError{"topology_confirmation_required", "新规则产生未确认的回环或重复到达风险，请停止线路后修改并确认"}
				}
				return err
			}
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		if writePipelineOverlapError(c, err) {
			return
		}
		if errors.Is(err, repo.ErrPipelineConfigVersionConflict) {
			Fail(c, http.StatusConflict, "pipeline_config_version_conflict", "配置已变化，请刷新后重试")
			return
		}
		if _, ok := errors.AsType[*pipelineValidationError](err); ok {
			writePipelineValidationError(c, err)
			return
		}
		FailInternal(c, err, "save_pipeline_rules_failed")
		return
	}
	event := baseAuditEvent(c)
	event.Action, event.TargetType, event.TargetID = model.AuditActionPipelineUpdate, model.AuditTargetTypePipeline, id.String()
	h.audit.Record(ctx, event)
	if h.dispatcher != nil {
		syncPipelineBestEffort(ctx, h.dispatcher, id)
	}
	h.getRules(c)
}

// Reapplying advances the configuration delivery version without creating a rule version.
func (h *pipelineHandler) applyRules(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}
	var input struct {
		ConfigVersion *uint64 `json:"configVersion" binding:"required"`
	}
	if c.ShouldBindJSON(&input) != nil || input.ConfigVersion == nil {
		Fail(c, http.StatusBadRequest, "invalid_request", "请求参数不合法")
		return
	}
	ctx := c.Request.Context()
	_, err = repo.BumpPipelineConfigVersion(ctx, h.db, id, *input.ConfigVersion)
	if err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		if errors.Is(err, repo.ErrPipelineConfigVersionConflict) {
			Fail(c, http.StatusConflict, "pipeline_config_version_conflict", "配置已变化或线路正在重新初始化，请刷新后重试")
			return
		}
		FailInternal(c, err, "apply_pipeline_rules_failed")
		return
	}
	event := baseAuditEvent(c)
	event.Action, event.TargetType, event.TargetID = model.AuditActionPipelineUpdate, model.AuditTargetTypePipeline, id.String()
	h.audit.Record(ctx, event)
	if h.dispatcher != nil {
		syncPipelineBestEffort(ctx, h.dispatcher, id)
	}
	h.getRules(c)
}
