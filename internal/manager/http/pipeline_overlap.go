package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/rule"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type pipelineOverlapError struct{ code, message string }

func (e *pipelineOverlapError) Error() string { return e.message }

func writePipelineOverlapError(c *gin.Context, err error) bool {
	if overlap, ok := errors.AsType[*pipelineOverlapError](err); ok {
		Fail(c, http.StatusConflict, overlap.code, overlap.message)
		return true
	}
	return false
}

// validateScopeOverlap runs under the save transaction. A source endpoint row
// serializes even different topologies sharing the same endpoint pair. Current
// locking reads avoid stale snapshots after waiting for another save to commit.
func (h *pipelineHandler) validateScopeOverlap(ctx context.Context, tx *gorm.DB, candidate *model.Pipeline) error {
	var endpoint model.Endpoint
	if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).First(&endpoint, "id=?", candidate.SourceEndpointID).Error; err != nil {
		return err
	}
	var existing []model.Pipeline
	query := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).Where("source_endpoint_id=? AND target_endpoint_id=?", candidate.SourceEndpointID, candidate.TargetEndpointID)
	if !candidate.ID.IsZero() {
		query = query.Where("id<>?", candidate.ID)
	}
	if err := query.Order("id").Find(&existing).Error; err != nil {
		return err
	}
	if len(existing) == 0 {
		return nil
	}
	a, err := h.sourceScopePlan(ctx, tx, candidate, endpoint.LowerCaseTableNames == "SENSITIVE")
	if err != nil {
		return err
	}
	for _, other := range existing {
		b, err := h.sourceScopePlan(ctx, tx, &other, endpoint.LowerCaseTableNames == "SENSITIVE")
		if err != nil {
			return err
		}
		witness, overlap, err := rule.FindTableOverlap(a, b)
		if err != nil {
			return &pipelineOverlapError{"pipeline_scope_unverifiable", "同步范围过于复杂，无法确认与管道“" + other.Name + "”是否重叠，请简化库表规则"}
		}
		if overlap {
			return &pipelineOverlapError{"pipeline_scope_overlap", fmt.Sprintf("同步范围与管道“%s”重叠：库 %q、表 %q，请调整过滤规则", other.Name, witness.Schema, witness.Table)}
		}
	}
	return nil
}

func (h *pipelineHandler) sourceScopePlan(ctx context.Context, tx *gorm.DB, pipeline *model.Pipeline, sensitive bool) (*rule.Plan, error) {
	version, err := repo.GetStrategyVersion(ctx, tx, pipeline.StrategyID, pipeline.StrategyVersion)
	if err != nil {
		return nil, err
	}
	document, err := pipelinespec.ParseStrategyRules(version.Rules)
	if err != nil {
		return nil, &pipelineOverlapError{"pipeline_scope_unverifiable", "所选策略无法解析，不能验证同步范围"}
	}
	// Nonempty mappings are rejected by current strategy saving. If an old
	// record contains them, never assume identity target names for validation.
	if document.Target != nil && document.Target.Mappings != nil {
		mappings := document.Target.Mappings
		if len(mappings.Schemas)+len(mappings.Tables)+len(mappings.Columns) > 0 {
			return nil, &pipelineOverlapError{"pipeline_scope_unverifiable", "所选策略包含目标映射，不能验证同步范围"}
		}
	}
	var filters []rule.ObjectFilterRule
	if document.Source != nil {
		for _, f := range document.Source.ObjectFilters {
			if f == nil {
				return nil, &pipelineOverlapError{"pipeline_scope_unverifiable", "所选策略含空过滤规则"}
			}
			filters = append(filters, rule.ObjectFilterRule{Action: rule.FilterAction(f.GetAction()), SchemaMatch: rule.MatchType(f.GetSchemaMatch()), SchemaPattern: f.GetSchemaPattern(), TableMatch: rule.MatchType(f.GetTableMatch()), TablePattern: f.GetTablePattern()})
		}
	}
	plan, err := rule.Compile(rule.RuleSet{ObjectFilters: filters}, rule.SideSource, sensitive)
	if err != nil {
		return nil, &pipelineOverlapError{"pipeline_scope_unverifiable", "所选策略过滤规则无效，不能验证同步范围"}
	}
	return plan, nil
}
