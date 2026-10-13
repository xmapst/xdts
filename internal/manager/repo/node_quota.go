package repo

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const MaxNodePipelineQuota uint32 = 10000

var ErrNodePipelineQuota = errors.New("repo: node pipeline quota exceeded")

type NodePipelineQuotaError struct {
	NodeID   uuidx.UUID
	Limit    uint32
	Assigned int
}

func (e *NodePipelineQuotaError) Error() string {
	return fmt.Sprintf("%s: node=%s assigned=%d limit=%d", ErrNodePipelineQuota, e.NodeID, e.Assigned, e.Limit)
}
func (*NodePipelineQuotaError) Unwrap() error { return ErrNodePipelineQuota }

// The caller holds the node lock. A locking read sees allocations committed by
// an earlier lock owner even inside a preexisting REPEATABLE READ transaction.
func countNodeAllocations(ctx context.Context, tx *gorm.DB, nodeID uuidx.UUID) (int, error) {
	var allocated []struct{ ID uuidx.UUID }
	err := tx.WithContext(ctx).Unscoped().Model(&model.Pipeline{}).Select("id").
		Where("(source_node_id = ? OR target_node_id = ?) AND (deleted_at IS NULL OR cleanup_state <> ?)", nodeID, nodeID, model.CleanupStateDone).
		Clauses(clause.Locking{Strength: "UPDATE"}).Find(&allocated).Error
	return len(allocated), err
}

func checkPipelineNodeQuotas(ctx context.Context, tx *gorm.DB, pipeline *model.Pipeline) error {
	ids := []uuidx.UUID{pipeline.SourceNodeID}
	if pipeline.TargetNodeID != pipeline.SourceNodeID {
		ids = append(ids, pipeline.TargetNodeID)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	limits := make([]*model.Node, 0, len(ids))
	for _, id := range ids {
		var nodes []model.Node
		if err := tx.WithContext(ctx).Unscoped().Clauses(clause.Locking{Strength: "UPDATE"}).Where("id = ?", id).Find(&nodes).Error; err != nil {
			return err
		}
		// Endpoint/node identity validation remains at the existing HTTP boundary.
		// Older repository callers can store references before creating nodes.
		if len(nodes) > 0 {
			if nodes[0].DeletedAt.Valid || nodes[0].Status == model.NodeStatusDisabled || nodes[0].Status == model.NodeStatusRevoked {
				return ErrNodeDisabledOrRevoked
			}
			limits = append(limits, &nodes[0])
		}
	}
	// Acquire every node before taking allocation row locks. Interleaving the
	// two lock classes would deadlock with an edit on the second node.
	for _, node := range limits {
		if node.MaxPipelines == 0 {
			continue
		}
		count, err := countNodeAllocations(ctx, tx, node.ID)
		if err != nil {
			return err
		}
		if count >= int(node.MaxPipelines) {
			return &NodePipelineQuotaError{NodeID: node.ID, Limit: node.MaxPipelines, Assigned: count}
		}
	}
	return nil
}
