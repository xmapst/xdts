package dispatch

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// A previous release proves this authenticated node formerly owned the role.
// Only a release notice is returned: connection credentials and shared queues
// remain with the current owner. Arbitrary non-owners receive no projection.
func (d *Dispatcher) releasedPipelineConfigs(ctx context.Context, p *model.Pipeline, nodeID uuidx.UUID) ([]*iproto.PipelineConfigItem, error) {
	var sides []model.Side
	if err := d.db.WithContext(ctx).Model(&model.PipelineRelease{}).Distinct("side").Where("pipeline_id=? AND node_id=? AND config_version<?", p.ID, nodeID, p.ConfigVersion).Pluck("side", &sides).Error; err != nil {
		return nil, err
	}
	var items []*iproto.PipelineConfigItem
	for _, side := range sides {
		role := iproto.PipelineRole_PIPELINE_ROLE_SOURCE
		owner := p.SourceNodeID
		if side == model.SideTarget {
			role, owner = iproto.PipelineRole_PIPELINE_ROLE_TARGET, p.TargetNodeID
		} else if side != model.SideSource {
			continue
		}
		if owner != nodeID {
			items = append(items, &iproto.PipelineConfigItem{PipelineId: p.ID.String(), ConfigVersion: p.ConfigVersion, Role: role, OwnershipReleased: true})
		}
	}
	return items, nil
}
