package repo

import (
	"context"
	"errors"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

func stateBundleRole(side model.Side) iproto.PipelineRole {
	if side == model.SideSource {
		return iproto.PipelineRole_PIPELINE_ROLE_SOURCE
	}
	return iproto.PipelineRole_PIPELINE_ROLE_TARGET
}

func validateMigrationState(p *model.Pipeline, side model.Side, oldNode, newNode uuidx.UUID, bundle *iproto.PipelineStateBundle) error {
	if iproto.ValidateStateBundle(bundle) != nil || bundle.PipelineId != p.ID.String() || bundle.RunId != p.RunID || bundle.Role != stateBundleRole(side) || bundle.SourceNodeId != oldNode.String() || bundle.DestinationNodeId != newNode.String() || bundle.SnapshotUuid != p.SnapshotUUID.String() || bundle.ConfigVersion != p.ConfigVersion+1 {
		return ErrPipelineMigrationSnapshotState
	}
	return nil
}

// GetPipelineStateBundle returns only the recipient's current-run handover.
// Corruption of a matching durable bundle fails assembly, never falls back to
// rebuilding the source's plan against today's table boundaries.
func GetPipelineStateBundle(ctx context.Context, db *gorm.DB, p *model.Pipeline, side model.Side) (*iproto.PipelineStateBundle, error) {
	var stored model.PipelineStateBundle
	if err := db.WithContext(ctx).First(&stored, "pipeline_id=? AND side=?", p.ID, side).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	if len(stored.Data) > iproto.MaxStateBundleBytes {
		return nil, fmt.Errorf("repo: stored state bundle exceeds limits")
	}
	var bundle iproto.PipelineStateBundle
	if err := proto.Unmarshal(stored.Data, &bundle); err != nil {
		return nil, fmt.Errorf("repo: invalid stored state bundle")
	}
	if bundle.RunId != p.RunID {
		return nil, nil
	}
	node := p.SourceNodeID
	if side == model.SideTarget {
		node = p.TargetNodeID
	}
	if iproto.ValidateStateBundle(&bundle) != nil || bundle.PipelineId != p.ID.String() || bundle.Role != stateBundleRole(side) || bundle.DestinationNodeId != node.String() || bundle.SnapshotUuid != p.SnapshotUUID.String() || bundle.ConfigVersion > p.ConfigVersion {
		return nil, fmt.Errorf("repo: stored state bundle ownership differs")
	}
	return &bundle, nil
}
