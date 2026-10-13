package pipelinespec

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

func SupportsPersistentLineage(node *model.Node) bool {
	if node == nil {
		return false
	}
	var capabilities []string
	return json.Unmarshal(node.Capabilities, &capabilities) == nil && iproto.ValidateNodeCapabilities(capabilities) == nil && slices.Contains(capabilities, iproto.CapabilityPersistentLineage)
}

func persistentLineagePair(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) (bool, error) {
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return false, err
		}
		if !SupportsPersistentLineage(node) {
			return false, nil
		}
	}
	return true, nil
}
