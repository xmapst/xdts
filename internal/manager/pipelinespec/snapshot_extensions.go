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

func snapshotExtensionPair(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) (bool, error) {
	if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
		return false, nil
	}
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return false, err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilitySnapshotPlanExtensions) {
			return false, nil
		}
	}
	return true, nil
}

func snapshotTableCreationPair(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) (bool, error) {
	if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
		return false, nil
	}
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return false, err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilitySnapshotTableCreation) {
			return false, nil
		}
	}
	return true, nil
}

func ddlSourceDependenciesPair(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) (bool, error) {
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return false, err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityDDLSourceDependencies) {
			return false, nil
		}
	}
	return true, nil
}

func snapshotRecoveryPair(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) (bool, error) {
	if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
		return false, nil
	}
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return false, err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilitySnapshotTableRecovery) {
			return false, nil
		}
	}
	return true, nil
}
