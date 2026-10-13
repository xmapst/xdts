package pipelinespec

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"gorm.io/gorm"
)

var ErrFullOnlyTargetCapability = errors.New("pipelinespec: full-only target does not advertise stopping at H")

func ValidateFullOnlyTarget(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline) error {
	if pipeline.InitMode != model.InitModeFullOnly {
		return nil
	}
	node, err := repo.GetNode(ctx, db, pipeline.TargetNodeID)
	if err != nil {
		return err
	}
	if !SupportsFullOnlyStop(node) {
		return ErrFullOnlyTargetCapability
	}
	return nil
}

func SupportsFullOnlyStop(node *model.Node) bool {
	if node == nil {
		return false
	}
	var capabilities []string
	return json.Unmarshal(node.Capabilities, &capabilities) == nil && iproto.ValidateNodeCapabilities(capabilities) == nil && slices.Contains(capabilities, iproto.CapabilityFullOnlyStop)
}
