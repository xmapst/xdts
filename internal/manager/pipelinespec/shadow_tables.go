package pipelinespec

import (
	"context"
	"encoding/json"
	"errors"
	"slices"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

func requireShadowTableCapabilities(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline, rules *StrategySourceRules, sourceCaseSensitive bool) error {
	if rules == nil || len(rules.ShadowTables) == 0 {
		return nil
	}
	if _, err := iproto.CompileShadowTables(rules.ShadowTables, sourceCaseSensitive); err != nil {
		return err
	}
	for _, id := range [2]uuidx.UUID{pipeline.SourceNodeID, pipeline.TargetNodeID} {
		node, err := repo.GetNode(ctx, db, id)
		if err != nil {
			return err
		}
		var capabilities []string
		if len(node.Capabilities) != 0 {
			if err := json.Unmarshal(node.Capabilities, &capabilities); err != nil {
				return errors.New("pipelinespec: node capabilities are invalid")
			}
		}
		if err := iproto.ValidateNodeCapabilities(capabilities); err != nil {
			return err
		}
		if !slices.Contains(capabilities, iproto.CapabilityShadowTables) {
			return errors.New("pipelinespec: both assigned nodes must advertise shadow_tables_v1 before enabling shadow tables")
		}
	}
	return nil
}
