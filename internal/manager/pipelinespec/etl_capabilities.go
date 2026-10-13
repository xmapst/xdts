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

func RequireETLCapabilities(ctx context.Context, db *gorm.DB, p *model.Pipeline, doc *StrategyRulesDoc) error {
	if doc == nil {
		return nil
	}
	var source, target bool
	if doc.Source != nil {
		for _, row := range doc.Source.RowFilters {
			if row.GetLanguage() == "lua" {
				source = true
			}
		}
	}
	if doc.Target != nil {
		for _, transform := range doc.Target.Transforms {
			if transform.GetKind() != iproto.CleanseTransformKind_CLEANSE_TRANSFORM_KIND_LUA && transform.GetKind() != iproto.CleanseTransformKind_CLEANSE_TRANSFORM_KIND_PLUGIN {
				continue
			}
			if transform.GetExecutionSide() == iproto.TransformExecutionSide_TRANSFORM_EXECUTION_SIDE_SOURCE {
				source = true
			} else {
				target = true
			}
		}
	}
	for _, role := range []struct {
		needed bool
		node   uuidx.UUID
	}{{source, p.SourceNodeID}, {target, p.TargetNodeID}} {
		if !role.needed {
			continue
		}
		node, err := repo.GetNode(ctx, db, role.node)
		if err != nil {
			return err
		}
		var capabilities []string
		if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityScriptETL) {
			return errors.New("执行 Lua 或插件 ETL 的节点需要升级并声明 script_etl_v1 能力")
		}
	}
	return nil
}
