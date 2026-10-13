package dispatch

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func optionalReinitUUID(id uuidx.UUID) string {
	if id.IsZero() {
		return ""
	}
	return id.String()
}

func (d *Dispatcher) recordReinitResult(ctx context.Context, id, node uuidx.UUID, cmdSeq uint64, result *iproto.PipelineCmdResult) error {
	if result.GetCmdType() != iproto.PipelineCmdType_PIPELINE_CMD_TYPE_REINIT {
		return nil
	}
	sides := make([]model.Side, 0, len(result.GetSides()))
	seen := map[iproto.PipelineRole]bool{}
	for _, side := range result.GetSides() {
		if side == nil || seen[side.GetRole()] {
			return fmt.Errorf("dispatch: invalid reinit side results")
		}
		seen[side.GetRole()] = true
		var role model.Side
		switch side.GetRole() {
		case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
			role = model.SideSource
		case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
			role = model.SideTarget
		default:
			return fmt.Errorf("dispatch: invalid reinit side role")
		}
		if side.GetError() == nil || (side.GetError().GetCode() == "" && side.GetError().GetMessage() == "") {
			sides = append(sides, role)
		}
	}
	if len(sides) == 0 {
		return nil
	}
	return repo.NewReinitializations(d.db).RecordApplied(ctx, id, node, result.GetConfigVersion(), cmdSeq, sides)
}
