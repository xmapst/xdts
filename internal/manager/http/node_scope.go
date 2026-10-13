package http

import (
	"context"
	"encoding/json"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
)

// Runtime bytes belong to this response. The stored heartbeat remains intact;
// only authorized pipeline references contribute to the response's count.
func (h *nodeHandler) scopeRuntime(ctx context.Context, node model.Node, pipelines []model.Pipeline) (model.Node, error) {
	user, scoped := repo.ReadScopeUser(ctx)
	if !scoped {
		return node, nil
	}
	restricted, err := repo.UserScopeRestricted(ctx, h.db, user)
	if err != nil {
		return model.Node{}, err
	}
	if !restricted {
		return node, nil
	}
	runtime, err := model.DecodeNodeRuntime(node.Runtime)
	if err != nil || runtime == nil {
		return node, err
	}
	known := runtime.PipelinesKnownCount != nil && uint64(*runtime.PipelinesKnownCount) == uint64(len(runtime.Pipelines))
	allowed := make(map[string]bool, len(pipelines))
	for _, p := range pipelines {
		allowed[p.ID.String()] = true
	}
	if runtime.Pipelines != nil {
		visible := make([]model.NodeRuntimePipeline, 0, len(runtime.Pipelines))
		for _, p := range runtime.Pipelines {
			if allowed[p.PipelineID] {
				visible = append(visible, p)
			}
		}
		runtime.Pipelines = visible
	}
	runtime.PipelinesKnownCount = nil
	if known {
		count := uint32(len(runtime.Pipelines))
		runtime.PipelinesKnownCount = &count
	}
	bytes, err := json.Marshal(runtime)
	if err != nil {
		return model.Node{}, err
	}
	node.Runtime = model.JSON(bytes)
	return node, nil
}
