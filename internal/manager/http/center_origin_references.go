package http

import (
	"context"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func (h *centerHandler) resolveOriginReferences(ctx context.Context, centerID uuidx.UUID, views []originUUIDView) error {
	var endpointIDs, pipelineIDs []uuidx.UUID
	for _, view := range views {
		id, err := uuidx.Parse(view.RefID)
		if err != nil || id.IsZero() {
			continue
		}
		switch model.OriginKind(view.Kind) {
		case model.OriginKindServer, model.OriginKindGroup:
			endpointIDs = append(endpointIDs, id)
		case model.OriginKindSynthetic:
			pipelineIDs = append(pipelineIDs, id)
		}
	}
	endpointNames, pipelineNames := map[string]string{}, map[string]string{}
	if len(endpointIDs) > 0 {
		var endpoints []model.Endpoint
		q := repo.ReadResourceScope(ctx, h.db.WithContext(ctx).Model(&model.Endpoint{}), repo.ReadEndpoint)
		if err := q.Select("id", "name").Where("id IN ? AND center_id=?", endpointIDs, centerID).Find(&endpoints).Error; err != nil {
			return err
		}
		for _, endpoint := range endpoints {
			endpointNames[endpoint.ID.String()] = endpoint.Name
		}
	}
	if len(pipelineIDs) > 0 {
		var pipelines []model.Pipeline
		q := repo.ReadResourceScope(ctx, h.db.WithContext(ctx).Model(&model.Pipeline{}), repo.ReadPipeline)
		if err := q.Select("id", "name").Where("id IN ? AND source_endpoint_id IN (SELECT id FROM endpoints WHERE center_id=?)", pipelineIDs, centerID).Find(&pipelines).Error; err != nil {
			return err
		}
		for _, pipeline := range pipelines {
			pipelineNames[pipeline.ID.String()] = pipeline.Name
		}
	}
	for i := range views {
		id, err := uuidx.Parse(views[i].RefID)
		if err != nil {
			continue
		}
		switch model.OriginKind(views[i].Kind) {
		case model.OriginKindServer, model.OriginKindGroup:
			if name := endpointNames[id.String()]; name != "" {
				views[i].RefName, views[i].RefType = name, "endpoint"
			}
		case model.OriginKindSynthetic:
			if name := pipelineNames[id.String()]; name != "" {
				views[i].RefName, views[i].RefType = name, "pipeline"
			}
		}
	}
	return nil
}
