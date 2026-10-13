package repo

import (
	"context"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/topologycheck"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// initializationEdges reads only endpoint identities and their owning centers.
// The complete graph is loaded once, including during startup validation.
func initializationEdges(ctx context.Context, db *gorm.DB, pipelines []model.Pipeline) ([]topologycheck.InitializationEdge, error) {
	ids := make([]uuidx.UUID, 0, len(pipelines)*2)
	for _, p := range pipelines {
		ids = append(ids, p.SourceEndpointID, p.TargetEndpointID)
	}
	var endpoints []model.Endpoint
	if err := db.WithContext(ctx).Select("id", "center_id").Where("id IN ?", ids).Find(&endpoints).Error; err != nil {
		return nil, err
	}
	centers := map[uuidx.UUID]string{}
	for _, e := range endpoints {
		centers[e.ID] = e.CenterID.String()
	}
	edges := make([]topologycheck.InitializationEdge, 0, len(pipelines))
	for _, p := range pipelines {
		source, ok := centers[p.SourceEndpointID]
		if !ok {
			return nil, ErrEndpointNotFound
		}
		target, ok := centers[p.TargetEndpointID]
		if !ok {
			return nil, ErrEndpointNotFound
		}
		edges = append(edges, topologycheck.InitializationEdge{ID: p.ID.String(), Source: source, Target: target, Mode: string(p.InitMode), SchemaMigration: !p.SchemaUUID.IsZero()})
	}
	return edges, nil
}

// CheckBidirectionalInitialization reads authoritative endpoints and committed
// schema identities. Configuration writers hold the topology lock.
func CheckBidirectionalInitialization(ctx context.Context, db *gorm.DB, candidate *model.Pipeline, confirmingSchema bool) error {
	var topology model.Topology
	if err := db.WithContext(ctx).Where("id=?", candidate.TopologyID).Find(&topology).Error; err != nil {
		return err
	}
	if topology.Type != model.TopologyTypeBidirectional && topology.ID.String() != AutomaticTopologyID {
		return nil
	}
	pipelines, err := ListPipelines(ctx, db, topologyPipelineFilter(candidate.TopologyID))
	if err != nil {
		return err
	}
	selected := []model.Pipeline{*candidate}
	for _, p := range pipelines {
		if topology.ID.String() == AutomaticTopologyID && (p.SourceEndpointID != candidate.TargetEndpointID || p.TargetEndpointID != candidate.SourceEndpointID) {
			continue
		}
		if p.ID != candidate.ID {
			selected = append(selected, p)
		}
	}
	edges, err := initializationEdges(ctx, db, selected)
	if err != nil {
		return err
	}
	edges[0].SchemaMigration = edges[0].SchemaMigration || confirmingSchema
	return topologycheck.ValidateInitialization(edges[0], edges[1:])
}

func CheckTopologyInitialization(ctx context.Context, db *gorm.DB, id uuidx.UUID) error {
	topology, err := GetTopology(ctx, db, id)
	if err != nil {
		return err
	}
	if topology.Type != model.TopologyTypeBidirectional && topology.ID.String() != AutomaticTopologyID {
		return nil
	}
	pipelines, err := ListPipelines(ctx, db, topologyPipelineFilter(id))
	if err != nil {
		return err
	}
	if len(pipelines) == 0 {
		return nil
	}
	edges, err := initializationEdges(ctx, db, pipelines)
	if err != nil {
		return err
	}
	for i, e := range edges {
		selected := edges
		if id.String() == AutomaticTopologyID {
			selected = nil
			for j, other := range pipelines {
				if other.SourceEndpointID == pipelines[i].TargetEndpointID && other.TargetEndpointID == pipelines[i].SourceEndpointID {
					selected = append(selected, edges[j])
				}
			}
		}
		if err := topologycheck.ValidateInitialization(e, selected); err != nil {
			return err
		}
	}
	return nil
}
