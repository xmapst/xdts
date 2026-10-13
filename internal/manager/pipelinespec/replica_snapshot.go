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

func ValidateSnapshotReplicaRules(doc *StrategyRulesDoc) error {
	if doc == nil || doc.Source == nil {
		return nil
	}
	return iproto.ValidateSnapshotReplicaPolicy(doc.Source.Snapshot)
}

func ResolveSnapshotReplicaEndpoint(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline,
	source *model.Endpoint, params *iproto.SnapshotParams) (*iproto.EndpointConn, error) {
	replica, err := resolveSnapshotReplicaEndpoint(ctx, db, pipeline, source, params)
	if err != nil || replica == nil {
		return nil, err
	}
	return buildEndpointConn(replica)
}

func resolveSnapshotReplicaEndpoint(ctx context.Context, db *gorm.DB, pipeline *model.Pipeline,
	source *model.Endpoint, params *iproto.SnapshotParams) (*model.Endpoint, error) {
	if err := iproto.ValidateSnapshotReplicaPolicy(params); err != nil {
		return nil, err
	}
	if !params.GetReadFromReplica() {
		return nil, nil
	}
	if pipeline.InitMode != model.InitModeFullAndIncremental && pipeline.InitMode != model.InitModeFullOnly {
		return nil, errors.New("pipelinespec: replica snapshot requires snapshot initialization")
	}
	id, _ := uuidx.Parse(params.GetReplicaEndpointId())
	replica, err := repo.GetEndpoint(ctx, db, id)
	if err != nil {
		return nil, errors.New("pipelinespec: registered replica endpoint unavailable")
	}
	if replica.CenterID != source.CenterID || replica.ID == source.ID || source.ServerUUID == "" ||
		replica.ServerUUID == "" || replica.ServerUUID == source.ServerUUID || source.LowerCaseTableNames == "" || replica.LowerCaseTableNames != source.LowerCaseTableNames {
		return nil, errors.New("pipelinespec: snapshot replica must be a distinct probed instance in the source center with the same identifier policy")
	}
	for _, value := range []string{source.ServerUUID, replica.ServerUUID} {
		id, err := uuidx.Parse(value)
		if err != nil || id.IsZero() || id.String() != value {
			return nil, errors.New("pipelinespec: snapshot instances require canonical probed UUIDs")
		}
	}
	switch source.LowerCaseTableNames {
	case "SENSITIVE", "LOWERCASE_STORED", "LOWERCASE_COMPARE":
	default:
		return nil, errors.New("pipelinespec: snapshot identifier policy is not probed")
	}
	node, err := repo.GetNode(ctx, db, pipeline.SourceNodeID)
	if err != nil {
		return nil, err
	}
	var capabilities []string
	if len(node.Capabilities) != 0 {
		if err := json.Unmarshal(node.Capabilities, &capabilities); err != nil {
			return nil, errors.New("pipelinespec: node capabilities are invalid")
		}
	}
	if err := iproto.ValidateNodeCapabilities(capabilities); err != nil {
		return nil, err
	}
	if !slices.Contains(capabilities, iproto.CapabilityReplicaSnapshot) {
		return nil, errors.New("pipelinespec: source node does not support replica snapshots")
	}
	return replica, nil
}
