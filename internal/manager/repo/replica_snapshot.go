package repo

import (
	"encoding/json"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// SnapshotReplicaEndpointID extracts the additional immutable endpoint reference.
// It never resolves credentials or silently ignores an invalid read policy.
func SnapshotReplicaEndpointID(rules model.JSON) (uuidx.UUID, bool, error) {
	var doc struct {
		Source *struct {
			Snapshot *struct {
				ReadFromReplica bool   `json:"read_from_replica"`
				EndpointID      string `json:"replica_endpoint_id"`
			} `json:"snapshot"`
		} `json:"source"`
	}
	if err := json.Unmarshal(rules, &doc); err != nil {
		return uuidx.UUID{}, false, err
	}
	if doc.Source == nil || doc.Source.Snapshot == nil {
		return uuidx.UUID{}, false, nil
	}
	params := &iproto.SnapshotParams{ReadFromReplica: doc.Source.Snapshot.ReadFromReplica, ReplicaEndpointId: doc.Source.Snapshot.EndpointID}
	if err := iproto.ValidateSnapshotReplicaPolicy(params); err != nil {
		return uuidx.UUID{}, false, err
	}
	if !params.GetReadFromReplica() {
		return uuidx.UUID{}, false, nil
	}
	id, err := uuidx.Parse(params.GetReplicaEndpointId())
	return id, true, err
}

// The immutable strategy version pins the read endpoint. Include this reference
// in both config refresh and deletion protection, including unfinished cleanup.
const pipelineReplicaEndpointReference = `EXISTS (
	SELECT 1 FROM strategy_versions replica_strategy
	WHERE replica_strategy.strategy_id = pipelines.strategy_id
	AND replica_strategy.version = pipelines.strategy_version
	AND JSON_UNQUOTE(JSON_EXTRACT(replica_strategy.rules, '$.source.snapshot.read_from_replica')) = 'true'
	AND JSON_UNQUOTE(JSON_EXTRACT(replica_strategy.rules, '$.source.snapshot.replica_endpoint_id')) = ?
)`
