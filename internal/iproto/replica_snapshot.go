package iproto

import (
	"errors"
	"uuid"
)

// ValidateSnapshotReplicaPolicy validates the stored endpoint reference. The
// Manager resolves credentials; the source worker verifies actual SQL identity.
func ValidateSnapshotReplicaPolicy(params *SnapshotParams) error {
	if !params.GetReadFromReplica() {
		if params.GetReplicaEndpointId() != "" {
			return errors.New("iproto: replica endpoint requires replica snapshot mode")
		}
		return nil
	}
	id, err := uuid.Parse(params.GetReplicaEndpointId())
	if err != nil || id == uuid.Nil() || id.String() != params.GetReplicaEndpointId() {
		return errors.New("iproto: replica snapshot requires a canonical endpoint UUID")
	}
	return nil
}
