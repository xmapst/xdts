package iproto

import (
	"errors"
	"slices"
	"strings"
	"uuid"
)

// DatabaseLineageRoute is an independently owned immutable runtime value.
type DatabaseLineageRoute struct {
	SourceCenter, TargetCenter         string
	SourceServerUUID, TargetServerUUID string
	Persistent                         bool
}

func DatabaseLineageRouteFromProto(route *DatabaseRoute) (DatabaseLineageRoute, error) {
	if route == nil {
		return DatabaseLineageRoute{}, nil
	}
	value := DatabaseLineageRoute{SourceCenter: route.GetSourceCenter(), TargetCenter: route.GetTargetCenter(),
		SourceServerUUID: route.GetSourceServerUuid(), TargetServerUUID: route.GetTargetServerUuid(), Persistent: route.GetPersistentLineageV1()}
	return value, value.Validate()
}

func (value DatabaseLineageRoute) Validate() error {
	if value == (DatabaseLineageRoute{}) {
		return nil
	}
	if strings.TrimSpace(value.SourceCenter) == "" || strings.TrimSpace(value.TargetCenter) == "" || strings.TrimSpace(value.SourceCenter) != value.SourceCenter || strings.TrimSpace(value.TargetCenter) != value.TargetCenter || len(value.SourceCenter) > 64 || len(value.TargetCenter) > 64 {
		return errors.New("iproto: database route centers required")
	}
	for _, text := range []string{value.SourceServerUUID, value.TargetServerUUID} {
		// Unprobed identities cannot authorize a same-center transfer.
		if text != "" && !canonicalDatabaseUUID(text) {
			return errors.New("iproto: invalid database route UUID")
		}
	}
	if value.Persistent && (!canonicalDatabaseUUID(value.SourceServerUUID) || !canonicalDatabaseUUID(value.TargetServerUUID) || value.SourceServerUUID == value.TargetServerUUID) {
		return errors.New("iproto: persistent lineage requires distinct probed databases")
	}
	return nil
}

func canonicalDatabaseUUID(text string) bool {
	id, err := uuid.Parse(text)
	return err == nil && id != uuid.Nil() && id.String() == text
}

func ValidateTxnLineage(txn *Txn) error {
	if txn.GetLineageVersion() > StoredLineageVersion || txn.GetLineageVersion() != 0 && len(txn.GetEndpointPath()) == 0 {
		return errors.New("iproto: unsupported or incomplete lineage proof")
	}
	if len(txn.GetEndpointPath()) > 256 {
		return errors.New("iproto: endpoint lineage exceeds 256 databases")
	}
	seen := make(map[string]bool, len(txn.GetEndpointPath()))
	for _, text := range txn.GetEndpointPath() {
		if !canonicalDatabaseUUID(text) || seen[text] {
			return errors.New("iproto: invalid or repeated endpoint lineage UUID")
		}
		seen[text] = true
	}
	return nil
}

// AllowsTxn applies center and physical endpoint route guards before a transfer.
func (r DatabaseLineageRoute) AllowsTxn(txn *Txn) bool {
	if ValidateTxnLineage(txn) != nil {
		return false
	}
	path := txn.GetEndpointPath()
	if r.TargetServerUUID != "" {
		origin, _, _ := strings.Cut(txn.GetGtid(), ":")
		id, err := uuid.Parse(origin)
		if slices.Contains(path, r.TargetServerUUID) || (err == nil && id.String() == r.TargetServerUUID) {
			return false
		}
	}
	if r.SourceCenter != "" && r.SourceCenter == r.TargetCenter {
		return canonicalDatabaseUUID(r.SourceServerUUID) && canonicalDatabaseUUID(r.TargetServerUUID) && r.SourceServerUUID != r.TargetServerUUID && len(path) > 0 && path[len(path)-1] == r.SourceServerUUID
	}
	return r.TargetCenter == "" || !slices.Contains(txn.GetPath(), r.TargetCenter)
}
