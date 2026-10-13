package binlogin

import (
	"errors"
	"fmt"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func lineageBytes(value any) ([]byte, bool) {
	switch value := value.(type) {
	case []byte:
		return value, true
	case string:
		if len(value) > iproto.MaxStoredLineageBytes {
			return nil, false
		}
		return []byte(value), true
	default:
		return nil, false
	}
}

// Reserved rows are handled before user filters and never become business rows.
// A DDL reservation has a different automatic GTID and is inert in this stream.
func (a *aggregator) feedLineageRows(e *replication.RowsEvent, change iproto.ChangeType) error {
	if !a.databaseRoute.Persistent {
		return nil
	}
	if change != iproto.ChangeType_CHANGE_TYPE_INSERT {
		return errors.New("binlogin: lineage records must be append-only")
	}
	for _, row := range e.Rows {
		if len(row) != 2 {
			return errors.New("binlogin: invalid lineage row width")
		}
		key, ok := lineageBytes(row[0])
		body, bodyOK := lineageBytes(row[1])
		if !ok || !bodyOK {
			return errors.New("binlogin: invalid lineage row values")
		}
		record, err := iproto.DecodeStoredDatabaseLineage(body, string(key))
		if err != nil {
			return err
		}
		if record.GTID != a.gtid {
			continue
		}
		if a.storedLineage != nil || len(a.rows) != 0 {
			return errors.New("binlogin: lineage metadata must precede business rows exactly once")
		}
		if a.budget != nil {
			if a.buffer == nil {
				a.buffer, err = a.budget.Reserve(0)
				if err != nil {
					return err
				}
			}
			if err := a.buffer.Grow(uint64(len(body))*4 + 256); err != nil {
				return err
			}
		}
		txn, err := record.SourceTxn(a.center, a.databaseRoute.SourceServerUUID)
		if err != nil {
			return err
		}
		a.storedLineage = txn
		a.originUnknown = false
		a.originRejected = a.lineageAllowed != nil && !a.lineageAllowed(txn, false)
	}
	return nil
}

func (a *aggregator) loadDDLLineage() error {
	if !a.databaseRoute.Persistent || a.loadLineage == nil {
		return nil
	}
	record, found, err := a.loadLineage(a.gtid)
	if err != nil {
		return fmt.Errorf("binlogin: restore DDL lineage: %w", err)
	}
	if !found {
		return nil
	}
	txn, err := record.SourceTxn(a.center, a.databaseRoute.SourceServerUUID)
	if err != nil {
		return err
	}
	a.storedLineage, a.originUnknown = txn, false
	return nil
}

func (a *aggregator) applyStoredOrigin(txn *iproto.Txn) bool {
	if a.storedLineage == nil {
		return false
	}
	source := a.storedLineage
	txn.OriginCenter, txn.Hops, txn.LineageVersion = source.OriginCenter, source.Hops, source.LineageVersion
	txn.Path, txn.EndpointPath = append([]string(nil), source.Path...), append([]string(nil), source.EndpointPath...)
	a.originUnknown = false
	return true
}

func (a *aggregator) isNativeLineageRoot() bool {
	return a.databaseRoute.Persistent && (strings.EqualFold(a.sourceUUID, a.databaseRoute.SourceServerUUID) || a.nativeGroupUUID != "" && strings.EqualFold(a.sourceUUID, a.nativeGroupUUID))
}

func reservedLineageTable(e *replication.TableMapEvent) bool {
	return dblineage.Reserved(string(e.Schema)) && strings.EqualFold(string(e.Table), dblineage.Table)
}

func reservedLineageDDL(schema, statement string) bool {
	objects, known := schemamig.DDLObjects(schema, statement)
	if !known {
		return dblineage.Reserved(schema)
	}
	if len(objects) == 0 {
		return false
	}
	for _, object := range objects {
		if !dblineage.Reserved(object.Schema) {
			return false
		}
	}
	return true
}
