package binlogin

import (
	"errors"
	"fmt"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/schemamig"
)

var errSnapshotDDLIncompatible = errors.New("binlogin: snapshot DDL incompatible")

type iDDLOperationFilter interface{ AllowDDL() bool }

func (a *aggregator) allowDDL(schema, ddl string) (bool, error) {
	if filter, ok := a.tableFilter.(iDDLOperationFilter); ok && !filter.AllowDDL() {
		return false, nil
	}
	objects, known := schemamig.DDLObjects(schema, ddl)
	if !known {
		if dblineage.Reserved(schema) {
			return false, nil
		}
		return true, nil
	} // Unrecognized syntax is validated by the target rewrite path.
	allowed := 0
	for _, object := range objects {
		if !dblineage.Reserved(object.Schema) && a.tableFilter.MatchTable(object.Schema, object.Table) {
			allowed++
		}
	}
	if allowed == 0 {
		return false, nil
	}
	if allowed != len(objects) {
		return false, fmt.Errorf("binlogin: DDL crosses included and excluded objects; split the source DDL")
	}
	return true, nil
}
