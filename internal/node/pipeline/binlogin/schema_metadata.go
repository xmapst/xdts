package binlogin

import (
	"fmt"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/node/schema"
)

// TableMap types and optional attributes take precedence. Only complete,
// ordered history may fill absent metadata; stored snapshots keep their keys
// and cannot silently claim to contain newly added column attributes.
func (a *aggregator) buildSchemaMetadata(event *replication.TableMapEvent) ([]mysqlx.ColumnMeta, []uint64, error) {
	meta, err := mysqlx.BuildColumnMeta(event)
	if err != nil {
		return nil, nil, err
	}
	keys := append([]uint64(nil), event.PrimaryKey...)
	if a.schemaTrack == nil {
		return nil, nil, fmt.Errorf("%w: schema tracker required", errSchemaHistory)
	}
	table, found := a.schemaTrack.Lookup(string(event.Schema), string(event.Table))
	complete := found && table.MetadataComplete && len(table.Columns) == len(meta)
	unsigned := event.UnsignedMap()
	for i := range meta {
		column := schema.ColumnStructure{}
		if complete {
			column = table.Columns[i]
		}
		if meta[i].Name == "" {
			if !complete {
				return nil, nil, fmt.Errorf("%w: complete ordered columns required for missing TableMap metadata", errSchemaHistory)
			}
			meta[i].Name = column.Name
		} else if complete && !strings.EqualFold(meta[i].Name, column.Name) {
			// Explicit event names can locate a historical column independently
			// of its stored order. They must never be overwritten by fallback.
			column = schema.ColumnStructure{}
			for _, candidate := range table.Columns {
				if strings.EqualFold(meta[i].Name, candidate.Name) {
					column = candidate
					break
				}
			}
		}
		if _, supplied := unsigned[i]; !supplied {
			meta[i].Unsigned = column.Unsigned
		}
		if meta[i].MySQLType == gmysql.MYSQL_TYPE_ENUM && len(meta[i].EnumValues) == 0 {
			meta[i].EnumValues = append([]string(nil), column.EnumValues...)
			if len(meta[i].EnumValues) == 0 {
				return nil, nil, fmt.Errorf("%w: ENUM member metadata missing", errSchemaHistory)
			}
		}
		if meta[i].MySQLType == gmysql.MYSQL_TYPE_SET && len(meta[i].SetValues) == 0 {
			meta[i].SetValues = append([]string(nil), column.SetValues...)
			if len(meta[i].SetValues) == 0 {
				return nil, nil, fmt.Errorf("%w: SET member metadata missing", errSchemaHistory)
			}
		}
	}
	if len(keys) == 0 && complete {
		for _, name := range table.PrimaryKey {
			for i, column := range meta {
				if strings.EqualFold(name, column.Name) {
					keys = append(keys, uint64(i))
					break
				}
			}
		}
	}
	return meta, keys, nil
}
