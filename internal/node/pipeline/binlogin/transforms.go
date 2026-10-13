package binlogin

import (
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

func (a *aggregator) transformRows(info *tableMapInfo, rows []*iproto.RowChange) error {
	if !a.transforms.HasTable(info.schema, info.table) || len(rows) == 0 {
		return nil
	}
	if a.schemaTrack == nil {
		return rule.ErrTransformConfig
	}
	table, ok := a.schemaTrack.Lookup(info.schema, info.table)
	if !ok || !table.MetadataComplete {
		return rule.ErrTransformConfig
	}
	keys := make(map[string]bool)
	for _, name := range table.PrimaryKey {
		keys[strings.ToLower(name)] = true
	}
	for _, key := range table.UniqueKeys {
		for _, name := range key.Columns {
			if name == "" {
				return rule.ErrTransformConfig
			}
			keys[strings.ToLower(name)] = true
		}
	}
	for _, name := range a.transforms.Columns(info.schema, info.table) {
		if keys[name] {
			return rule.ErrTransformConfig
		}
	}
	for _, row := range rows {
		var err error
		row.Before, err = iproto.TransformRow(a.transforms, info.schema, info.table, row.ColumnNames, row.Before)
		if err != nil {
			return err
		}
		row.After, err = iproto.TransformRow(a.transforms, info.schema, info.table, row.ColumnNames, row.After)
		if err != nil {
			return err
		}
	}
	return nil
}
