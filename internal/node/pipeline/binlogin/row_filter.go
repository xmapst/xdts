package binlogin

import (
	"errors"
	"strings"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
)

// Evaluate complete source images before projecting columns. A boundary
// crossing changes the emitted operation without reapplying operation switches.
func buildFilteredRowChanges(event *replication.RowsEvent, change iproto.ChangeType, info *tableMapInfo, columns IColumnFilter, filter iproto.ISourceRowPlan) ([]*iproto.RowChange, error) {
	if filter == nil || !filter.HasRowFilters(info.schema, info.table) {
		return buildRowChanges(event, change, info, columns)
	}
	projected, kept := filterColumns(info, columns)
	if len(projected) == 0 || len(primaryKeyColumnNames(info, kept)) != len(info.primaryKeyIdx) {
		return nil, rule.ErrRowEvaluation
	}
	for _, name := range filter.RowFilterColumns(info.schema, info.table) {
		for i, column := range info.meta {
			if strings.EqualFold(name, column.Name) && i < len(info.generated) && info.generated[i] {
				return nil, rule.ErrRowEvaluation
			}
		}
	}
	if change != iproto.ChangeType_CHANGE_TYPE_INSERT && change != iproto.ChangeType_CHANGE_TYPE_DELETE && change != iproto.ChangeType_CHANGE_TYPE_UPDATE {
		return nil, errors.New("binlogin: unsupported filtered operation")
	}
	step := 1
	if change == iproto.ChangeType_CHANGE_TYPE_UPDATE {
		step = 2
	}
	if len(event.Rows)%step != 0 {
		return nil, errors.New("binlogin: incomplete filtered row images")
	}
	names := filter.RowFilterColumns(info.schema, info.table)
	indices := make([]int, len(names))
	for i, name := range names {
		found := false
		for index, column := range info.meta {
			if strings.EqualFold(name, column.Name) {
				indices[i], found = index, true
				break
			}
		}
		if !found {
			return nil, rule.ErrRowEvaluation
		}
	}
	match := func(raw []any) (bool, error) {
		if len(raw) != len(info.meta) {
			return false, rule.ErrRowEvaluation
		}
		size := 0
		for _, index := range indices {
			switch value := raw[index].(type) {
			case string:
				size += len(value)
			case []byte:
				size += len(value)
			}
			if size > 4*1024*1024 {
				return false, rule.ErrRowEvaluation
			}
		}
		row, err := decodeRow(raw, info, indices)
		if err != nil {
			return false, rule.ErrRowEvaluation
		}
		values := make([]any, len(row.Values))
		for i, value := range row.Values {
			values[i], err = iproto.RowFilterValue(value)
			if err != nil {
				return false, rule.ErrRowEvaluation
			}
		}
		return filter.MatchRow(info.schema, info.table, names, values)
	}
	var result []*iproto.RowChange
	for i := 0; i < len(event.Rows); i += step {
		before, err := match(event.Rows[i])
		if err != nil {
			return nil, err
		}
		selected := event.Rows[i : i+step]
		operation := change
		if step == 2 {
			after, err := match(event.Rows[i+1])
			if err != nil {
				return nil, err
			}
			if !before && !after {
				continue
			}
			if !before {
				operation, selected = iproto.ChangeType_CHANGE_TYPE_INSERT, event.Rows[i+1:i+2]
			} else if !after {
				operation, selected = iproto.ChangeType_CHANGE_TYPE_DELETE, event.Rows[i:i+1]
			}
		} else if !before {
			continue
		}
		ownedEvent := *event
		ownedEvent.Rows = selected
		rows, err := buildRowChanges(&ownedEvent, operation, info, columns)
		if err != nil {
			return nil, err
		}
		result = append(result, rows...)
	}
	return result, nil
}
