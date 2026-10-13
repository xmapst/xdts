package snapshot

import (
	"fmt"
	"strings"
)

func snapshotProjection(baseline, current Table, key Key) ([]Column, error) {
	actual := make(map[string]Column, len(current.Columns))
	for _, col := range current.Columns {
		actual[strings.ToLower(col.Name)] = col
	}
	var columns []Column
	for _, col := range baseline.Columns {
		fresh, exists := actual[strings.ToLower(col.Name)]
		if !exists {
			continue
		}
		if !strings.EqualFold(col.DataType, fresh.DataType) || col.Unsigned != fresh.Unsigned || col.Generated != fresh.Generated || col.ColumnType != "" && !strings.EqualFold(col.ColumnType, fresh.ColumnType) || col.Collation != "" && !strings.EqualFold(col.Collation, fresh.Collation) {
			return nil, fmt.Errorf("snapshot: column %s.%s.%s changed type; reinitialize table", baseline.Schema, baseline.Name, col.Name)
		}
		if col.Nullable != fresh.Nullable {
			return nil, fmt.Errorf("snapshot: column nullability changed; reinitialize table")
		}
		columns = append(columns, col)
	}
	for _, col := range key.Columns {
		fresh, exists := actual[strings.ToLower(col.Name)]
		if !exists || fresh.Nullable {
			return nil, fmt.Errorf("snapshot: positioning column changed; reinitialize table")
		}
	}
	indexes := append([]Index{current.PrimaryKey}, current.UniqueKeys...)
	found := false
	for _, idx := range indexes {
		if idx.Name != key.Name || idx.Incomplete || len(idx.Columns) != len(key.Columns) {
			continue
		}
		same := true
		for i, col := range key.Columns {
			same = same && strings.EqualFold(idx.Columns[i], col.Name)
		}
		found = found || same
	}
	if !found {
		return nil, fmt.Errorf("snapshot: positioning index changed; reinitialize table")
	}
	oldUnique := make(map[string]Index)
	oldUnique[baseline.PrimaryKey.Name] = baseline.PrimaryKey
	for _, idx := range baseline.UniqueKeys {
		oldUnique[idx.Name] = idx
	}
	for _, idx := range indexes {
		if idx.Name == "" {
			continue
		}
		old, exists := oldUnique[idx.Name]
		if !exists || len(old.Columns) != len(idx.Columns) || old.Incomplete != idx.Incomplete {
			return nil, fmt.Errorf("snapshot: unique index changed; reinitialize table")
		}
		for i, col := range idx.Columns {
			if !strings.EqualFold(col, old.Columns[i]) {
				return nil, fmt.Errorf("snapshot: unique index changed; reinitialize table")
			}
		}
	}
	return columns, nil
}
