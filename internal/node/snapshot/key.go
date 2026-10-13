package snapshot

import (
	"fmt"
	"slices"
	"strings"
)

// ChooseKey prefers the primary key, then a complete non-null unique index in
// stable index-name order. It never treats a nullable unique index as unique
// row positioning: MySQL permits multiple NULLs in such an index.
func ChooseKey(table Table) (Key, error) {
	if err := validateTable(table); err != nil {
		return Key{}, err
	}
	resolve := func(index Index) (Key, bool) {
		if index.Incomplete || len(index.Columns) == 0 {
			return Key{}, false
		}
		key := Key{Name: index.Name}
		seen := make(map[string]bool)
		for _, name := range index.Columns {
			found := false
			for _, column := range table.Columns {
				if strings.EqualFold(column.Name, name) && !column.Nullable && !seen[strings.ToLower(column.Name)] {
					key.Columns = append(key.Columns, column)
					seen[strings.ToLower(column.Name)] = true
					found = true
					break
				}
			}
			if !found {
				return Key{}, false
			}
		}
		return key, true
	}
	if key, ok := resolve(table.PrimaryKey); ok {
		return key, nil
	}
	indexes := slices.Clone(table.UniqueKeys)
	slices.SortFunc(indexes, func(a, b Index) int { return strings.Compare(a.Name, b.Name) })
	for _, index := range indexes {
		if key, ok := resolve(index); ok {
			return key, nil
		}
	}
	return Key{}, fmt.Errorf("%w: %s.%s", ErrNoUsableKey, table.Schema, table.Name)
}

func validateTable(table Table) error {
	if !validIdentifier(table.Schema) || !validIdentifier(table.Name) || len(table.Columns) == 0 {
		return fmt.Errorf("snapshot: invalid table metadata")
	}
	seen := make(map[string]bool)
	for _, c := range table.Columns {
		name := strings.ToLower(c.Name)
		if !validIdentifier(c.Name) || strings.TrimSpace(c.DataType) == "" || seen[name] {
			return fmt.Errorf("snapshot: invalid or duplicate column metadata")
		}
		seen[name] = true
	}
	return nil
}

func validIdentifier(name string) bool {
	return name != "" && !strings.ContainsRune(name, '\x00')
}

func integerBits(dataType string) int {
	switch strings.ToLower(dataType) {
	case "tinyint":
		return 8
	case "smallint":
		return 16
	case "mediumint":
		return 24
	case "int", "integer":
		return 32
	case "bigint":
		return 64
	default:
		return 0
	}
}
