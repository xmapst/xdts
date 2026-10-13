package snapshot

import (
	"fmt"
	"strings"
)

// Select builds the short read for a committed chunk. Values are bound, and
// identifiers are quoted. Callers must validate projection columns against the
// selected table metadata before reading, and use a read-only connection.
func (c Chunk) Select(columns []string) (string, []any, error) {
	if !validIdentifier(c.Schema) || !validIdentifier(c.Table) || len(columns) == 0 || len(c.Key.Columns) == 0 {
		return "", nil, fmt.Errorf("snapshot: invalid chunk query metadata")
	}
	projection := make([]string, len(columns))
	for i, column := range columns {
		if !validIdentifier(column) {
			return "", nil, fmt.Errorf("snapshot: invalid projection column")
		}
		projection[i] = quoteIdentifier(column)
	}
	for _, column := range c.Key.Columns {
		if !validIdentifier(column.Name) || column.Nullable {
			return "", nil, fmt.Errorf("snapshot: invalid positioning column")
		}
	}
	lowerOp, upperOp := ">", "<="
	switch c.Strategy {
	case StrategyInteger:
		if len(c.Key.Columns) != 1 || integerBits(c.Key.Columns[0].DataType) == 0 {
			return "", nil, fmt.Errorf("snapshot: invalid integer chunk key")
		}
		lowerOp, upperOp = ">=", "<"
	case StrategyKeyset:
	default:
		return "", nil, fmt.Errorf("snapshot: invalid chunk strategy")
	}
	expressions := keyExpressions(c.Key)
	var conditions []string
	var args []any
	for _, bound := range []struct {
		value Boundary
		op    string
	}{{c.Lower, lowerOp}, {c.Upper, upperOp}} {
		if len(bound.value.Values) == 0 {
			continue
		}
		if !validBoundary(bound.value, c.Key) {
			return "", nil, fmt.Errorf("snapshot: invalid chunk boundary")
		}
		conditions = append(conditions, tuple(expressions)+" "+bound.op+" "+placeholders(len(expressions)))
		for _, value := range bound.value.Values {
			args = append(args, value)
		}
	}
	query := "SELECT " + strings.Join(projection, ", ") + " FROM " + qualifiedTable(Table{Schema: c.Schema, Name: c.Table})
	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}
	query += " ORDER BY " + strings.Join(expressions, ", ")
	return query, args, nil
}
