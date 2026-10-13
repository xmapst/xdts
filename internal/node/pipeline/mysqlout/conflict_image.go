package mysqlout

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/snapshot"
	"google.golang.org/protobuf/proto"
)

// readConflictRow locks the positioning key through the actual target index.
// Values use the same text protocol decoder as snapshot reads, preserving NULL,
// binary data, unsigned integers, decimals and temporal precision.
func (t *dmlTxn) readConflictRow(ctx context.Context, rc *iproto.RowChange) (*iproto.Row, []string, error) {
	query, args, err := buildDeleteSourceOverride(rc.Schema, rc.Table, rc.ColumnNames, rc.Before, rc.PrimaryKeyColumns)
	if err != nil {
		return nil, nil, err
	}
	where := strings.TrimPrefix(query, "DELETE FROM ")
	cols := make([]string, len(rc.ColumnNames))
	for i, name := range rc.ColumnNames {
		cols[i] = quoteIdent(name)
	}
	query = "SELECT " + strings.Join(cols, ",") + " FROM " + where + " FOR UPDATE"
	statement := t.statements[query]
	if statement == nil && len(t.statements) < 32 {
		statement, err = t.tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, nil, fmt.Errorf("mysqlout: prepare conflict target row failed: %w", err)
		}
		if t.statements == nil {
			t.statements = make(map[string]*sql.Stmt)
		}
		// Share the transaction's bounded shape budget with DML. Cache only
		// the prepared statement: each call reads and locks the current row,
		// and obtains fresh column metadata. sql.Tx owns statement cleanup.
		t.statements[query] = statement
	}
	var rows *sql.Rows
	if statement != nil {
		rows, err = statement.QueryContext(ctx, args...)
	} else {
		rows, err = t.tx.QueryContext(ctx, query, args...)
	}
	if err != nil {
		return nil, nil, fmt.Errorf("mysqlout: read conflict target row failed: %w", err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}
	if !rows.Next() {
		return nil, nil, rows.Err()
	}
	raw := make([]sql.RawBytes, len(cols))
	dest := make([]any, len(cols))
	for i := range raw {
		dest[i] = &raw[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, nil, err
	}
	typeNames := make([]string, len(cols))
	row := &iproto.Row{Values: make([]*iproto.ColumnValue, len(cols))}
	for i, typ := range types {
		dataType := strings.ToLower(typ.DatabaseTypeName())
		unsigned := strings.HasPrefix(dataType, "unsigned ")
		dataType = strings.TrimPrefix(dataType, "unsigned ")
		if dataType == "integer" {
			dataType = "int"
		}
		typeNames[i] = dataType
		value, e := snapshot.DecodeSQLValue(raw[i], snapshot.Column{Name: rc.ColumnNames[i], DataType: dataType, Unsigned: unsigned})
		if e != nil {
			return nil, nil, fmt.Errorf("mysqlout: decode conflict target column failed: %w", e)
		}
		row.Values[i] = value
	}
	if rows.Next() {
		return nil, nil, fmt.Errorf("mysqlout: conflict positioning key matched multiple rows")
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return row, typeNames, nil
}

func conflictRowsEqual(left, right *iproto.Row, types []string) bool {
	if left == nil || right == nil {
		return left == right
	}
	if len(left.Values) != len(right.Values) {
		return false
	}
	for i, a := range left.Values {
		b := right.Values[i]
		if i < len(types) {
			var err error
			a, err = projectTargetValue(a, types[i])
			if err != nil {
				return false
			}
			b, err = projectTargetValue(b, types[i])
			if err != nil {
				return false
			}
		}
		if proto.Equal(a, b) {
			continue
		}
		// Binlog BLOB-family values include TEXT as bytes; text protocol uses
		// the actual column charset to distinguish TEXT from binary BLOB.
		// Normalize only proven text columns, preserving content and NULL.
		if i < len(types) {
			switch types[i] {
			case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
				av, aok := conflictTextValue(a)
				bv, bok := conflictTextValue(b)
				if aok && bok && av == bv {
					continue
				}
			}
		}
		// MySQL FLOAT is stored with 32-bit precision, while both wire values
		// use f64. Text protocol formatting must not create a conflict.
		if i < len(types) && types[i] == "float" {
			av, aok := a.GetValue().(*iproto.ColumnValue_F64)
			bv, bok := b.GetValue().(*iproto.ColumnValue_F64)
			if aok && bok && float32(av.F64) == float32(bv.F64) {
				continue
			}
		}
		if i < len(types) && types[i] == "json" {
			av, aok := a.GetValue().(*iproto.ColumnValue_Text)
			bv, bok := b.GetValue().(*iproto.ColumnValue_Text)
			if aok && bok && equalConflictJSON(av.Text, bv.Text) {
				continue
			}
		}
		// DECIMAL scale is a presentation difference.
		if a.GetValue() == nil || b.GetValue() == nil {
			return false
		}
		if av, ok := a.GetValue().(*iproto.ColumnValue_Decimal); ok {
			bv, ok := b.GetValue().(*iproto.ColumnValue_Decimal)
			if !ok {
				return false
			}
			ar, ak := new(big.Rat).SetString(av.Decimal)
			br, bk := new(big.Rat).SetString(bv.Decimal)
			if ak && bk && ar.Cmp(br) == 0 {
				continue
			}
		}
		return false
	}
	return true
}

func conflictTextValue(value *iproto.ColumnValue) (string, bool) {
	if value.GetZeroDate() {
		return "", false
	}
	switch v := value.GetValue().(type) {
	case *iproto.ColumnValue_Text:
		return v.Text, utf8.ValidString(v.Text)
	case *iproto.ColumnValue_RawBytes:
		return string(v.RawBytes), utf8.Valid(v.RawBytes)
	default:
		return "", false
	}
}

// mismatches are detected under the same row lock as the subsequent write.
func (t *dmlTxn) inspectBeforeImage(ctx context.Context, rc *iproto.RowChange, sink conflictSink) (conflictSink, error) {
	target, types, err := t.readConflictRow(ctx, rc)
	if err != nil {
		return nil, err
	}
	reason := ""
	if target != nil && !conflictRowsEqual(rc.Before, target, types) {
		if rc.Type == iproto.ChangeType_CHANGE_TYPE_UPDATE {
			reason = "update_mismatch"
		} else {
			reason = "delete_mismatch"
		}
		if sink != nil {
			detail := buildConflictDetail(rc)
			detail.Reason = reason
			detail.TargetRow = target
			sink(detail)
		}
	}
	// Keep rows-affected detection for missing rows and changed-key collisions.
	// Existing-row mismatch is already represented by a single diagnostic.
	return func(detail *iproto.ConflictEventDetail) {
		if reason != "" || sink == nil {
			return
		}
		detail.TargetRow = target
		sink(detail)
	}, nil
}

// Numeric JSON values retain arbitrary precision instead of passing through f64.
func equalConflictJSON(a, b string) bool {
	decode := func(raw string) (any, error) {
		d := json.NewDecoder(strings.NewReader(raw))
		d.UseNumber()
		var v any
		if err := d.Decode(&v); err != nil {
			return nil, err
		}
		return normalizeConflictJSON(v), nil
	}
	av, ae := decode(a)
	bv, be := decode(b)
	return ae == nil && be == nil && reflect.DeepEqual(av, bv)
}
func normalizeConflictJSON(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, ok := new(big.Rat).SetString(string(x)); ok {
			return struct{ Number string }{n.RatString()}
		}
	case map[string]any:
		for k, value := range x {
			x[k] = normalizeConflictJSON(value)
		}
	case []any:
		for i, value := range x {
			x[i] = normalizeConflictJSON(value)
		}
	}
	return v
}
