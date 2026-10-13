package mysqlout

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var errLWWTransactionRejected = errors.New("mysqlout: all transaction rows lost LWW comparison")

func (t *dmlTxn) validateLWWTimestamp(ctx context.Context, rc *iproto.RowChange, column string) (int, error) {
	index := -1
	for i, name := range rc.GetColumnNames() {
		if strings.EqualFold(name, column) {
			index = i
			break
		}
	}
	if index < 0 {
		return 0, fmt.Errorf("mysqlout: LWW timestamp column is absent from synchronized row")
	}
	var dataType string
	var precision sql.NullInt64
	if err := t.tx.QueryRowContext(ctx, `SELECT DATA_TYPE,DATETIME_PRECISION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND COLUMN_NAME=?`, rc.GetSchema(), rc.GetTable(), column).Scan(&dataType, &precision); err != nil {
		return 0, fmt.Errorf("mysqlout: LWW timestamp metadata unavailable: %w", err)
	}
	switch strings.ToLower(dataType) {
	case "datetime", "timestamp":
		if !precision.Valid || precision.Int64 < 3 {
			return 0, fmt.Errorf("mysqlout: LWW temporal column requires millisecond precision")
		}
	case "bigint":
	default:
		return 0, fmt.Errorf("mysqlout: LWW timestamp must be BIGINT milliseconds or DATETIME/TIMESTAMP(3..6)")
	}
	return index, nil
}

func compareLWWTimestamp(a, b *iproto.ColumnValue) (int, error) {
	if a == nil || b == nil || a.GetZeroDate() || b.GetZeroDate() {
		return 0, fmt.Errorf("mysqlout: LWW timestamp is missing or zero date")
	}
	numeric := func(v *iproto.ColumnValue) (*big.Int, string, error) {
		switch x := v.GetValue().(type) {
		case *iproto.ColumnValue_I64:
			return big.NewInt(x.I64), "integer", nil
		case *iproto.ColumnValue_U64:
			return new(big.Int).SetUint64(x.U64), "integer", nil
		case *iproto.ColumnValue_TemporalUsec:
			return big.NewInt(x.TemporalUsec), "temporal", nil
		default:
			return nil, "", fmt.Errorf("mysqlout: invalid LWW timestamp value")
		}
	}
	av, at, err := numeric(a)
	if err != nil {
		return 0, err
	}
	bv, bt, err := numeric(b)
	if err != nil {
		return 0, err
	}
	if at != bt {
		return 0, fmt.Errorf("mysqlout: LWW timestamp types disagree")
	}
	return av.Cmp(bv), nil
}

// canonicalLWWRow does not depend on source column order. SQL presentation
// differences are normalized before SHA-256 so equal timestamps converge.
func canonicalLWWRow(columns []string, row *iproto.Row, types []string) ([]byte, error) {
	if row == nil || len(row.GetValues()) != len(columns) {
		return nil, fmt.Errorf("mysqlout: invalid LWW row width")
	}
	values := make(map[string]any, len(columns))
	for i, name := range columns {
		cv := row.Values[i]
		if cv == nil || cv.GetValue() == nil {
			return nil, fmt.Errorf("mysqlout: missing LWW row value")
		}
		normalized := proto.Clone(cv).(*iproto.ColumnValue)
		if i < len(types) {
			var err error
			normalized, err = projectTargetValue(normalized, types[i])
			if err != nil {
				return nil, err
			}
			switch types[i] {
			case "float":
				if v, ok := normalized.Value.(*iproto.ColumnValue_F64); ok {
					v.F64 = float64(float32(v.F64))
				}
			case "json":
				if v, ok := normalized.Value.(*iproto.ColumnValue_Text); ok {
					d := json.NewDecoder(strings.NewReader(v.Text))
					d.UseNumber()
					var parsed any
					if err := d.Decode(&parsed); err != nil {
						return nil, fmt.Errorf("mysqlout: invalid LWW JSON value")
					}
					raw, err := json.Marshal(canonicalLWWJSON(parsed))
					if err != nil {
						return nil, err
					}
					v.Text = string(raw)
				}
			}
		}
		if v, ok := normalized.Value.(*iproto.ColumnValue_Decimal); ok {
			r, valid := new(big.Rat).SetString(v.Decimal)
			if !valid {
				return nil, fmt.Errorf("mysqlout: invalid LWW decimal value")
			}
			v.Decimal = r.RatString()
		}
		raw, err := protojson.Marshal(normalized)
		if err != nil {
			return nil, err
		}
		var value any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		values[name] = value
	}
	return json.Marshal(values)
}

// readInsertConflict queries the target's complete unique key metadata. Wire
// unique_keys is not authoritative and may be empty for a known target index.
func (t *dmlTxn) readInsertConflict(ctx context.Context, rc *iproto.RowChange) (*iproto.Row, []string, error) {
	return t.readInsertConflictExcept(ctx, rc, nil)
}

// A native UPDATE may still match its own positioning key after its duplicate
// statement is rolled back. That row is not the conflicting unique-key row.
func (t *dmlTxn) readInsertConflictExcept(ctx context.Context, rc *iproto.RowChange, exclude *iproto.Row) (*iproto.Row, []string, error) {
	indexes, err := t.tx.QueryContext(ctx, `SELECT INDEX_NAME,COLUMN_NAME,SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX`, rc.GetSchema(), rc.GetTable())
	if err != nil {
		return nil, nil, err
	}
	keys := map[string][]string{}
	unsupported := map[string]bool{}
	for indexes.Next() {
		var name string
		var col sql.NullString
		var prefix sql.NullInt64
		if err := indexes.Scan(&name, &col, &prefix); err != nil {
			_ = indexes.Close()
			return nil, nil, err
		}
		if !col.Valid || prefix.Valid {
			unsupported[name] = true
		}
		keys[name] = append(keys[name], col.String)
	}
	err = indexes.Err()
	_ = indexes.Close()
	if err != nil {
		return nil, nil, err
	}
	names := make([]string, 0, len(keys))
	for name := range keys {
		names = append(names, name)
	}
	slices.Sort(names)
	var target *iproto.Row
	var types []string
	for _, key := range names {
		if unsupported[key] {
			return nil, nil, fmt.Errorf("mysqlout: LWW requires synchronized complete unique indexes")
		}
		probe := proto.Clone(rc).(*iproto.RowChange)
		probe.Before = rc.GetAfter()
		probe.PrimaryKeyColumns = keys[key]
		// Nullable unique keys containing NULL cannot collide in MySQL.
		nullable := false
		for _, col := range keys[key] {
			index := slices.Index(rc.ColumnNames, col)
			if index < 0 {
				return nil, nil, fmt.Errorf("mysqlout: LWW unique key column is not synchronized")
			}
			if rc.After.Values[index].GetIsNull() {
				nullable = true
				break
			}
		}
		if nullable {
			continue
		}
		found, foundTypes, err := t.readConflictRow(ctx, probe)
		if err != nil {
			return nil, nil, err
		}
		if found == nil {
			continue
		}
		if exclude != nil && conflictRowsEqual(exclude, found, foundTypes) {
			continue
		}
		if target != nil && !conflictRowsEqual(target, found, foundTypes) {
			return nil, nil, fmt.Errorf("mysqlout: LWW insert collides with multiple target rows")
		}
		target, types = found, foundTypes
	}
	return target, types, nil
}

func (w *Writer) applyLWWRow(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange, column string, sink conflictSink) (bool, error) {
	index, err := tx.validateLWWTimestamp(ctx, rc, column)
	if err != nil {
		return false, err
	}
	if len(rc.GetColumnNames()) == 0 || len(rc.GetPrimaryKeyColumns()) == 0 {
		return false, fmt.Errorf("mysqlout: LWW requires complete row and positioning key")
	}
	incoming := rc.GetAfter()
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE {
		incoming = rc.GetBefore()
	}
	if incoming == nil || len(incoming.Values) != len(rc.ColumnNames) {
		return false, fmt.Errorf("mysqlout: invalid LWW incoming row")
	}
	// Validate incoming timestamps even when the row does not exist yet.
	if _, err := compareLWWTimestamp(incoming.Values[index], incoming.Values[index]); err != nil {
		return false, err
	}
	var target *iproto.Row
	var types []string
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_INSERT {
		target, types, err = tx.readInsertConflict(ctx, rc)
	} else {
		target, types, err = tx.readConflictRow(ctx, rc)
	}
	if err != nil {
		return false, err
	}
	wins := true
	if target != nil {
		cmp, err := compareLWWTimestamp(incoming.Values[index], target.Values[index])
		if err != nil {
			return false, err
		}
		if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE {
			wins = cmp >= 0
		} else if cmp != 0 {
			wins = cmp > 0
		} else {
			left, err := canonicalLWWRow(rc.ColumnNames, incoming, types)
			if err != nil {
				return false, err
			}
			right, err := canonicalLWWRow(rc.ColumnNames, target, types)
			if err != nil {
				return false, err
			}
			lh, rh := sha256.Sum256(left), sha256.Sum256(right)
			wins = bytes.Compare(lh[:], rh[:]) > 0
		}
	}
	if !wins {
		if sink != nil {
			sink(&iproto.ConflictEventDetail{Change: rc, TargetRow: target, Policy: iproto.ConflictMode_CONFLICT_MODE_LWW, Resolution: iproto.ConflictResolution_CONFLICT_RESOLUTION_DISCARDED, Reason: "lww_rejected"})
		}
		return false, nil
	}
	// UPDATE may collide through any unique key, including a changed PK.
	// Lock and compare that actual destination before modifying the old row.
	var destination *iproto.Row
	if rc.GetType() != iproto.ChangeType_CHANGE_TYPE_DELETE {
		destination, types, err = tx.readInsertConflict(ctx, rc)
		if err != nil {
			return false, err
		}
		if destination != nil && !conflictRowsEqual(destination, target, types) {
			cmp, err := compareLWWTimestamp(incoming.Values[index], destination.Values[index])
			if err != nil {
				return false, err
			}
			left, err := canonicalLWWRow(rc.ColumnNames, incoming, types)
			if err != nil {
				return false, err
			}
			right, err := canonicalLWWRow(rc.ColumnNames, destination, types)
			if err != nil {
				return false, err
			}
			lh, rh := sha256.Sum256(left), sha256.Sum256(right)
			if cmp < 0 || cmp == 0 && bytes.Compare(lh[:], rh[:]) <= 0 {
				if sink != nil {
					sink(&iproto.ConflictEventDetail{Change: rc, TargetRow: destination, Policy: iproto.ConflictMode_CONFLICT_MODE_LWW, Resolution: iproto.ConflictResolution_CONFLICT_RESOLUTION_DISCARDED, Reason: "lww_rejected"})
				}
				return false, nil
			}
		}
		if destination != nil {
			changed, err := primaryKeyChanged(rc.ColumnNames, rc.PrimaryKeyColumns, destination, incoming)
			if err != nil {
				return false, err
			}
			if changed {
				if sink != nil {
					sink(&iproto.ConflictEventDetail{Change: rc, TargetRow: destination, Policy: iproto.ConflictMode_CONFLICT_MODE_LWW, Resolution: iproto.ConflictResolution_CONFLICT_RESOLUTION_APPLIED, Reason: "insert_duplicate"})
				}
				if w.opts.foreignKeyChecksValue() == 1 {
					return false, errForeignKeyChecksUnsafeForPrimaryKeyChange
				}
				query, args, err := buildDeleteSourceOverride(rc.Schema, rc.Table, rc.ColumnNames, destination, rc.PrimaryKeyColumns)
				if err != nil {
					return false, err
				}
				if _, err := tx.execDML(ctx, query, args...); err != nil {
					return false, err
				}
			}
		}
	}
	report := func(d *iproto.ConflictEventDetail) {
		d.Policy = iproto.ConflictMode_CONFLICT_MODE_LWW
		if d.TargetRow == nil {
			d.TargetRow = target
		}
		if sink != nil {
			sink(d)
		}
	}
	if err := applyRowChange(ctx, tx, rc, w.opts.foreignKeyChecksValue() == 1, report); err != nil {
		return false, err
	}
	return true, nil
}

// Tags distinguish arbitrary JSON objects from normalized numeric values.
func canonicalLWWJSON(v any) any {
	switch x := v.(type) {
	case nil:
		return []any{"null"}
	case bool:
		return []any{"boolean", x}
	case string:
		return []any{"string", x}
	case json.Number:
		if n, ok := new(big.Rat).SetString(string(x)); ok {
			return []any{"number", n.RatString()}
		}
		return []any{"number", string(x)}
	case []any:
		values := make([]any, len(x))
		for i, value := range x {
			values[i] = canonicalLWWJSON(value)
		}
		return []any{"array", values}
	case map[string]any:
		values := make(map[string]any, len(x))
		for k, value := range x {
			values[k] = canonicalLWWJSON(value)
		}
		return []any{"object", values}
	}
	return v
}
