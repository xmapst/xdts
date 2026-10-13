package mysqlout

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

// ErrConflictStopped is a permanent policy decision. It never includes row data.
var ErrConflictStopped = errors.New("mysqlout: conflict STOP policy stopped the transaction")

func (w *Writer) applyStopRow(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange, sink conflictSink) error {
	if err := iproto.ValidateRowChange(rc); err != nil {
		return err
	}
	var target *iproto.Row
	var types []string
	var err error
	if rc.GetType() != iproto.ChangeType_CHANGE_TYPE_INSERT {
		target, types, err = tx.readConflictRow(ctx, rc)
		if err != nil {
			return err
		}
	}
	stop := func(reason string) error {
		if sink != nil {
			sink(&iproto.ConflictEventDetail{Change: rc, TargetRow: target, Policy: iproto.ConflictMode_CONFLICT_MODE_STOP, Resolution: iproto.ConflictResolution_CONFLICT_RESOLUTION_TXN_ROLLED_BACK, Reason: reason})
		}
		return ErrConflictStopped
	}
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE {
		if target == nil {
			return stop("update_missing")
		}
		if !conflictRowsEqual(rc.Before, target, types) {
			return stop("update_mismatch")
		}
	} else if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_DELETE {
		if target == nil {
			return stop("delete_missing")
		}
		if !conflictRowsEqual(rc.Before, target, types) {
			return stop("delete_mismatch")
		}
	}
	var query string
	var args []any
	switch rc.GetType() {
	case iproto.ChangeType_CHANGE_TYPE_INSERT:
		query, args, err = buildStopInsert(rc)
	case iproto.ChangeType_CHANGE_TYPE_UPDATE:
		query, args, err = buildStopUpdate(rc)
	case iproto.ChangeType_CHANGE_TYPE_DELETE:
		query, args, err = buildDeleteSourceOverride(rc.Schema, rc.Table, rc.ColumnNames, rc.Before, rc.PrimaryKeyColumns)
	default:
		return errUnsupportedChangeType
	}
	if err != nil {
		return err
	}
	result, err := tx.execDML(ctx, query, args...)
	if err != nil {
		var duplicate *mysql.MySQLError
		if errors.As(err, &duplicate) && duplicate.Number == 1062 {
			// Query the actual collision when its unique columns are synchronized.
			// Prefix/functional indexes may not expose a complete wire key; the
			// duplicate itself remains authoritative even without a target image.
			previous := target
			target = nil
			found, _, lookupErr := tx.readInsertConflictExcept(ctx, rc, previous)
			if lookupErr == nil {
				target = found
			} else if IsRetryable(lookupErr) || ctx.Err() != nil {
				return lookupErr
			}
			return stop("insert_duplicate")
		}
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	// An unchanged UPDATE may report zero affected rows. Its existence and
	// before image have already been checked under the same transaction lock.
	if rc.GetType() == iproto.ChangeType_CHANGE_TYPE_UPDATE && (rows == 0 || rows == 1) || rc.GetType() != iproto.ChangeType_CHANGE_TYPE_UPDATE && rows == 1 {
		return nil
	}
	return fmt.Errorf("mysqlout: STOP statement affected an unexpected number of rows: %w", ErrConflictStopped)
}

func buildStopUpdate(rc *iproto.RowChange) (string, []any, error) {
	if len(rc.PrimaryKeyColumns) == 0 || len(rc.Before.GetValues()) != len(rc.ColumnNames) || len(rc.After.GetValues()) != len(rc.ColumnNames) {
		return "", nil, fmt.Errorf("mysqlout: invalid STOP UPDATE row width")
	}
	assignments := make([]string, len(rc.ColumnNames))
	args := make([]any, 0, len(rc.ColumnNames)+len(rc.PrimaryKeyColumns))
	for i, column := range rc.ColumnNames {
		value, err := columnValueToArg(rc.After.Values[i])
		if err != nil {
			return "", nil, err
		}
		assignments[i] = quoteIdent(column) + " = ?"
		args = append(args, value)
	}
	conditions := make([]string, len(rc.PrimaryKeyColumns))
	for i, key := range rc.PrimaryKeyColumns {
		index := slices.Index(rc.ColumnNames, key)
		if index < 0 {
			return "", nil, fmt.Errorf("mysqlout: STOP UPDATE positioning column is absent")
		}
		value, err := columnValueToArg(rc.Before.Values[index])
		if err != nil {
			return "", nil, err
		}
		conditions[i] = quoteIdent(key) + " = ?"
		args = append(args, value)
	}
	query := "UPDATE " + quoteIdent(rc.Schema) + "." + quoteIdent(rc.Table) + " SET " + strings.Join(assignments, ", ") + " WHERE " + strings.Join(conditions, " AND ")
	return query, args, nil
}

func buildStopInsert(rc *iproto.RowChange) (string, []any, error) {
	if len(rc.ColumnNames) == 0 || len(rc.After.GetValues()) != len(rc.ColumnNames) {
		return "", nil, fmt.Errorf("mysqlout: invalid STOP INSERT row width")
	}
	columns := make([]string, len(rc.ColumnNames))
	placeholders := make([]string, len(rc.ColumnNames))
	args := make([]any, len(rc.ColumnNames))
	for i, column := range rc.ColumnNames {
		value, err := columnValueToArg(rc.After.Values[i])
		if err != nil {
			return "", nil, err
		}
		columns[i], placeholders[i], args[i] = quoteIdent(column), "?", value
	}
	query := "INSERT INTO " + quoteIdent(rc.Schema) + "." + quoteIdent(rc.Table) + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	return query, args, nil
}
