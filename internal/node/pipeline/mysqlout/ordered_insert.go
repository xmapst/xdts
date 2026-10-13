package mysqlout

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

// Each row remains a separate INSERT, preserving auto-increment and duplicate
// effects in source order. The driver encodes parameters; business values never
// become application-generated SQL. Only this transaction owns its connection.
func (w *Writer) applyOrderedInserts(ctx context.Context, tx *dmlTxn, group []*iproto.RowChange, maxBytes uint64, sink conflictSink) (int, error) {
	if tx.conn == nil {
		return 0, nil
	}
	var queries []string
	var args []driver.NamedValue
	var bytes uint64
	var query string
	for index, change := range group {
		var values []any
		if index == 0 {
			var err error
			query, values, err = buildInsertSourceOverride(change.Schema, change.Table, change.ColumnNames, change.After, change.PrimaryKeyColumns)
			if err != nil {
				return 0, err
			}
			if err := assertLooksLikeDML(query); err != nil {
				return 0, err
			}
		} else {
			first := group[0]
			if change.Type != first.Type || change.Schema != first.Schema || change.Table != first.Table ||
				!slices.Equal(change.ColumnNames, first.ColumnNames) || !slices.Equal(change.PrimaryKeyColumns, first.PrimaryKeyColumns) {
				return 0, nil // No SQL has been sent; retain the original path.
			}
			row := change.After.GetValues()
			if len(row) != len(first.ColumnNames) {
				return 0, fmt.Errorf("mysqlout: INSERT 的 after 列值数量(%d)与 column_names 数量(%d)"+
					"不一致（%s.%s）", len(row), len(first.ColumnNames), change.Schema, change.Table)
			}
			values = make([]any, len(row))
			for column, value := range row {
				arg, err := columnValueToArg(value)
				if err != nil {
					return 0, fmt.Errorf("mysqlout: 列 %s.%s.%s: %w", change.Schema, change.Table, change.ColumnNames[column], err)
				}
				values[column] = arg
			}
		}
		// Bound SQL framing, parameter storage and worst-case escaping before
		// appending. NULL/numeric/temporal fields also receive fixed overhead.
		cost := uint64(len(query)) + 1
		converted := make([]driver.NamedValue, 0, len(values))
		for _, value := range values {
			value, err := driver.DefaultParameterConverter.ConvertValue(value)
			if err != nil {
				return 0, nil // No SQL has been sent; retain the original path.
			}
			switch v := value.(type) {
			case string:
				cost += uint64(len(v))*2 + 64
			case []byte:
				cost += uint64(len(v))*2 + 64
			case nil, int64, uint64, float64, bool, time.Time:
				cost += 96
			default:
				return 0, nil
			}
			converted = append(converted, driver.NamedValue{Ordinal: len(args) + len(converted) + 1, Value: value})
		}
		if cost > maxBytes || bytes > maxBytes-cost {
			break
		}
		bytes += cost
		queries = append(queries, query)
		args = append(args, converted...)
	}
	if len(queries) < 2 {
		return 0, nil
	}
	var affected []int64
	err := tx.conn.Raw(func(raw any) error {
		executor, ok := raw.(driver.ExecerContext)
		if !ok {
			return driver.ErrSkip
		}
		result, err := executor.ExecContext(ctx, strings.Join(queries, ";"), args)
		if err != nil {
			return err
		}
		individual, ok := result.(mysql.Result)
		if !ok {
			return fmt.Errorf("mysqlout: ordered insert individual results unavailable")
		}
		affected = individual.AllRowsAffected()
		return nil
	})
	if errors.Is(err, driver.ErrSkip) {
		// Driver parameter interpolation rejected the packet before transmission.
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(affected) != len(queries) {
		return 0, fmt.Errorf("mysqlout: ordered insert result count differs from sent rows")
	}
	for index, rows := range affected {
		change := group[index]
		if maybeReportConflict(sink, iproto.ChangeType_CHANGE_TYPE_INSERT, rows, change) == conflictUnexpectedRows {
			slog.Warn("unexpected rows affected, conflict undetermined",
				slog.String("schema", change.Schema), slog.String("table", change.Table),
				slog.Any("change_type", change.Type), slog.Int64("rows_affected", rows))
		}
	}
	return len(queries), nil
}
