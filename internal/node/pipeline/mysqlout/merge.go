package mysqlout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type mergeTable struct {
	safe        bool
	orderedSafe bool
	packet      uint64
	autoColumn  string
}

// Catalog checks are scoped to one GTID transaction. Opening the table first
// holds its metadata lock until commit, so concurrent DDL cannot invalidate
// InnoDB/trigger/column guarantees between validation and the merged statement.
func inspectMergeTable(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange) mergeTable {
	catalog, err := tx.targetColumns(ctx, rc.Schema, rc.Table)
	if err != nil {
		return mergeTable{}
	}
	facts, ok := tx.mergeCatalog[rc.Schema+"\x00"+rc.Table]
	if !ok {
		facts = inspectMergeCatalog(ctx, tx, rc)
	}
	if !facts.visible || facts.engine != "InnoDB" || facts.triggers != 0 ||
		(!strings.Contains(facts.mode, "STRICT_TRANS_TABLES") && !strings.Contains(facts.mode, "STRICT_ALL_TABLES")) {
		return mergeTable{}
	}
	columns := make(map[string]bool, len(rc.ColumnNames))
	for _, name := range rc.ColumnNames {
		columns[strings.ToLower(name)] = true
	}
	safe, orderedSafe := true, true
	var autoColumn string
	for name, column := range catalog {
		if !columns[name] || column.generated {
			orderedSafe = false
		}
		auto := strings.Contains(strings.ToLower(column.extra), "auto_increment")
		if auto {
			switch column.dataType {
			case "tinyint", "smallint", "mediumint", "int", "bigint":
				autoColumn = name
			}
		}
		if !columns[name] || auto || column.generated {
			safe = false
		}
	}
	if facts.checks != 0 {
		autoColumn = ""
	}
	return mergeTable{safe: safe && len(catalog) == len(columns), orderedSafe: orderedSafe && len(catalog) == len(columns), packet: min(facts.packet/2, 4<<20), autoColumn: autoColumn}
}

const mergeGrantsSelect = "SHOW GRANTS FOR CURRENT_USER()"
const mergeCatalogSelect = `SELECT
 (SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?),
 (SELECT COUNT(*) FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=?),
 @@SESSION.sql_mode,@@SESSION.max_allowed_packet,
 (SELECT COUNT(*) FROM information_schema.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND CONSTRAINT_TYPE='CHECK')`

type mergeCatalog struct {
	visible          bool
	engine, mode     string
	triggers, checks int
	packet           uint64
}

func readMergeVisibility(rows *sql.Rows) (bool, error) {
	visible := false
	for rows.Next() {
		var statement string
		if err := rows.Scan(&statement); err != nil {
			return false, err
		}
		privileges, rest, found := strings.Cut(statement, " ON ")
		if !found || !strings.HasPrefix(rest, "*.* TO ") {
			continue
		}
		for privilege := range strings.SplitSeq(strings.TrimPrefix(privileges, "GRANT "), ",") {
			if privilege = strings.TrimSpace(privilege); privilege == "ALL PRIVILEGES" || privilege == "TRIGGER" {
				visible = true
			}
		}
	}
	return visible, rows.Err()
}

func readMergeCatalog(rows *sql.Rows, visible bool) (mergeCatalog, error) {
	facts := mergeCatalog{visible: visible}
	if !rows.Next() {
		return mergeCatalog{}, fmt.Errorf("mysqlout: merge catalog row is missing: %w", rows.Err())
	}
	if err := rows.Scan(&facts.engine, &facts.triggers, &facts.mode, &facts.packet, &facts.checks); err != nil {
		return mergeCatalog{}, err
	}
	if rows.Next() {
		return mergeCatalog{}, fmt.Errorf("mysqlout: unexpected merge catalog row")
	}
	return facts, rows.Err()
}

func inspectMergeCatalog(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange) mergeCatalog {
	results, err := tx.tx.QueryContext(ctx, mergeGrantsSelect+"; "+mergeCatalogSelect, rc.Schema, rc.Table, rc.Schema, rc.Table, rc.Schema, rc.Table)
	if err != nil {
		return mergeCatalog{}
	}
	defer func(cleanup func() error) { _ = cleanup() }(results.Close)
	visible, err := readMergeVisibility(results)
	if err != nil || !visible || !results.NextResultSet() {
		return mergeCatalog{}
	}
	facts, err := readMergeCatalog(results, visible)
	if err != nil || results.NextResultSet() || results.Err() != nil || results.Close() != nil {
		return mergeCatalog{}
	}
	return facts
}

// Only explicit positive integers avoid allocation/reservation of implicit
// auto-increment values. A plain INSERT either inserts every independent row
// without conflicts, or its atomic duplicate failure returns to per-row ODKU.
func (t mergeTable) explicitAutoValues(group []*iproto.RowChange) bool {
	if !t.orderedSafe || t.autoColumn == "" || len(group) == 0 {
		return false
	}
	column := -1
	for index, name := range group[0].ColumnNames {
		if strings.EqualFold(name, t.autoColumn) {
			column = index
			break
		}
	}
	if column < 0 {
		return false
	}
	for _, change := range group {
		values := change.GetAfter().GetValues()
		if len(values) != len(change.GetColumnNames()) || column >= len(values) {
			return false
		}
		switch value := values[column].GetValue().(type) {
		case *iproto.ColumnValue_U64:
			if value.U64 == 0 {
				return false
			}
		case *iproto.ColumnValue_I64:
			if value.I64 <= 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func targetMergeKeysMatch(ctx context.Context, tx *dmlTxn, rc *iproto.RowChange) bool {
	rows, err := tx.tx.QueryContext(ctx, "SELECT INDEX_NAME,COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 ORDER BY INDEX_NAME,SEQ_IN_INDEX", rc.Schema, rc.Table)
	if err != nil {
		return false
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	var keys []string
	for rows.Next() {
		var index, name string
		if rows.Scan(&index, &name) != nil || index != "PRIMARY" {
			return false
		}
		keys = append(keys, strings.ToLower(name))
	}
	if rows.Err() != nil || len(keys) != len(rc.PrimaryKeyColumns) || len(keys) == 0 {
		return false
	}
	for i, key := range keys {
		if key != strings.ToLower(rc.PrimaryKeyColumns[i]) {
			return false
		}
	}
	return true
}

// A zero count requests the original per-row path. Optimization is conditional
// on proving the same effects and diagnostics, never on aggregate RowsAffected.
func (w *Writer) mergeRows(ctx context.Context, tx *dmlTxn, changes []*iproto.RowChange, cache map[string]mergeTable, sink conflictSink) (int, error) {
	if !w.opts.WritePolicy.GetMergeSameTableRows() || len(changes) < 2 || w.opts.foreignKeyChecksValue() != 0 {
		return 0, nil
	}
	mode := w.opts.ConflictPolicy.GetMode()
	if mode != iproto.ConflictMode_CONFLICT_MODE_UNSPECIFIED && mode != iproto.ConflictMode_CONFLICT_MODE_SOURCE_WINS {
		return 0, nil
	}
	first := changes[0]
	if len(first.ColumnNames) == 0 {
		return 0, nil
	}
	limit := min(len(changes), iproto.WriteBatchSize(w.opts.WritePolicy), 65535/len(first.ColumnNames))
	count := 1
	for count < limit {
		next := changes[count]
		if next.Type != first.Type || next.Schema != first.Schema || next.Table != first.Table || !slices.Equal(next.ColumnNames, first.ColumnNames) || !slices.Equal(next.PrimaryKeyColumns, first.PrimaryKeyColumns) {
			break
		}
		count++
	}
	if count < 2 {
		return 0, nil
	}
	key := first.Schema + "\x00" + first.Table
	safety, ok := cache[key]
	if !ok {
		safety = inspectMergeTable(ctx, tx, first)
		cache[key] = safety
	}
	if !safety.safe && (!safety.orderedSafe || first.Type != iproto.ChangeType_CHANGE_TYPE_INSERT) {
		return 0, nil
	}
	var size uint64
	for i := 0; i < count; i++ {
		cost := uint64(proto.Size(changes[i]))*2 + 256
		if cost > safety.packet-size {
			count = i
			break
		}
		size += cost
	}
	if count < 2 {
		return 0, nil
	}
	group := changes[:count]
	if first.Type == iproto.ChangeType_CHANGE_TYPE_INSERT {
		if !safety.safe && !safety.explicitAutoValues(group) {
			return w.applyOrderedInserts(ctx, tx, group, safety.packet, sink)
		}
		err := tx.execMergedInserts(ctx, group)
		var sqlErr *mysql.MySQLError
		if errors.As(err, &sqlErr) && sqlErr.Number == 1062 {
			// InnoDB rolls back the rows atomically. With explicit auto values,
			// per-row retry also preserves the counter reached before this error;
			// do not discard the duplicate failure or commit an empty GTID.
			return 0, nil
		}
		return count, err
	}
	if first.Type != iproto.ChangeType_CHANGE_TYPE_UPDATE && first.Type != iproto.ChangeType_CHANGE_TYPE_DELETE {
		return 0, nil
	}
	if !targetMergeKeysMatch(ctx, tx, first) {
		return 0, nil
	}
	seen := make(map[string]bool, len(group))
	var diagnostics []*iproto.ConflictEventDetail
	for _, change := range group {
		if change.Type == iproto.ChangeType_CHANGE_TYPE_UPDATE {
			changed, err := primaryKeyChanged(change.ColumnNames, change.PrimaryKeyColumns, change.Before, change.After)
			if err != nil {
				return 0, err
			}
			if changed {
				return 0, nil
			}
		}
		target, types, err := tx.readConflictRow(ctx, change)
		if err != nil {
			return 0, err
		}
		if target == nil {
			return 0, nil
		}
		position := &iproto.Row{}
		for _, column := range change.PrimaryKeyColumns {
			for index, name := range change.ColumnNames {
				if strings.EqualFold(column, name) {
					position.Values = append(position.Values, target.Values[index])
					break
				}
			}
		}
		encoded, err := proto.Marshal(position)
		if err != nil {
			return 0, err
		}
		if seen[string(encoded)] {
			return 0, nil
		}
		seen[string(encoded)] = true
		if !conflictRowsEqual(change.Before, target, types) {
			detail := buildConflictDetail(change)
			detail.TargetRow = target
			detail.Reason = "update_mismatch"
			if change.Type == iproto.ChangeType_CHANGE_TYPE_DELETE {
				detail.Reason = "delete_mismatch"
			}
			diagnostics = append(diagnostics, detail)
		}
	}
	var query string
	var args []any
	var err error
	if first.Type == iproto.ChangeType_CHANGE_TYPE_UPDATE {
		query, args, err = buildMergedValues(group)
		keys := make(map[string]bool, len(first.PrimaryKeyColumns))
		for _, name := range first.PrimaryKeyColumns {
			keys[strings.ToLower(name)] = true
		}
		var assignments []string
		for _, name := range first.ColumnNames {
			if !keys[strings.ToLower(name)] {
				q := quoteIdent(name)
				assignments = append(assignments, q+"=VALUES("+q+")")
			}
		}
		if len(assignments) == 0 {
			q := quoteIdent(first.ColumnNames[0])
			assignments = []string{q + "=" + q}
		}
		query += " ON DUPLICATE KEY UPDATE " + strings.Join(assignments, ",")
	} else {
		var tuples []string
		for _, change := range group {
			_, values, e := buildDeleteSourceOverride(change.Schema, change.Table, change.ColumnNames, change.Before, change.PrimaryKeyColumns)
			if e != nil {
				return 0, e
			}
			args = append(args, values...)
			tuples = append(tuples, "("+strings.TrimSuffix(strings.Repeat("?,", len(values)), ",")+")")
		}
		keys := make([]string, len(first.PrimaryKeyColumns))
		for i, name := range first.PrimaryKeyColumns {
			keys[i] = quoteIdent(name)
		}
		query = "DELETE FROM " + quoteIdent(first.Schema) + "." + quoteIdent(first.Table) + " WHERE (" + strings.Join(keys, ",") + ") IN (" + strings.Join(tuples, ",") + ")"
	}
	if err != nil {
		return 0, err
	}
	if _, err = tx.execDML(ctx, query, args...); err != nil {
		return count, err
	}
	if sink != nil {
		for _, detail := range diagnostics {
			sink(detail)
		}
	}
	return count, nil
}

func buildMergedValues(group []*iproto.RowChange) (string, []any, error) {
	rows := make([]*iproto.Row, len(group))
	for i, change := range group {
		rows[i] = change.After
	}
	first := group[0]
	query, args, err := buildReplace(first.Schema, first.Table, first.ColumnNames, rows)
	return strings.Replace(query, "REPLACE INTO ", "INSERT INTO ", 1), args, err
}

// execMergedInserts accepts only rows already admitted by mergeRows. Its SQL
// comes from the same package constructor; source GTIDs and rollback ownership
// remain in dmlTxn. The driver encodes parameters using the existing session.
func (t *dmlTxn) execMergedInserts(ctx context.Context, group []*iproto.RowChange) error {
	query, args, err := buildMergedValues(group)
	if err != nil {
		return err
	}
	if err := assertLooksLikeDML(query); err != nil {
		return err
	}
	_, err = t.tx.ExecContext(ctx, query, args...)
	return err
}
