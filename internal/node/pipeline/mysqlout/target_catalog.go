package mysqlout

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
)

// A catalog result belongs to this original GTID transaction. Physical probes
// precede each catalog read and keep MDL until this transaction ends.
func (tx *dmlTxn) targetColumns(ctx context.Context, schema, table string) (map[string]targetColumn, error) {
	key := schema + "\x00" + table
	if columns, ok := tx.columns[key]; ok {
		return columns, nil
	}
	if err := tx.loadTargetColumns(ctx, [][2]string{{schema, table}}); err != nil {
		return nil, err
	}
	return tx.columns[key], nil
}

// Prefetch preserves first-use table order and the existing before-DML checks.
// Batches bound query construction; neither results nor MDL cross a GTID.
func (tx *dmlTxn) prefetchTargetColumns(ctx context.Context, changes []*iproto.RowChange) error {
	return tx.prefetchColumns(ctx, changes, false)
}

func (tx *dmlTxn) prefetchMergeCatalog(ctx context.Context, changes []*iproto.RowChange) error {
	return tx.prefetchColumns(ctx, changes, true)
}

// Only adjacent equal shapes can enter mergeRows. Selection is local to this
// original GTID and does not replace any mandatory column or physical probe.
func mergeCandidateTables(changes []*iproto.RowChange, batchSize int) map[string]bool {
	selected := make(map[string]bool)
	if batchSize < 2 {
		return selected
	}
	for i := 1; i < len(changes); i++ {
		previous, current := changes[i-1], changes[i]
		if previous == nil || current == nil || len(current.ColumnNames) == 0 || current.Type != previous.Type ||
			current.Schema != previous.Schema || current.Table != previous.Table ||
			!slices.Equal(current.ColumnNames, previous.ColumnNames) || !slices.Equal(current.PrimaryKeyColumns, previous.PrimaryKeyColumns) {
			continue
		}
		switch current.Type {
		case iproto.ChangeType_CHANGE_TYPE_INSERT, iproto.ChangeType_CHANGE_TYPE_UPDATE, iproto.ChangeType_CHANGE_TYPE_DELETE:
			selected[current.Schema+"\x00"+current.Table] = true
		}
	}
	return selected
}

func (tx *dmlTxn) prefetchReplayCatalog(ctx context.Context, changes []*iproto.RowChange, batchSize int) error {
	return tx.prefetchSelectedColumns(ctx, changes, true, mergeCandidateTables(changes, batchSize))
}

func (tx *dmlTxn) prefetchColumns(ctx context.Context, changes []*iproto.RowChange, merge bool) error {
	return tx.prefetchSelectedColumns(ctx, changes, merge, nil)
}

func (tx *dmlTxn) prefetchSelectedColumns(ctx context.Context, changes []*iproto.RowChange, merge bool, selected map[string]bool) error {
	const maxTables = 16
	const maxSQLBytes = 1 << 20
	seen := make(map[string]bool)
	var batch [][2]string
	bytes := 0
	for _, row := range changes {
		key := row.Schema + "\x00" + row.Table
		if _, ok := tx.columns[key]; ok {
			continue
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		cost := 4*(len(row.Schema)+len(row.Table)) + len(targetCatalogSelect) + 128
		if merge {
			cost += len(mergeCatalogSelect) + 12*(len(row.Schema)+len(row.Table)) + len(mergeGrantsSelect)
		}
		if cost > maxSQLBytes {
			return fmt.Errorf("mysqlout: target catalog query exceeds bound")
		}
		if len(batch) == maxTables || bytes+cost > maxSQLBytes {
			if err := tx.loadSelectedCatalogBatch(ctx, batch, merge, selected); err != nil {
				return err
			}
			batch, bytes = nil, 0
		}
		batch = append(batch, [2]string{row.Schema, row.Table})
		bytes += cost
	}
	if len(batch) > 0 {
		return tx.loadSelectedCatalogBatch(ctx, batch, merge, selected)
	}
	return nil
}

const targetCatalogSelect = "SELECT COLUMN_NAME,DATA_TYPE,GENERATION_EXPRESSION,EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?"

func (tx *dmlTxn) loadSelectedCatalogBatch(ctx context.Context, tables [][2]string, merge bool, selected map[string]bool) error {
	if !merge {
		return tx.loadTargetColumns(ctx, tables)
	}
	hadLineage := tx.pendingLineage != nil
	if err := tx.loadSelectedColumnsAndMerge(ctx, tables, true, selected); err != nil {
		if hadLineage && tx.pendingLineage != nil {
			return err
		}
		// Merge inspection is optional. Restore the original mandatory catalog
		// path on older servers or a denied optional metadata statement.
		return tx.loadTargetColumns(ctx, tables)
	}
	return nil
}

func (tx *dmlTxn) loadTargetColumns(ctx context.Context, tables [][2]string) error {
	return tx.loadColumnsAndMerge(ctx, tables, false)
}

func (tx *dmlTxn) loadColumnsAndMerge(ctx context.Context, tables [][2]string, merge bool) error {
	return tx.loadSelectedColumnsAndMerge(ctx, tables, merge, nil)
}

func (tx *dmlTxn) loadSelectedColumnsAndMerge(ctx context.Context, tables [][2]string, merge bool, selected map[string]bool) error {
	var mergeTables [][2]string
	if merge {
		for _, table := range tables {
			if selected == nil || selected[table[0]+"\x00"+table[1]] {
				mergeTables = append(mergeTables, table)
			}
		}
	}
	merge = len(mergeTables) != 0
	var query strings.Builder
	args := make([]any, 0, len(tables)*8+2)
	lineage := tx.pendingLineage
	if lineage != nil {
		query.WriteString(dblineage.Insert + "; ")
		args = append(args, lineage.gtid, lineage.body)
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, dblineage.Timeout)
		defer cancel()
	}
	for index, table := range tables {
		if index > 0 {
			query.WriteString("; ")
		}
		query.WriteString("SELECT 1 FROM " + quoteIdent(table[0]) + "." + quoteIdent(table[1]) + " LIMIT 0; " + targetCatalogSelect)
		args = append(args, table[0], table[1])
	}
	if merge {
		query.WriteString("; " + mergeGrantsSelect)
		for _, table := range mergeTables {
			query.WriteString("; " + mergeCatalogSelect)
			args = append(args, table[0], table[1], table[0], table[1], table[0], table[1])
		}
	}
	rows, err := tx.tx.QueryContext(ctx, query.String(), args...)
	if err != nil {
		return err
	}
	// A Query result starts at the first SELECT: the driver's leading OK
	// processing proves the fixed lineage INSERT executed successfully.
	// Optional metadata fallback may now repeat SELECTs without that INSERT.
	if lineage != nil {
		tx.pendingLineage = nil
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	for index, table := range tables {
		if index > 0 && !rows.NextResultSet() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("mysqlout: physical table result is missing")
		}
		if rows.Next() {
			return fmt.Errorf("mysqlout: physical table probe returned unexpected rows")
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if !rows.NextResultSet() {
			if err := rows.Err(); err != nil {
				return err
			}
			return fmt.Errorf("mysqlout: target catalog result is missing")
		}
		columns := make(map[string]targetColumn)
		for rows.Next() {
			var name, kind, expression, extra string
			if err := rows.Scan(&name, &kind, &expression, &extra); err != nil {
				return err
			}
			columns[strings.ToLower(name)] = targetColumn{generated: expression != "", dataType: strings.ToLower(kind), extra: extra}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(columns) == 0 {
			return fmt.Errorf("mysqlout: target catalog is empty")
		}
		if tx.columns == nil {
			tx.columns = make(map[string]map[string]targetColumn)
		}
		tx.columns[table[0]+"\x00"+table[1]] = columns
	}
	if !merge {
		if err := rows.Close(); err != nil {
			return err
		}
		tx.pendingLineage = nil
		return nil
	}
	if !rows.NextResultSet() {
		return fmt.Errorf("mysqlout: merge grant result is missing: %w", rows.Err())
	}
	visible, err := readMergeVisibility(rows)
	if err != nil {
		return err
	}
	pending := make(map[string]mergeCatalog, len(mergeTables))
	for _, table := range mergeTables {
		if !rows.NextResultSet() {
			return fmt.Errorf("mysqlout: merge catalog result is missing: %w", rows.Err())
		}
		facts, err := readMergeCatalog(rows, visible)
		if err != nil {
			return err
		}
		pending[table[0]+"\x00"+table[1]] = facts
	}
	if rows.NextResultSet() {
		return fmt.Errorf("mysqlout: unexpected merge catalog result")
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if tx.mergeCatalog == nil {
		tx.mergeCatalog = make(map[string]mergeCatalog)
	}
	tx.pendingLineage = nil
	maps.Copy(tx.mergeCatalog, pending)
	return nil
}
