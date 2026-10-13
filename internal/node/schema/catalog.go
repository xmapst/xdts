package schema

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/xmapst/xdts/internal/node/position"
)

// Catalog borrows a source-only pool. Each table definition is read with an
// explicit read-only session and bound to an unchanged source GTID set. The
// caller must compare that set with its replay position before installing it.
type Catalog struct {
	db            *sql.DB
	caseSensitive bool
	timeout       time.Duration
}

var errCatalogChanged = errors.New("schema: source changed while reading catalog")

func NewCatalog(db *sql.DB, caseSensitive bool, timeout time.Duration) (*Catalog, error) {
	if db == nil || timeout <= 0 {
		return nil, fmt.Errorf("schema: database and positive catalog timeout required")
	}
	return &Catalog{db: db, caseSensitive: caseSensitive, timeout: timeout}, nil
}

func (c *Catalog) LoadTable(ctx context.Context, schemaName, tableName string) (TableStructure, string, error) {
	return c.loadTable(ctx, schemaName, tableName, nil)
}

// LoadTableForReplay accepts a current definition only after proving every
// intervening committed transaction is row based and contains no schema change.
// Missing binlogs, statement transactions and unknown events remain errors.
func (c *Catalog) LoadTableForReplay(ctx context.Context, schemaName, tableName, replay string) (TableStructure, error) {
	table, _, err := c.loadTable(ctx, schemaName, tableName, func(ctx context.Context, conn *sql.Conn, through string) error {
		return verifyRowOnlyHistory(ctx, conn, replay, through)
	})
	return table, err
}

func CatalogChanged(err error) bool { return errors.Is(err, errCatalogChanged) }

func (c *Catalog) loadTable(ctx context.Context, schemaName, tableName string, verify func(context.Context, *sql.Conn, string) error) (TableStructure, string, error) {
	if schemaName == "" || tableName == "" || !utf8.ValidString(schemaName+tableName) || utf8.RuneCountInString(schemaName) > 64 || utf8.RuneCountInString(tableName) > 64 || strings.ContainsRune(schemaName+tableName, 0) {
		return TableStructure{}, "", fmt.Errorf("schema: invalid catalog identifier")
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var result TableStructure
	var validThrough string
	err := position.WithReadOnlyConn(ctx, c.db, func(ctx context.Context, conn *sql.Conn) error {
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&validThrough); err != nil {
			return fmt.Errorf("schema: read catalog baseline failed")
		}
		quote := func(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }
		var actualName, definition string
		if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quote(schemaName)+"."+quote(tableName)).Scan(&actualName, &definition); err != nil {
			return fmt.Errorf("schema: read source table definition failed")
		}
		tracker := NewTracker(c.caseSensitive)
		if _, warning := tracker.ApplyDDL(schemaName, definition, "", 0); warning != "" {
			return fmt.Errorf("schema: catalog definition cannot be tracked")
		}
		var ok bool
		result, ok = tracker.Lookup(schemaName, actualName)
		if !ok || len(result.Columns) == 0 {
			return fmt.Errorf("schema: source object is not a base table")
		}
		// Cross-check the parser's physical column list and generated attributes
		// against the read-only information_schema, rather than inferring them
		// from TableMap (which never carries a complete unique-key collection).
		rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME, GENERATION_EXPRESSION, EXTRA
			FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`, schemaName, tableName)
		if err != nil {
			return fmt.Errorf("schema: read source columns failed")
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		index := 0
		for rows.Next() {
			var name, generated, extra string
			if err := rows.Scan(&name, &generated, &extra); err != nil {
				return fmt.Errorf("schema: decode source column failed")
			}
			if index >= len(result.Columns) || !strings.EqualFold(result.Columns[index].Name, name) || result.Columns[index].Generated != (generated != "") {
				return fmt.Errorf("schema: catalog column definition changed or cannot be tracked")
			}
			result.Columns[index].GeneratedStored = generated != "" && strings.Contains(extra, "STORED GENERATED")
			index++
		}
		if err := rows.Err(); err != nil || index != len(result.Columns) {
			return fmt.Errorf("schema: catalog columns incomplete")
		}
		_ = rows.Close()
		var after string
		if err := conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&after); err != nil {
			return fmt.Errorf("schema: read catalog final position failed")
		}
		if verify != nil {
			// Also cover metadata reads that began before the replay position.
			// A DDL committed during those reads cannot be hidden by a later
			// replay position that already contains that DDL.
			if after != validThrough {
				if err := verifyRowOnlyHistory(ctx, conn, validThrough, after); err != nil {
					return err
				}
			}
			if err := verify(ctx, conn, after); err != nil {
				return err
			}
			validThrough = after
		} else if after != validThrough {
			return errCatalogChanged
		}
		return nil
	})
	if err != nil {
		return TableStructure{}, "", err
	}
	return result.clone(), validThrough, nil
}

// Install transfers a standalone table value into the session's Tracker.
func (t *Tracker) Install(table TableStructure) error {
	if table.Schema == "" || table.Table == "" || len(table.Columns) == 0 {
		return fmt.Errorf("schema: incomplete catalog table")
	}
	t.tables[t.tableKey(table.Schema, table.Table)] = table.clone()
	return nil
}
