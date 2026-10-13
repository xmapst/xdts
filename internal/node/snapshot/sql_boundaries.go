package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/node/position"
)

// SQLBoundaryReader borrows a source-only pool. Each operation takes a dedicated
// read-only connection and resets it before returning it to the pool. The pool
// remains owned by the source workflow that constructed this reader.
type SQLBoundaryReader struct {
	db       *sql.DB
	timeout  time.Duration
	filter   ITableFilter
	filterMu sync.RWMutex
	replica  *ReplicaReadPolicy
}

func NewSQLBoundaryReader(db *sql.DB, timeout time.Duration) (*SQLBoundaryReader, error) {
	if db == nil || timeout <= 0 {
		return nil, fmt.Errorf("snapshot: database and positive query timeout required")
	}
	return &SQLBoundaryReader{db: db, timeout: timeout}, nil
}

// Table reads exact source metadata. Prefix and functional unique indexes are
// recorded as incomplete; they cannot be used as complete row-position keys.
func (r *SQLBoundaryReader) Table(ctx context.Context, schema, name string) (Table, error) {
	if !validIdentifier(schema) || !validIdentifier(name) || dblineage.Reserved(schema) {
		return Table{}, fmt.Errorf("snapshot: invalid table identifier")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	table := Table{Schema: schema, Name: name}
	err := r.withReadOnlyConn(ctx, func(ctx context.Context, conn *sql.Conn) error {
		var estimate sql.NullInt64
		if err := conn.QueryRowContext(ctx, `SELECT TABLE_ROWS FROM information_schema.TABLES
			WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND TABLE_TYPE = 'BASE TABLE'`, schema, name).Scan(&estimate); err != nil {
			return fmt.Errorf("snapshot: read table metadata failed")
		}
		if estimate.Valid && estimate.Int64 > 0 {
			table.EstimatedRows = uint64(estimate.Int64)
		}
		rows, err := conn.QueryContext(ctx, `SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, GENERATION_EXPRESSION,COALESCE(COLLATION_NAME,'')
			FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, schema, name)
		if err != nil {
			return fmt.Errorf("snapshot: read column metadata failed")
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		for rows.Next() {
			var column Column
			var definition, nullable, generated string
			if err := rows.Scan(&column.Name, &column.DataType, &definition, &nullable, &generated, &column.Collation); err != nil {
				return fmt.Errorf("snapshot: decode column metadata failed")
			}
			column.Unsigned = integerBits(column.DataType) != 0 && strings.Contains(strings.ToLower(definition), "unsigned")
			column.Nullable = nullable == "YES"
			column.Generated = generated != ""
			column.ColumnType = definition
			table.Columns = append(table.Columns, column)
		}
		if rows.Err() != nil {
			return fmt.Errorf("snapshot: read columns failed")
		}
		_ = rows.Close()
		indexes, err := conn.QueryContext(ctx, `SELECT INDEX_NAME, COLUMN_NAME, SUB_PART
			FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND NON_UNIQUE = 0
			ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, name)
		if err != nil {
			return fmt.Errorf("snapshot: read index metadata failed")
		}
		defer func(cleanup func() error) { _ = cleanup() }(indexes.Close)
		var current *Index
		for indexes.Next() {
			var indexName string
			var column sql.NullString
			var prefix sql.NullInt64
			if err := indexes.Scan(&indexName, &column, &prefix); err != nil {
				return fmt.Errorf("snapshot: decode index metadata failed")
			}
			if current == nil || current.Name != indexName {
				if indexName == "PRIMARY" {
					table.PrimaryKey = Index{Name: indexName}
					current = &table.PrimaryKey
				} else {
					table.UniqueKeys = append(table.UniqueKeys, Index{Name: indexName})
					current = &table.UniqueKeys[len(table.UniqueKeys)-1]
				}
			}
			if !column.Valid || prefix.Valid {
				current.Incomplete = true
			}
			if column.Valid {
				current.Columns = append(current.Columns, column.String)
			}
		}
		if indexes.Err() != nil {
			return fmt.Errorf("snapshot: read indexes failed")
		}
		return nil
	})
	if err != nil {
		return Table{}, err
	}
	if err := validateTable(table); err != nil {
		return Table{}, err
	}
	return table, nil
}

func (r *SQLBoundaryReader) IntegerBounds(ctx context.Context, table Table, key Key) (string, string, error) {
	if err := validateReadKey(table, key); err != nil {
		return "", "", err
	}
	if len(key.Columns) != 1 || integerBits(key.Columns[0].DataType) == 0 {
		return "", "", fmt.Errorf("snapshot: integer bounds require one integer key")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var min, max sql.NullString
	column := quoteIdentifier(key.Columns[0].Name)
	err := r.withReadOnlyConn(ctx, func(ctx context.Context, conn *sql.Conn) error {
		if err := conn.QueryRowContext(ctx, "SELECT MIN("+column+"), MAX("+column+") FROM "+qualifiedTable(table)).Scan(&min, &max); err != nil {
			return fmt.Errorf("snapshot: query integer bounds failed")
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return min.String, max.String, nil
}

func (r *SQLBoundaryReader) Page(ctx context.Context, table Table, key Key, after Boundary, limit uint64) (Page, error) {
	if err := validateReadKey(table, key); err != nil {
		return Page{}, err
	}
	if limit == 0 || (len(after.Values) > 0 && !validBoundary(after, key)) {
		return Page{}, fmt.Errorf("snapshot: invalid keyset page request")
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	expressions := keyExpressions(key)
	query := "SELECT " + strings.Join(expressions, ", ") + " FROM " + qualifiedTable(table)
	var args []any
	if len(after.Values) > 0 {
		query += " WHERE " + tuple(expressions) + " > " + placeholders(len(expressions))
		for _, value := range after.Values {
			args = append(args, value)
		}
	}
	query += " ORDER BY " + strings.Join(expressions, ", ") + " LIMIT ?"
	args = append(args, limit)
	var page Page
	err := r.withReadOnlyConn(ctx, func(ctx context.Context, conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("snapshot: query keyset page failed")
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		values := make([]sql.RawBytes, len(key.Columns))
		dest := make([]any, len(values))
		for i := range values {
			dest[i] = &values[i]
		}
		for rows.Next() {
			if err := rows.Scan(dest...); err != nil {
				return fmt.Errorf("snapshot: decode keyset page failed")
			}
			page.Last.Values = make([][]byte, len(values))
			for i, value := range values {
				if value == nil {
					return fmt.Errorf("snapshot: nullable positioning key returned")
				}
				page.Last.Values[i] = append([]byte{}, value...)
			}
			page.Rows++
		}
		if rows.Err() != nil {
			return fmt.Errorf("snapshot: read keyset page failed")
		}
		return nil
	})
	return page, err
}

// Boundary and row queries must use identical UTC/text session settings,
// particularly when a TIMESTAMP is part of a positioning key. The borrowed
// database pool is source-only; its owner must not share it with target writers.
func (r *SQLBoundaryReader) withReadOnlyConn(ctx context.Context, f func(context.Context, *sql.Conn) error) error {
	return position.WithReadOnlyConn(ctx, r.db, func(ctx context.Context, conn *sql.Conn) error {
		if r.replica != nil {
			if err := r.replica.verify(ctx, conn); err != nil {
				return err
			}
		}
		if _, err := conn.ExecContext(ctx, "SET SESSION time_zone = '+00:00'"); err != nil {
			return fmt.Errorf("snapshot: set UTC session failed")
		}
		if _, err := conn.ExecContext(ctx, "SET NAMES utf8mb4"); err != nil {
			return fmt.Errorf("snapshot: set text session failed")
		}
		return f(ctx, conn)
	})
}

func validateReadKey(table Table, key Key) error {
	chosen, err := ChooseKey(table)
	if err != nil {
		return err
	}
	if len(chosen.Columns) != len(key.Columns) || chosen.Name != key.Name {
		return fmt.Errorf("snapshot: positioning key disagrees with metadata")
	}
	for i, c := range chosen.Columns {
		if c != key.Columns[i] {
			return fmt.Errorf("snapshot: positioning column disagrees with metadata")
		}
	}
	return nil
}

func quoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func qualifiedTable(table Table) string {
	return quoteIdentifier(table.Schema) + "." + quoteIdentifier(table.Name)
}

func keyExpressions(key Key) []string {
	expressions := make([]string, len(key.Columns))
	for i, c := range key.Columns {
		expressions[i] = quoteIdentifier(c.Name)
		// ENUM/SET order numerically in MySQL. BIT arrives as raw binary;
		// projecting its number gives a parameter that compares consistently.
		switch strings.ToLower(c.DataType) {
		case "enum", "set", "bit":
			expressions[i] = "(" + expressions[i] + " + 0)"
		}
	}
	return expressions
}

func tuple(expressions []string) string {
	if len(expressions) == 1 {
		return expressions[0]
	}
	return "(" + strings.Join(expressions, ", ") + ")"
}

func placeholders(count int) string {
	return tuple(strings.Split(strings.TrimSuffix(strings.Repeat("?,", count), ","), ","))
}
