package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/dblineage"
)

type ITableFilter interface{ MatchTable(string, string) bool }

// SetTableFilter installs an immutable rule mirror. A catalog operation takes
// one snapshot of it before SQL IO; runtime updates never mutate that snapshot.
func (r *SQLBoundaryReader) SetTableFilter(filter ITableFilter) {
	r.filterMu.Lock()
	r.filter = filter
	r.filterMu.Unlock()
}

func (r *SQLBoundaryReader) Tables(ctx context.Context) ([]Table, error) {
	return r.catalogTables(ctx, nil)
}

// TablesForExtension discovers newly selected objects using the current rule
// mirror. Completed tables retain their durable baseline and order, including
// objects excluded by a later rule. They need no new SQL or key allocation.
func (r *SQLBoundaryReader) TablesForExtension(ctx context.Context, plan Plan) ([]Table, error) {
	return r.catalogTables(ctx, &plan)
}

func (r *SQLBoundaryReader) catalogTables(ctx context.Context, plan *Plan) ([]Table, error) {
	r.filterMu.RLock()
	filter := r.filter
	r.filterMu.RUnlock()
	known := make(map[[2]string]bool)
	var tables []Table
	if plan != nil {
		for _, planned := range plan.Tables {
			known[[2]string{planned.Schema, planned.Table}] = true
			if planned.Baseline == nil {
				metadata, err := r.Table(ctx, planned.Schema, planned.Table)
				if err != nil {
					return nil, err
				}
				tables = append(tables, metadata)
			} else {
				tables = append(tables, *planned.clone().Baseline)
			}
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var names [][2]string
	err := r.withReadOnlyConn(queryCtx, func(ctx context.Context, conn *sql.Conn) error {
		rows, err := conn.QueryContext(ctx, `SELECT TABLE_SCHEMA,TABLE_NAME FROM information_schema.TABLES
			WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA NOT IN ('information_schema','mysql','performance_schema','sys')
			ORDER BY TABLE_SCHEMA,TABLE_NAME`)
		if err != nil {
			return fmt.Errorf("snapshot: list application tables failed")
		}
		defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
		for rows.Next() {
			var schema, name string
			if err := rows.Scan(&schema, &name); err != nil {
				return fmt.Errorf("snapshot: decode table list failed")
			}
			if !known[[2]string{schema, name}] && !dblineage.Reserved(schema) && (filter == nil || filter.MatchTable(schema, name)) {
				names = append(names, [2]string{schema, name})
			}
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if len(names) == 0 && len(tables) == 0 {
		return nil, fmt.Errorf("snapshot: selected table range is empty")
	}
	for _, name := range names {
		table, err := r.Table(ctx, name[0], name[1])
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (r *SQLBoundaryReader) GTIDSet(ctx context.Context) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	var set string
	err := r.withReadOnlyConn(ctx, func(ctx context.Context, conn *sql.Conn) error {
		return conn.QueryRowContext(ctx, "SELECT @@GLOBAL.gtid_executed").Scan(&set)
	})
	if err != nil {
		return "", fmt.Errorf("snapshot: query GTID watermark failed")
	}
	return set, nil
}

var _ IRunSource = (*SQLBoundaryReader)(nil)
var _ IPlanReader = (*Reader)(nil)

// DefaultOperationTimeout bounds a single commit or confirmed AMQP publication,
// rather than the duration of the whole snapshot.
const DefaultOperationTimeout = 30 * time.Second
