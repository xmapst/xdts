package snapshot

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

// ValidateReinitPlan checks the confirmed authority before reading or persisting
// a snapshot. Every selected source table must carry its confirmed definition.
func ValidateReinitPlan(plan *iproto.ReinitSourcePlan) error {
	if plan == nil || len(plan.GetDigest()) != 64 || len(plan.GetTables()) == 0 || len(plan.GetTables()) > 4000 {
		return fmt.Errorf("snapshot: invalid confirmed source plan")
	}
	if _, err := hex.DecodeString(plan.GetDigest()); err != nil {
		return fmt.Errorf("snapshot: invalid confirmed source digest")
	}
	seen := map[[2]string]bool{}
	for _, table := range plan.GetTables() {
		key := [2]string{table.GetSchema(), table.GetTable()}
		if !validIdentifier(key[0]) || !validIdentifier(key[1]) || seen[key] {
			return fmt.Errorf("snapshot: invalid confirmed source scope")
		}
		seen[key] = true
		if table.GetExpectedDdl() == "" {
			return fmt.Errorf("snapshot: confirmed source definition required")
		}
		kind, schema, name, err := schemamig.CreationIdentity(key[0], table.GetExpectedDdl())
		if err != nil || kind != schemamig.ObjectTable || schema != key[0] || name != key[1] {
			return fmt.Errorf("snapshot: confirmed source definition identity mismatch")
		}
	}
	return nil
}

type iReinitSource interface {
	VerifyReinitTables(context.Context, *iproto.ReinitSourcePlan, []Table) error
	PlannedTables(context.Context, Plan) ([]Table, error)
}

func matchReinitScope(plan *iproto.ReinitSourcePlan, tables []TablePlan) error {
	if len(tables) != len(plan.GetTables()) {
		return fmt.Errorf("snapshot: stored confirmed source scope changed")
	}
	actual := map[[2]string]bool{}
	for _, table := range tables {
		actual[[2]string{table.Schema, table.Table}] = true
	}
	for _, table := range plan.GetTables() {
		if !actual[[2]string{table.GetSchema(), table.GetTable()}] {
			return fmt.Errorf("snapshot: stored confirmed source scope changed")
		}
	}
	return nil
}

func (r *SQLBoundaryReader) VerifyReinitTables(ctx context.Context, plan *iproto.ReinitSourcePlan, tables []Table) error {
	if err := ValidateReinitPlan(plan); err != nil {
		return err
	}
	if len(tables) != len(plan.GetTables()) {
		return fmt.Errorf("snapshot: confirmed source table range changed")
	}
	actual := map[[2]string]bool{}
	for _, table := range tables {
		actual[[2]string{table.Schema, table.Name}] = true
	}
	for _, table := range plan.GetTables() {
		if !actual[[2]string{table.GetSchema(), table.GetTable()}] {
			return fmt.Errorf("snapshot: confirmed source table range changed")
		}
		queryCtx, cancel := context.WithTimeout(ctx, r.timeout)
		err := r.withReadOnlyConn(queryCtx, func(ctx context.Context, conn *sql.Conn) error {
			var name, ddl string
			if err := conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quoteIdentifier(table.GetSchema())+"."+quoteIdentifier(table.GetTable())).Scan(&name, &ddl); err != nil {
				return fmt.Errorf("snapshot: read confirmed source definition failed")
			}
			matched, err := schemamig.ConfirmedTableDefinitionEqual(table.GetExpectedDdl(), ddl)
			if err != nil || !matched {
				return fmt.Errorf("snapshot: confirmed source table definition changed")
			}
			return nil
		})
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}

// Restore exactly the persisted table order; new objects belong to the ongoing
// incremental stream and must never receive previously allocated chunk IDs.
func (r *SQLBoundaryReader) PlannedTables(ctx context.Context, plan Plan) ([]Table, error) {
	tables := make([]Table, 0, len(plan.Tables))
	for _, planned := range plan.Tables {
		table, err := r.Table(ctx, planned.Schema, planned.Table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}
