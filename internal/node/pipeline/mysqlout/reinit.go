package mysqlout

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func validReinitName(name string) bool {
	return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
}

// ApplyReinitPlan verifies every table before any DELETE. The caller persists
// plan identity and completion; SQL GTIDs cover a crash before that completion.
func (w *Writer) ApplyReinitPlan(ctx context.Context, plan *iproto.ReinitTargetPlan, schemaPlan *iproto.SchemaMigrationPlan) (resultErr error) {
	if plan == nil || len(plan.GetTables()) == 0 || len(plan.GetTables()) > 4000 || (plan.GetHandling() != iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_KEEP && plan.GetHandling() != iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_CLEAR) {
		return fmt.Errorf("mysqlout: invalid reinit target plan")
	}
	clear := plan.GetHandling() == iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_CLEAR
	id, err := uuid.Parse(plan.GetPrepareUuid())
	if clear && (err != nil || id == uuid.Nil() || id.String() != plan.GetPrepareUuid()) || !clear && plan.GetPrepareUuid() != "" {
		return fmt.Errorf("mysqlout: invalid reinit preparation UUID")
	}
	var grants []string
	if err := w.ensureConnection(ctx); err != nil {
		return err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	if clear {
		grants, err = w.reinitTriggerGrants(ctx, w.conn)
		if err != nil {
			return err
		}
	}
	seen := map[string]bool{}
	var lowerCase int
	if err := w.conn.QueryRowContext(ctx, "SELECT @@lower_case_table_names").Scan(&lowerCase); err != nil {
		return err
	}
	present := make([]bool, len(plan.GetTables()))
	for i, table := range plan.GetTables() {
		key := table.GetSchema() + "\x00" + table.GetTable()
		if lowerCase != 0 {
			key = strings.ToLower(key)
		}
		if !validReinitName(table.GetSchema()) || !validReinitName(table.GetTable()) || dblineage.Reserved(table.GetSchema()) || seen[key] {
			return fmt.Errorf("mysqlout: invalid reinit table scope")
		}
		seen[key] = true
		present[i], err = w.verifyReinitTable(ctx, w.conn, table, schemaPlan, lowerCase, clear, grants)
		if err != nil {
			return err
		}
	}
	if !clear {
		return nil
	}
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return err
	}
	executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	for i, table := range plan.GetTables() {
		gtid := fmt.Sprintf("%s:%d", plan.GetPrepareUuid(), i+1)
		one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
		if executed.Contain(one) {
			continue
		}
		if err := withGTIDTxn(ctx, w.conn, gtid, func(ctx context.Context, tx *dmlTxn) error {
			if err := w.writeCleanupLineage(ctx, tx, gtid); err != nil {
				return err
			}
			if present[i] {
				// This read retains a metadata lock until the DELETE transaction
				// ends. Read definitions through the same transaction, so queued
				// DDL cannot interpose between verification and the write.
				rows, err := tx.tx.QueryContext(ctx, "SELECT 1 FROM "+quoteIdent(table.GetSchema())+"."+quoteIdent(table.GetTable())+" LIMIT 0")
				if err != nil {
					return err
				}
				if err := rows.Close(); err != nil {
					return err
				}
			}
			currentGrants, err := w.reinitTriggerGrants(ctx, tx.tx)
			if err != nil {
				return err
			}
			nowPresent, err := w.verifyReinitTable(ctx, tx.tx, table, schemaPlan, lowerCase, true, currentGrants)
			if err != nil {
				return err
			}
			if nowPresent != present[i] {
				return fmt.Errorf("mysqlout: reinit target presence changed during preparation")
			}
			if !nowPresent {
				return nil
			}
			_, err = tx.execDML(ctx, "DELETE FROM "+quoteIdent(table.GetSchema())+"."+quoteIdent(table.GetTable()))
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) reinitTriggerGrants(ctx context.Context, query iSchemaQuery) ([]string, error) {
	rows, err := query.QueryContext(ctx, "SHOW GRANTS")
	if err != nil {
		return nil, err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	var grants []string
	for rows.Next() {
		var grant string
		if err := rows.Scan(&grant); err != nil {
			return nil, err
		}
		grants = append(grants, grant)
	}
	return grants, rows.Err()
}

// verifyReinitTable is used both by the all-table preflight and, under a
// transaction metadata lock, immediately before deleting an existing table.
func (w *Writer) verifyReinitTable(ctx context.Context, query iSchemaQuery, table *iproto.ReinitTargetTable, schemaPlan *iproto.SchemaMigrationPlan, lowerCase int, clear bool, grants []string) (bool, error) {
	expected := table.GetExpectedDdl()
	fresh := expected == ""
	if fresh {
		for _, step := range schemaPlan.GetSteps() {
			if step.GetKind() == iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE && step.GetAction() == iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE && step.GetSchema() == table.GetSchema() && step.GetObjectName() == table.GetTable() {
				expected = step.GetDdl()
				break
			}
		}
	}
	kind, schema, name, parseErr := schemamig.CreationIdentity(table.GetSchema(), expected)
	matches := schema == table.GetSchema() && name == table.GetTable()
	if lowerCase != 0 {
		matches = strings.EqualFold(schema, table.GetSchema()) && strings.EqualFold(name, table.GetTable())
	}
	if parseErr != nil || kind != schemamig.ObjectTable || !matches {
		return false, fmt.Errorf("mysqlout: reinit table definition identity mismatch")
	}
	var count int
	var tableType, engine string
	if err := query.QueryRowContext(ctx, "SELECT COUNT(*),COALESCE(MAX(TABLE_TYPE),''),COALESCE(MAX(ENGINE),'') FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", table.GetSchema(), table.GetTable()).Scan(&count, &tableType, &engine); err != nil {
		return false, err
	}
	if count == 0 {
		if !fresh {
			return false, fmt.Errorf("mysqlout: confirmed reinit target table missing")
		}
		return false, nil
	}
	if tableType != "BASE TABLE" || clear && !strings.EqualFold(engine, "InnoDB") {
		return false, fmt.Errorf("mysqlout: reinit requires a transactional target table")
	}
	actual, err := w.schemaDefinitionWith(ctx, query, "TABLE", table.GetSchema(), table.GetTable())
	if err != nil {
		return false, err
	}
	if fresh {
		actual, err = w.tableDefinitionForComparison(ctx, query, table.GetSchema(), table.GetTable(), actual)
		if err != nil {
			return false, err
		}
		expected, _, err = schemamig.SplitTableForeignKeys(table.GetSchema(), expected)
		if err == nil {
			actual, _, err = schemamig.SplitTableForeignKeys(table.GetSchema(), actual)
		}
		if err != nil {
			return false, err
		}
	}
	matched := false
	if fresh {
		diff, compareErr := schemamig.CompareDefinitions(schemamig.ObjectTable, expected, actual)
		err, matched = compareErr, len(diff) == 0
	} else {
		matched, err = schemamig.ConfirmedTableDefinitionEqual(expected, actual)
	}
	if err != nil || !matched {
		return false, fmt.Errorf("mysqlout: confirmed reinit target definition changed")
	}
	if clear {
		if !schemamig.GrantsTableTrigger(grants, table.GetSchema(), table.GetTable()) {
			return false, fmt.Errorf("mysqlout: reinit cannot verify complete DELETE trigger visibility")
		}
		if err := w.verifyReinitDeleteTriggers(ctx, query, table.GetSchema(), table.GetTable()); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (w *Writer) verifyReinitDeleteTriggers(ctx context.Context, query iSchemaQuery, schema, table string) error {
	rows, err := query.QueryContext(ctx, "SELECT TRIGGER_SCHEMA,TRIGGER_NAME FROM information_schema.TRIGGERS WHERE EVENT_OBJECT_SCHEMA=? AND EVENT_OBJECT_TABLE=? AND EVENT_MANIPULATION='DELETE' ORDER BY TRIGGER_SCHEMA,TRIGGER_NAME LIMIT 4001", schema, table)
	if err != nil {
		return err
	}
	type triggerName struct{ schema, name string }
	var triggers []triggerName
	for rows.Next() {
		var name triggerName
		if err := rows.Scan(&name.schema, &name.name); err != nil {
			_ = rows.Close()
			return err
		}
		triggers = append(triggers, name)
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return err
	}
	if len(triggers) > 4000 {
		return fmt.Errorf("mysqlout: reinit trigger count exceeds limit")
	}
	for _, trigger := range triggers {
		ddl, err := w.schemaDefinitionWith(ctx, query, "TRIGGER", trigger.schema, trigger.name)
		if err != nil {
			return err
		}
		guarded, err := schemamig.TriggerSuppressesApply(ddl)
		if err != nil || !guarded {
			return fmt.Errorf("mysqlout: reinit DELETE trigger is not guarded")
		}
	}
	return nil
}
