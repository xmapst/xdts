package probe

import (
	"context"
	"database/sql"
	"errors"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func (r *precheckRun) checkTables(ctx context.Context) error {
	keyless, missing, badLWW, nonempty, badTriggers := []string{}, []string{}, []string{}, []string{}, []string{}
	checksLWW := r.args.GetCheckLww()
	definitionBytes := 0
	lowerCase := r.result.GetKeyParams()["lower_case_table_names"]
	seen := map[string]bool{}
	for _, table := range r.args.GetTables() {
		schema, name := table.GetSchema(), table.GetTable()
		identity := schema + "." + name
		key := schema + "\x00" + name
		if lowerCase != "0" {
			key = strings.ToLower(key)
		}
		if seen[key] {
			return errors.New("duplicate table after server name folding")
		}
		seen[key] = true
		predicate := "BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=?"
		if lowerCase != "0" {
			predicate = "LOWER(TABLE_SCHEMA)=LOWER(?) AND LOWER(TABLE_NAME)=LOWER(?)"
		}
		var actualSchema, actualTable, tableCollation string
		err := r.conn.QueryRowContext(ctx, "SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_TYPE='BASE TABLE' AND "+predicate, schema, name).Scan(&actualSchema, &actualTable, &tableCollation)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		checksLWW = checksLWW || table.GetLwwTimestampColumn() != ""
		if errors.Is(err, sql.ErrNoRows) {
			if r.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
				return errors.New("selected source table missing or not visible")
			}
			missing = append(missing, identity)
			// A missing target LWW column needs proof from the migration plan.
			if table.GetLwwTimestampColumn() != "" {
				badLWW = append(badLWW, identity)
			}
			continue
		}
		qualified := quoteDefinitionName(actualSchema) + "." + quoteDefinitionName(actualTable)
		var ignored, ddl string
		if err := r.conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+qualified).Scan(&ignored, &ddl); err != nil {
			return err
		}
		definitionBytes += len(ddl) + len(actualSchema) + len(actualTable) + len(tableCollation)
		if definitionBytes > maxDefsBytes {
			return errDefsTooLarge
		}
		r.result.TableDefinitions = append(r.result.TableDefinitions, &iproto.DefsEntry{Schema: actualSchema, ObjectName: actualTable, Kind: iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE, Definition: ddl, TableCollationV1: tableCollation})
		if r.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
			usable, engine, err := schemamig.InitializationTableProperties(ddl)
			if err != nil {
				return err
			}
			if !usable || (r.args.GetSnapshot() && !strings.EqualFold(engine, "InnoDB")) {
				keyless = append(keyless, identity)
			}
		} else {
			if !schemamig.GrantsTableTrigger(r.grants, actualSchema, actualTable) {
				badTriggers = append(badTriggers, identity+":trigger_visibility_unverified")
			} else {
				unguarded, err := r.unguardedTriggers(ctx, actualSchema, actualTable)
				if err != nil {
					return err
				}
				badTriggers = append(badTriggers, unguarded...)
			}
			if r.args.GetSnapshot() {
				var present int
				err := r.conn.QueryRowContext(ctx, "SELECT 1 FROM "+qualified+" LIMIT 1").Scan(&present)
				if err != nil && !errors.Is(err, sql.ErrNoRows) {
					return err
				}
				if err == nil {
					nonempty = append(nonempty, identity)
				}
			}
		}
		if table.GetLwwTimestampColumn() != "" {
			var kind string
			var precision sql.NullInt64
			err := r.conn.QueryRowContext(ctx, `SELECT DATA_TYPE,DATETIME_PRECISION FROM information_schema.COLUMNS
				WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND LOWER(COLUMN_NAME)=LOWER(?)`, actualSchema, actualTable, table.GetLwwTimestampColumn()).Scan(&kind, &precision)
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return err
			}
			if err != nil || (kind != "timestamp" && kind != "datetime") || !precision.Valid || precision.Int64 < 3 {
				badLWW = append(badLWW, identity+"."+table.GetLwwTimestampColumn())
			}
		}
	}
	if r.side == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		code, warning := "PRE-07", false
		if !r.args.GetSnapshot() {
			code, warning = "PRE-18", r.args.GetKeylessIncrementalWarning()
		}
		r.add(code, len(keyless) == 0, warning, "同步表必须存在完整主键或非空完整唯一键；全量表使用 InnoDB", map[string]any{"tables": keyless, "missing": missing})
	} else {
		r.add("PRE-12", len(missing) == 0 || r.args.GetAllowMissingTargetTables(), false,
			"目标表必须存在；缺失表须由已审核结构迁移计划覆盖", map[string]any{"missing": missing, "requires_migration_plan": len(missing) != 0 && r.args.GetAllowMissingTargetTables()})
		r.add("PRE-16", len(badTriggers) == 0, false, "同步范围内目标触发器必须可完整检查且带 @xdts_apply 守卫", map[string]any{"unguarded_or_unverified": badTriggers})
		if r.args.GetSnapshot() {
			r.add("PRE-19", len(nonempty) == 0, true, "全量初始化目标表应为空，非空需明确确认", map[string]any{"nonempty": nonempty})
		}
	}
	if checksLWW {
		r.add("PRE-17", len(badLWW) == 0, false, "LWW 时间戳列必须存在且为 datetime/timestamp、精度至少 3", map[string]any{"columns": badLWW})
	}
	return nil
}

func (r *precheckRun) unguardedTriggers(ctx context.Context, schema, table string) ([]string, error) {
	rows, err := r.conn.QueryContext(ctx, `SELECT TRIGGER_NAME FROM information_schema.TRIGGERS
		WHERE BINARY TRIGGER_SCHEMA=? AND BINARY EVENT_OBJECT_TABLE=? ORDER BY TRIGGER_NAME LIMIT 4001`, schema, table)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			return nil, err
		}
		names = append(names, name)
		if len(names) > maxDefsObjects {
			_ = rows.Close()
			return nil, errDefsTooLarge
		}
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, err
	}
	bad := []string{}
	for _, name := range names {
		var trigger, mode, ddl, charset, collation, dbCollation string
		var created sql.NullString
		err := r.conn.QueryRowContext(ctx, "SHOW CREATE TRIGGER "+quoteDefinitionName(schema)+"."+quoteDefinitionName(name)).Scan(&trigger, &mode, &ddl, &charset, &collation, &dbCollation, &created)
		if err != nil {
			return nil, err
		}
		if len(ddl) > maxDefsBytes {
			return nil, errDefsTooLarge
		}
		guarded, err := schemamig.TriggerSuppressesApply(ddl)
		if err != nil || !guarded {
			bad = append(bad, schema+"."+name)
		}
	}
	return bad, nil
}
