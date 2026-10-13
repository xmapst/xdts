package mysqlout

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func schemaObjectKind(k iproto.SchemaObjectKind) (schemamig.ObjectKind, string, error) {
	switch k {
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_DATABASE:
		return schemamig.ObjectUnknown, "DATABASE", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TABLE:
		return schemamig.ObjectTable, "TABLE", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW:
		return schemamig.ObjectView, "VIEW", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_PROCEDURE:
		return schemamig.ObjectProcedure, "PROCEDURE", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FUNCTION:
		return schemamig.ObjectFunction, "FUNCTION", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TRIGGER:
		return schemamig.ObjectTrigger, "TRIGGER", nil
	case iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_EVENT:
		return schemamig.ObjectEvent, "EVENT", nil
	}
	return 0, "", fmt.Errorf("mysqlout: unknown schema object kind")
}

// ApplySchemaStep consumes already rewritten SQL. Reapplying a mapping or a
// trigger guard here would change the confirmed plan. A duplicate GTID still
// verifies the actual definition so externally changed targets cannot pass.
func (w *Writer) ApplySchemaStep(ctx context.Context, identity string, step *iproto.SchemaMigrationStep) (resultErr error) {
	id, err := uuid.Parse(identity)
	if err != nil || id == uuid.Nil() || id.String() != identity || step == nil || step.GetSeq() == 0 || step.GetSeq() > maxSnapshotChunk {
		return fmt.Errorf("mysqlout: invalid migration identity")
	}
	if step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE && step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_SKIP {
		return fmt.Errorf("mysqlout: migration action not executable")
	}
	if dblineage.Reserved(step.GetSchema()) {
		return fmt.Errorf("mysqlout: reserved metadata cannot be migrated")
	}
	if err := w.ensureConnection(ctx); err != nil {
		return err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	if step.GetKind() == iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FOREIGN_KEY {
		return w.applyForeignKeyStep(ctx, identity, step)
	}
	kind, show, err := schemaObjectKind(step.GetKind())
	if err != nil {
		return err
	}
	parsedKind, schema, name, err := schemamig.CreationIdentity(step.GetSchema(), step.GetDdl())
	if err != nil || parsedKind != kind || schema != step.GetSchema() || name != step.GetObjectName() {
		return fmt.Errorf("mysqlout: migration statement identity mismatch")
	}
	if step.GetViewPlaceholder() {
		if step.GetKind() != iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW || step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE || step.GetViewReplacementSeq() <= step.GetSeq() || step.GetViewReplacementSeq() > maxSnapshotChunk || step.GetViewReplacementDdl() == "" || step.GetViewPlaceholderSeq() != 0 || step.GetViewPlaceholderDdl() != "" {
			return fmt.Errorf("mysqlout: invalid view placeholder successor")
		}
		finalKind, finalSchema, finalName, finalErr := schemamig.CreationIdentity(step.GetSchema(), step.GetViewReplacementDdl())
		replacement, replaceErr := schemamig.ViewIsReplacement(step.GetViewReplacementDdl())
		if finalErr != nil || replaceErr != nil || !replacement || finalKind != kind || finalSchema != schema || finalName != name {
			return fmt.Errorf("mysqlout: view successor identity mismatch")
		}
		set, err := w.GTIDSet(ctx)
		if err != nil {
			return err
		}
		actual, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
		if err != nil {
			return err
		}
		final, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, fmt.Sprintf("%s:%d", identity, step.GetViewReplacementSeq()))
		if actual.Contain(final) {
			return w.ApplySchemaStep(ctx, identity, &iproto.SchemaMigrationStep{Seq: step.GetViewReplacementSeq(), Kind: step.GetKind(), Schema: step.GetSchema(), ObjectName: step.GetObjectName(), Action: iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_SKIP, Ddl: step.GetViewReplacementDdl()})
		}
	} else if step.GetViewReplacementSeq() != 0 || step.GetViewReplacementDdl() != "" {
		return fmt.Errorf("mysqlout: view replacement metadata without placeholder")
	}
	if step.GetViewPlaceholderSeq() != 0 || step.GetViewPlaceholderDdl() != "" {
		if kind != schemamig.ObjectView || step.GetViewPlaceholder() || step.GetViewPlaceholderSeq() == 0 || step.GetViewPlaceholderSeq() >= step.GetSeq() || step.GetViewPlaceholderDdl() == "" || step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE {
			return fmt.Errorf("mysqlout: invalid view replacement predecessor")
		}
		priorKind, priorSchema, priorName, priorErr := schemamig.CreationIdentity(schema, step.GetViewPlaceholderDdl())
		priorReplacement, priorReplaceErr := schemamig.ViewIsReplacement(step.GetViewPlaceholderDdl())
		if priorErr != nil || priorReplaceErr != nil || priorReplacement || priorKind != kind || priorSchema != schema || priorName != name {
			return fmt.Errorf("mysqlout: view predecessor identity mismatch")
		}
	}
	if kind == schemamig.ObjectView && step.GetAction() == iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE {
		replacement, err := schemamig.ViewIsReplacement(step.GetDdl())
		if err != nil || replacement != (step.GetViewPlaceholderSeq() != 0) {
			return fmt.Errorf("mysqlout: replacement view requires plan-owned placeholder")
		}
	}
	if step.GetAction() == iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE {
		gtid := fmt.Sprintf("%s:%d", identity, step.GetSeq())
		set, err := w.GTIDSet(ctx)
		if err != nil {
			return err
		}
		executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
		if err != nil {
			return err
		}
		one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
		if !executed.Contain(one) {
			if step.GetViewPlaceholderSeq() != 0 {
				prior, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, fmt.Sprintf("%s:%d", identity, step.GetViewPlaceholderSeq()))
				if !executed.Contain(prior) {
					return fmt.Errorf("mysqlout: view placeholder GTID missing")
				}
				if err := w.ApplySchemaStep(ctx, identity, &iproto.SchemaMigrationStep{Seq: step.GetViewPlaceholderSeq(), Kind: step.GetKind(), Schema: schema, ObjectName: name, Action: iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_SKIP, Ddl: step.GetViewPlaceholderDdl()}); err != nil {
					return fmt.Errorf("mysqlout: view placeholder verification failed: %w", err)
				}
			}
			if show != "DATABASE" {
				if _, err := w.conn.ExecContext(ctx, "USE "+quoteIdent(step.GetSchema())); err != nil {
					return err
				}
			}
			if err := w.reserveDDLLineage(ctx, w.syntheticSourceTxn(gtid)); err != nil {
				return err
			}
			if err := w.executeDDL(ctx, gtid, step.GetDdl()); err != nil {
				return err
			}
		}
	}
	actual, err := w.schemaDefinition(ctx, show, step.GetSchema(), step.GetObjectName())
	if err != nil {
		return err
	}
	var diff []schemamig.Difference
	switch show {
	case "DATABASE":
		diff, err = schemamig.CompareDatabaseDefinitions(step.GetDdl(), actual)
	case "TABLE":
		var expectedBase, actualBase string
		actual, err = w.tableDefinitionForComparison(ctx, w.conn, step.GetSchema(), step.GetObjectName(), actual)
		if err != nil {
			return err
		}
		expectedBase, _, err = schemamig.SplitTableForeignKeys(step.GetSchema(), step.GetDdl())
		if err == nil {
			actualBase, _, err = schemamig.SplitTableForeignKeys(step.GetSchema(), actual)
		}
		if err == nil {
			diff, err = schemamig.CompareDefinitions(kind, expectedBase, actualBase)
		}
	default:
		diff, err = schemamig.CompareDefinitionsInSchema(kind, step.GetSchema(), step.GetDdl(), actual)
	}
	if err != nil {
		return fmt.Errorf("mysqlout: migration verification failed: %w", err)
	}
	if len(diff) != 0 {
		return fmt.Errorf("mysqlout: migration definition differs at %s", diff[0].Path)
	}
	return nil
}

func (w *Writer) schemaDefinition(ctx context.Context, kind, schema, name string) (string, error) {
	return w.schemaDefinitionWith(ctx, w.conn, kind, schema, name)
}

type iSchemaQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (w *Writer) tableDefinitionForComparison(ctx context.Context, query iSchemaQuery, schema, name, ddl string) (string, error) {
	var collation string
	if err := query.QueryRowContext(ctx, "SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE BINARY TABLE_SCHEMA=? AND BINARY TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", schema, name).Scan(&collation); err != nil {
		return "", err
	}
	return schemamig.WithTableCollation(ddl, collation)
}

func (w *Writer) schemaDefinitionWith(ctx context.Context, query iSchemaQuery, kind, schema, name string) (string, error) {
	qualified := quoteIdent(schema) + "." + quoteIdent(name)
	if kind == "DATABASE" {
		qualified = quoteIdent(schema)
	}
	rows, err := query.QueryContext(ctx, "SHOW CREATE "+kind+" "+qualified)
	if err != nil {
		return "", err
	}
	defer func(cleanup func() error) { _ = cleanup() }(rows.Close)
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("mysqlout: migration definition missing")
	}
	values := make([]sql.NullString, len(cols))
	dest := make([]any, len(cols))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := rows.Scan(dest...); err != nil {
		return "", err
	}
	for i, col := range cols {
		if strings.EqualFold(col, "Create "+kind) || kind == "TRIGGER" && strings.EqualFold(col, "SQL Original Statement") {
			if values[i].Valid && values[i].String != "" {
				return values[i].String, nil
			}
		}
	}
	return "", fmt.Errorf("mysqlout: migration definition unavailable")
}
