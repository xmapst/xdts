package mysqlout

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

func (w *Writer) applyForeignKeyStep(ctx context.Context, identity string, step *iproto.SchemaMigrationStep) (resultErr error) {
	expected, err := schemamig.ForeignKeyIdentity(step.GetSchema(), step.GetDdl())
	if err != nil || expected.Schema != step.GetSchema() || expected.Name != step.GetObjectName() || expected.Table != step.GetTableName() || !step.GetAfterSnapshot() {
		return fmt.Errorf("mysqlout: invalid foreign key step identity")
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
			if _, err := w.conn.ExecContext(ctx, "USE "+quoteIdent(step.GetSchema())); err != nil {
				return err
			}
			// ALTER with checks enabled validates existing rows. It never silently
			// installs a constraint over orphan data created during the snapshot.
			if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 1"); err != nil {
				return err
			}
			defer func() {
				restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connectTimeout)
				defer cancel()
				_, err := w.conn.ExecContext(restoreCtx, "SET SESSION foreign_key_checks = ?", w.opts.foreignKeyChecksValue())
				if err != nil {
					resultErr = errors.Join(resultErr, fmt.Errorf("mysqlout: restore foreign key checks failed: %w", sessionRestoreError{err}))
				}
			}()
			if err := w.reserveDDLLineage(ctx, w.syntheticSourceTxn(gtid)); err != nil {
				return err
			}
			if err := w.executeDDL(ctx, gtid, step.GetDdl()); err != nil {
				return err
			}
		}
	}
	actualDDL, err := w.schemaDefinition(ctx, "TABLE", step.GetSchema(), step.GetTableName())
	if err != nil {
		return err
	}
	_, keys, err := schemamig.SplitTableForeignKeys(step.GetSchema(), actualDDL)
	if err != nil {
		return err
	}
	for _, actual := range keys {
		if strings.EqualFold(actual.Name, expected.Name) {
			if len(schemamig.CompareForeignKeys(expected, actual)) != 0 {
				return fmt.Errorf("mysqlout: foreign key definition mismatch")
			}
			return nil
		}
	}
	return fmt.Errorf("mysqlout: foreign key definition missing")
}
