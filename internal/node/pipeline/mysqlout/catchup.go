package mysqlout

import (
	"context"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

func (w *Writer) BeginCatchup(high string) error {
	if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, high); err != nil {
		return err
	}
	w.catchingUp = true
	return nil
}

// Only the single incremental handler calls this, after its ordered applied
// position reaches H. Restore the configured session on the next DB operation.
func (w *Writer) EndCatchup() { w.catchingUp = false; w.restoreForeignKeys = true }

func (w *Writer) applyCatchupTxn(ctx context.Context, txn *iproto.Txn) error {
	if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
		return err
	}
	return withGTIDTxn(ctx, w.conn, txn.GetGtid(), func(ctx context.Context, tx *dmlTxn) error {
		if err := w.writeTxnLineage(ctx, tx, txn); err != nil {
			return err
		}
		var err error
		txn, err = w.projectTargetTxn(ctx, tx, txn)
		if err != nil {
			return err
		}
		for _, row := range txn.GetRows() {
			if err := w.validateTargetColumns(ctx, row.GetSchema(), row.GetTable(), row.GetColumnNames()); err != nil {
				return err
			}
		}
		txn, err = w.prepareTransformTxn(ctx, tx.tx, txn)
		if err != nil {
			return err
		}
		for _, row := range txn.GetRows() {
			switch row.GetType() {
			case iproto.ChangeType_CHANGE_TYPE_DELETE, iproto.ChangeType_CHANGE_TYPE_UPDATE:
				query, args, err := buildDeleteSourceOverride(row.GetSchema(), row.GetTable(), row.GetColumnNames(), row.GetBefore(), row.GetPrimaryKeyColumns())
				if err != nil {
					return err
				}
				if _, err := tx.execDML(ctx, query, args...); err != nil {
					return err
				}
			case iproto.ChangeType_CHANGE_TYPE_INSERT:
			default:
				return fmt.Errorf("mysqlout: unsupported catchup row change")
			}
			if row.GetType() != iproto.ChangeType_CHANGE_TYPE_DELETE {
				query, args, err := buildReplace(row.GetSchema(), row.GetTable(), row.GetColumnNames(), []*iproto.Row{row.GetAfter()})
				if err != nil {
					return err
				}
				if _, err := tx.execDML(ctx, query, args...); err != nil {
					return err
				}
			}
		}
		return nil
	})
}
