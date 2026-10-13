package mysqlout

import (
	"context"
	"database/sql"
	"errors"
	"reflect"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
)

func (w *Writer) lineageRecord(txn *iproto.Txn) (iproto.StoredDatabaseLineage, []byte, error) {
	record, err := iproto.CaptureTargetLineage(txn, w.opts.DatabaseRoute)
	if err != nil {
		return record, nil, err
	}
	body, err := record.Encode()
	return record, body, err
}

func (w *Writer) syntheticSourceTxn(gtid string) *iproto.Txn {
	route := w.opts.DatabaseRoute
	return &iproto.Txn{Gtid: gtid, OriginCenter: route.SourceCenter, Path: []string{route.SourceCenter}, EndpointPath: []string{route.SourceServerUUID}, Hops: 1, LineageVersion: iproto.StoredLineageVersion}
}

// REINIT cleanup originates locally on this physical target. It starts a new
// synthetic lineage, rather than claiming visits inherited from copied rows.
func (w *Writer) writeCleanupLineage(ctx context.Context, tx *dmlTxn, gtid string) error {
	if !w.opts.DatabaseRoute.Persistent {
		return nil
	}
	route := w.opts.DatabaseRoute
	record := iproto.StoredDatabaseLineage{Version: iproto.StoredLineageVersion, GTID: gtid, OriginCenter: route.TargetCenter, Path: []string{route.TargetCenter}, EndpointPath: []string{route.TargetServerUUID}, Hops: 1, Complete: true}
	body, err := record.Encode()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, dblineage.Timeout)
	defer cancel()
	_, err = tx.execLineageInsert(ctx, record.GTID, body)
	return err
}

// Only normal DML stages the first INSERT for its mandatory projection query.
// Snapshot, catchup and cleanup keep their immediately executed operation.
type stagedTxnLineage struct {
	gtid string
	body []byte
}

func (w *Writer) stageTxnLineage(tx *dmlTxn, txn *iproto.Txn) error {
	if !w.opts.DatabaseRoute.Persistent {
		return nil
	}
	record, body, err := w.lineageRecord(txn)
	if err != nil {
		return err
	}
	tx.pendingLineage = &stagedTxnLineage{gtid: record.GTID, body: body}
	return nil
}

// This must be the first write in the original GTID transaction, so a cascade
// reader sees authoritative metadata before deciding whether to decode rows.
func (w *Writer) writeTxnLineage(ctx context.Context, tx *dmlTxn, txn *iproto.Txn) error {
	if !w.opts.DatabaseRoute.Persistent {
		return nil
	}
	record, body, err := w.lineageRecord(txn)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, dblineage.Timeout)
	defer cancel()
	_, err = tx.execLineageInsert(ctx, record.GTID, body)
	return err
}

// DDL commits implicitly. Reserve its lineage under an automatic GTID first;
// the original DDL GTID is the proof that the business operation committed.
// A crash before DDL leaves only an inert record, safely reusable on retry.
func (w *Writer) reserveDDLLineage(ctx context.Context, txn *iproto.Txn) error {
	if !w.opts.DatabaseRoute.Persistent {
		return nil
	}
	record, body, err := w.lineageRecord(txn)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, dblineage.Timeout)
	defer cancel()
	_, err = w.conn.ExecContext(ctx, dblineage.Insert, record.GTID, body)
	if err == nil {
		return nil
	}
	var sqlErr *mysql.MySQLError
	if !errors.As(err, &sqlErr) || sqlErr.Number != 1062 {
		return err
	}
	existing, found, loadErr := dblineage.New(w.conn).Load(ctx, record.GTID)
	if loadErr != nil {
		return loadErr
	}
	if !found || !reflect.DeepEqual(existing, record) {
		return errors.New("mysqlout: DDL lineage reservation differs from retry")
	}
	return nil
}

// This fixed INSERT runs once per original GTID. It uses the existing driver's
// parameter interpolation without retaining a statement that cannot be reused.
// Accept values only; callers cannot supply transaction-control SQL.
func (tx *dmlTxn) execLineageInsert(ctx context.Context, gtid string, body []byte) (sql.Result, error) {
	return tx.tx.ExecContext(ctx, dblineage.Insert, gtid, body)
}
