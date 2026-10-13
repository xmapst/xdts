package mysqlout

import (
	"context"
	"errors"
	"fmt"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/schemamig"
	"google.golang.org/protobuf/proto"
)

type IRecoveryStore interface {
	Get(context.Context, string, string, any) error
	PutCritical(context.Context, store.Op) error
}
type recoveryDeferred struct {
	Body []byte `bson:"body"`
}
type tableRecovery struct {
	store    IRecoveryStore
	prefix   string
	blocked  map[[2]string]bool
	isolated map[[2]string]bool
	pending  map[string]uint64
	recorded map[string]uint64
	count    uint64
	bytes    uint64
}

func (w *Writer) recoveryTableKey(schema, table string) [2]string {
	if !w.opts.SchemaOptions.SourceCaseSensitive {
		return [2]string{strings.ToLower(schema), strings.ToLower(table)}
	}
	return [2]string{schema, table}
}

// Called before the incremental workflow receives ownership of the Writer.
func (w *Writer) ConfigureTableRecovery(storage IRecoveryStore, pipeline string, run uint64) error {
	if storage == nil || pipeline == "" {
		return errors.New("mysqlout: recovery storage required")
	}
	w.recovery = &tableRecovery{store: storage, prefix: fmt.Sprintf("%d:%s:%d.recovery_deferred.", len(pipeline), pipeline, run)}
	return nil
}

func (w *Writer) SetRecoveryTables(ctx context.Context, tables []*iproto.SnapshotRecoveryTable) error {
	if w.recovery == nil {
		if len(tables) > 0 {
			return errors.New("mysqlout: recovery not negotiated")
		}
		return nil
	}
	if err := iproto.ValidateSnapshotRecoveryTables(tables, true); err != nil {
		return err
	}
	r := w.recovery
	r.blocked = map[[2]string]bool{}
	r.isolated = map[[2]string]bool{}
	r.pending = map[string]uint64{}
	r.recorded = map[string]uint64{}
	r.count = 0
	r.bytes = 0
	for _, table := range tables {
		key := w.recoveryTableKey(table.Schema, table.Table)
		r.blocked[key] = true
		r.isolated[key] = true
	}
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return err
	}
	executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	for index := uint64(1); ; index++ {
		var record recoveryDeferred
		err := r.store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s%d", r.prefix, index), &record)
		if errors.Is(err, xdoc.ErrNotFound) {
			break
		}
		if err != nil {
			return err
		}
		txn := new(iproto.Txn)
		if err := proto.Unmarshal(record.Body, txn); err != nil {
			return err
		}
		if !isSingleGTID(txn.GetGtid()) || iproto.ValidateTxn(txn) != nil {
			return errors.New("mysqlout: invalid stored deferred transaction")
		}
		r.count = index
		if _, exists := r.recorded[txn.Gtid]; exists {
			return errors.New("mysqlout: duplicate stored recovery transaction")
		}
		r.recorded[txn.Gtid] = index
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.Gtid)
		if err != nil {
			return err
		}
		if !executed.Contain(one) {
			r.bytes += uint64(len(record.Body))
			r.pending[txn.Gtid] = index
			for _, row := range txn.Rows {
				r.blocked[w.recoveryTableKey(row.Schema, row.Table)] = true
			}
			if ddl := txn.GetDdl(); ddl != nil {
				objects, _ := schemamig.DDLObjects(ddl.Schema, ddl.Statement)
				for _, object := range objects {
					r.blocked[w.recoveryTableKey(object.Schema, object.Table)] = true
				}
			}
		}
		if r.bytes > 512<<20 || index > 100000 {
			return errors.New("mysqlout: deferred recovery log exceeds bound")
		}
	}
	return nil
}

func (w *Writer) deferRecoveryTxn(ctx context.Context, txn *iproto.Txn) (bool, error) {
	r := w.recovery
	if r == nil || len(r.blocked) == 0 {
		return false, nil
	}
	if !isSingleGTID(txn.GetGtid()) || iproto.ValidateTxn(txn) != nil {
		return false, errors.New("mysqlout: invalid recovery transaction")
	}
	blocked := false
	if ddl := txn.GetDdl(); ddl != nil {
		// DDL is durably retained until all earlier dependent transactions can
		// commit. It is replayed under its native GTID before cleanup begins.
		objects, known := schemamig.DDLObjects(ddl.Schema, ddl.Statement)
		blocked = !known || len(objects) == 0
		for _, object := range objects {
			blocked = blocked || r.blocked[w.recoveryTableKey(object.Schema, object.Table)]
		}
	}
	for _, row := range txn.Rows {
		blocked = blocked || r.blocked[w.recoveryTableKey(row.Schema, row.Table)]
	}
	if !blocked {
		return false, nil
	}
	if _, exists := r.pending[txn.Gtid]; exists {
		return true, nil
	}
	if _, exists := r.recorded[txn.Gtid]; exists {
		// The durable record was already committed under its native GTID.
		// Normal apply verifies dedup; never append it again on source replay.
		return false, nil
	}
	body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(txn)
	if err != nil {
		return false, err
	}
	if r.bytes+uint64(len(body)) > 512<<20 || r.count >= 100000 {
		return false, errors.New("mysqlout: deferred recovery budget exhausted")
	}
	index := r.count + 1
	id := fmt.Sprintf("%s%d", r.prefix, index)
	err = r.store.PutCritical(ctx, store.Op{Collection: "snapshot_targets", ID: id, Doc: recoveryDeferred{Body: body}, InsertOnly: true})
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return false, err
	}
	var actual recoveryDeferred
	if err := r.store.Get(ctx, "snapshot_targets", id, &actual); err != nil {
		return false, err
	}
	check := new(iproto.Txn)
	if err := proto.Unmarshal(actual.Body, check); err != nil || !proto.Equal(check, txn) {
		return false, errors.New("mysqlout: deferred transaction identity changed")
	}
	r.count = index
	r.bytes += uint64(len(body))
	r.pending[txn.Gtid] = index
	if r.recorded == nil {
		r.recorded = map[string]uint64{}
	}
	r.recorded[txn.Gtid] = index
	for _, row := range txn.Rows {
		r.blocked[w.recoveryTableKey(row.Schema, row.Table)] = true
	}
	if ddl := txn.GetDdl(); ddl != nil {
		objects, _ := schemamig.DDLObjects(ddl.Schema, ddl.Statement)
		for _, object := range objects {
			r.blocked[w.recoveryTableKey(object.Schema, object.Table)] = true
		}
	}
	return true, nil
}

// The incremental owner serializes this flush before permitting cleanup. It
// commits every original cross-table transaction intact and in source order.
func (w *Writer) FlushRecoveryTransactions(ctx context.Context, through string) (iproto.BatchApplyResult, error) {
	var result iproto.BatchApplyResult
	if w.recovery == nil {
		return result, errors.New("mysqlout: recovery not negotiated")
	}
	limit, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, through)
	if err != nil {
		return result, err
	}
	for index := uint64(1); index <= w.recovery.count; index++ {
		var record recoveryDeferred
		if err := w.recovery.store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s%d", w.recovery.prefix, index), &record); err != nil {
			return result, err
		}
		txn := new(iproto.Txn)
		if err := proto.Unmarshal(record.Body, txn); err != nil {
			return result, err
		}
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.Gtid)
		if err != nil {
			return result, err
		}
		if !limit.Contain(one) {
			continue
		}
		applied, err := w.applyTxnResult(ctx, txn)
		if err != nil {
			return result, err
		}
		if applied {
			result.AppliedGTIDs = append(result.AppliedGTIDs, txn.Gtid)
		} else {
			return result, errors.New("mysqlout: deferred recovery transaction was rejected")
		}
	}
	return result, nil
}

// After a completed replacement is read from disk, every retained transaction
// was acknowledged before that replacement. Replay the entire journal, even
// if a source watermark overshot the replacement's H before it paused SQL.
func (w *Writer) FlushCompletedRecoveryTransactions(ctx context.Context) (iproto.BatchApplyResult, error) {
	if w.recovery == nil {
		return iproto.BatchApplyResult{}, errors.New("mysqlout: recovery not negotiated")
	}
	set, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	for gtid := range w.recovery.pending {
		if err := set.Update(gtid); err != nil {
			return iproto.BatchApplyResult{}, err
		}
	}
	return w.FlushRecoveryTransactions(ctx, set.String())
}

// Cleanup has a separate synthetic GNO range from chunks. The GTID itself is
// the authoritative idempotence proof even if the local completion write fails.
func (w *Writer) PrepareRecoveryTable(ctx context.Context, identity *iproto.SnapshotRecoveryIdentity, table *iproto.SnapshotRecoveryTable, index uint64) (resultErr error) {
	if index >= 4000 {
		return errors.New("mysqlout: recovery cleanup index outside range")
	}
	if err := iproto.ValidateSnapshotRecoveryIdentity(identity); err != nil {
		return err
	}
	if err := iproto.ValidateSnapshotRecoveryTables([]*iproto.SnapshotRecoveryTable{table}, false); err != nil {
		return err
	}
	mapped := w.mapSnapshot(&iproto.SnapshotChunk{Schema: table.Schema, Table: table.Table})
	if err := w.ensureConnection(ctx); err != nil {
		return err
	}
	defer func() { w.observeConnectionError(resultErr) }()
	gtid := fmt.Sprintf("%s:%d", identity.Uuid, maxSnapshotChunk-index)
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return err
	}
	actual, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
	if err != nil {
		return err
	}
	if actual.Contain(one) {
		return nil
	}
	var ddlName, ddl string
	if err := w.conn.QueryRowContext(ctx, "SHOW CREATE TABLE "+quoteIdent(mapped.Schema)+"."+quoteIdent(mapped.Table)).Scan(&ddlName, &ddl); err != nil {
		return err
	}
	plan := &iproto.ReinitTargetPlan{Handling: iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_KEEP, Tables: []*iproto.ReinitTargetTable{{Schema: mapped.Schema, Table: mapped.Table, ExpectedDdl: ddl}}}
	if err := w.ApplyReinitPlan(ctx, plan, nil); err != nil {
		return err
	}
	if _, err := w.conn.ExecContext(ctx, "SET SESSION foreign_key_checks=0"); err != nil {
		return err
	}
	return withGTIDTxn(ctx, w.conn, gtid, func(ctx context.Context, tx *dmlTxn) error {
		// Replacement is copied from the source. Carry source-to-target
		// lineage so a reverse pipeline cannot replay this DELETE at source.
		if err := w.writeTxnLineage(ctx, tx, w.syntheticSourceTxn(gtid)); err != nil {
			return err
		}
		rows, err := tx.tx.QueryContext(ctx, "SELECT 1 FROM "+quoteIdent(mapped.Schema)+"."+quoteIdent(mapped.Table)+" LIMIT 0")
		if err != nil {
			return err
		}
		if err := rows.Close(); err != nil {
			return err
		}
		grants, err := w.reinitTriggerGrants(ctx, tx.tx)
		if err != nil {
			return err
		}
		if _, err := w.verifyReinitTable(ctx, tx.tx, plan.Tables[0], nil, 0, true, grants); err != nil {
			return err
		}
		_, err = tx.execDML(ctx, "DELETE FROM "+quoteIdent(mapped.Schema)+"."+quoteIdent(mapped.Table))
		return err
	})
}
