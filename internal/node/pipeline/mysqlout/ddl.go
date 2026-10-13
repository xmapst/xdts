package mysqlout

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
)

// applyDDL is called only by the ordered consumer, after all previous DML has
// committed. DDL executes without BeginTx: its implicit commit owns the GTID.
func (w *Writer) applyDDL(ctx context.Context, txn *iproto.Txn) (bool, error) {
	gtid := txn.GetGtid()
	if !isSingleGTID(gtid) {
		return false, fmt.Errorf("mysqlout: invalid DDL GTID")
	}
	seq, err := strconv.ParseUint(gtid[strings.LastIndexByte(gtid, ':')+1:], 10, 64)
	if err != nil || seq == 0 || seq > maxSnapshotChunk {
		return false, fmt.Errorf("mysqlout: invalid DDL transaction sequence")
	}
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return false, err
	}
	executed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return false, err
	}
	one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
	if err != nil {
		return false, err
	}
	if executed.Contain(one) {
		return true, nil
	}
	policy := w.opts.DDLPolicy
	switch policy.GetMode() {
	case iproto.DDLMode_DDL_MODE_IGNORE:
		return false, nil // Ignore has no target side effect, including no empty GTID transaction.
	case iproto.DDLMode_DDL_MODE_ONLY_FROM_CENTERS:
		if !slices.Contains(policy.GetOnlyFromCenters(), txn.GetOriginCenter()) {
			return false, nil
		}
	case iproto.DDLMode_DDL_MODE_UNSPECIFIED, iproto.DDLMode_DDL_MODE_SYNC:
	default:
		return false, fmt.Errorf("mysqlout: invalid DDL policy")
	}
	ddl := txn.GetDdl()
	if err := iproto.ValidateDDLSourceDependencies(ddl); err != nil {
		return false, err
	}
	if len(ddl.GetSourceDependencies()) > 0 && !w.opts.DDLSourceDependencies {
		return false, fmt.Errorf("mysqlout: original DDL dependencies not negotiated")
	}
	mapping := w.opts.Mapping
	options, err := w.incrementalSchemaOptions(ctx, ddl.GetSchema(), ddl.GetStatement())
	if err != nil {
		return false, err
	}
	if policy := ddl.GetColumnPolicy(); policy != nil {
		if len(policy.GetRules()) != 0 {
			mapping.ColumnFilter, err = iproto.CompileColumnFilters(policy.GetRules(), policy.GetSourceCaseSensitive())
			if err != nil {
				return false, err
			}
		}
		options.SourceCaseSensitive = policy.GetSourceCaseSensitive()
	}
	dependencies := make(map[schemamig.SchemaTable]schemamig.TableDependencies, len(ddl.GetSourceDependencies()))
	for _, value := range ddl.GetSourceDependencies() {
		dependencies[schemamig.SchemaTable{Schema: value.GetSchema(), Table: value.GetTable()}] = iproto.OriginalDependencies(value)
	}
	r, err := schemamig.RewriteIncremental(schemamig.IncrementalRequest{Schema: ddl.GetSchema(), DDL: ddl.GetStatement(), Mapping: mapping, Options: options, SourceDependencies: dependencies})
	if err != nil {
		return false, err
	}
	if r.Blocked {
		return false, fmt.Errorf("mysqlout: DDL rewrite blocked: %s: %w", r.Reason, errDDLNotSupported)
	}
	if r.Drop && !policy.GetAllowDrop() {
		return false, fmt.Errorf("mysqlout: destructive DROP requires allow_drop")
	}
	if r.Truncate && !policy.GetAllowTruncate() {
		return false, fmt.Errorf("mysqlout: TRUNCATE requires allow_truncate")
	}
	if r.Noop {
		return false, nil
	}
	objects, known := schemamig.DDLObjects(r.Schema, r.DDL)
	if !known && dblineage.Reserved(r.Schema) {
		return false, fmt.Errorf("mysqlout: reserved metadata DDL denied")
	}
	for _, object := range objects {
		if dblineage.Reserved(object.Schema) {
			return false, fmt.Errorf("mysqlout: reserved metadata DDL denied")
		}
	}
	if r.UsesDefaultSchema && r.Schema != "" {
		if _, err := w.conn.ExecContext(ctx, "USE "+quoteIdent(r.Schema)); err != nil {
			return false, err
		}
	}
	if err := w.reserveDDLLineage(ctx, txn); err != nil {
		return false, err
	}
	err = w.executeDDL(ctx, gtid, r.DDL)
	return err == nil, err
}

// executeDDL never exposes the DML transaction wrapper. Always restore the
// session with an independent timeout, even when the caller cancels the DDL.
func (w *Writer) executeDDL(ctx context.Context, gtid, statement string) (resultErr error) {
	if _, err := w.conn.ExecContext(ctx, "SET gtid_next = ?", gtid); err != nil {
		return err
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), connectTimeout)
		defer cancel()
		_, err := w.conn.ExecContext(rctx, "SET gtid_next = 'AUTOMATIC'")
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("mysqlout: restore DDL gtid_next failed: %w", sessionRestoreError{err}))
		}
	}()
	_, resultErr = w.conn.ExecContext(ctx, statement)
	return resultErr
}

func validateDDLPolicy(policy *iproto.DDLPolicy) error {
	return iproto.ValidateDDLPolicy(policy)
}
