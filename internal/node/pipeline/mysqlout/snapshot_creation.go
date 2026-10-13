package mysqlout

import (
	"context"
	"errors"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

// Native GTID ownership serializes concurrent CREATE attempts from independent
// snapshot workers. Success is authoritative only after the original GTID exists.
func (w *Writer) prepareSnapshotCreation(ctx context.Context, creation *iproto.Txn) error {
	if !w.opts.DatabaseRoute.AllowsTxn(creation) {
		return fmt.Errorf("mysqlout: snapshot CREATE lineage rejected")
	}
	_, applyErr := w.applyTxnResult(ctx, creation)
	if _, ok := errors.AsType[sessionRestoreError](applyErr); ok {
		return applyErr
	}
	set, err := w.GTIDSet(ctx)
	if err != nil {
		return err
	}
	actual, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, creation.GetGtid())
	if err != nil {
		return err
	}
	if actual.Contain(one) {
		return nil
	}
	if applyErr != nil {
		return applyErr
	}
	return fmt.Errorf("mysqlout: snapshot CREATE policy did not commit original GTID")
}
