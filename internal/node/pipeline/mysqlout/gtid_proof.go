package mysqlout

import (
	"context"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
)

// HasAppliedGTIDs runs in the writer owner after all replay workers have joined.
// It proves complete native transactions, independently of observed positions.
func (w *Writer) HasAppliedGTIDs(ctx context.Context, gtids []string) (bool, error) {
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, "")
	if err != nil {
		return false, err
	}
	for _, gtid := range gtids {
		if err := set.Update(gtid); err != nil {
			return false, err
		}
	}
	if len(gtids) == 0 {
		return false, nil
	}
	if err := w.ensureConnection(ctx); err != nil {
		return false, err
	}
	var contained int
	err = w.conn.QueryRowContext(ctx, "SELECT GTID_SUBSET(?, @@GLOBAL.gtid_executed)", set.String()).Scan(&contained)
	if err != nil {
		w.observeConnectionError(err)
		return false, err
	}
	return contained == 1, nil
}
