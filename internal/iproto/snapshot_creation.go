package iproto

import (
	"errors"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/schemamig"
	"google.golang.org/protobuf/proto"
)

// ValidateSnapshotCreation binds a complete original DDL transaction to the
// physical source table. Destination mapping remains the Writer's authority.
func ValidateSnapshotCreation(txn *Txn, schema, table string) error {
	if txn == nil {
		return nil
	}
	if txn.GetDdl() == nil || len(txn.GetRows()) != 0 || proto.Size(txn) > 1<<20 {
		return errors.New("iproto: invalid snapshot table creation transaction")
	}
	if err := ValidateTxn(txn); err != nil {
		return err
	}
	one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.GetGtid())
	if err != nil || !singleSnapshotCreationGTID(one) {
		return errors.New("iproto: snapshot creation requires one original GTID")
	}
	object, ok := schemamig.SnapshotCreation(txn.GetDdl().GetSchema(), txn.GetDdl().GetStatement())
	equal := strings.EqualFold
	if txn.GetDdl().GetColumnPolicy().GetSourceCaseSensitive() {
		equal = func(a, b string) bool { return a == b }
	}
	if !ok || !equal(object.Schema, schema) || !equal(object.Table, table) {
		return errors.New("iproto: snapshot creation table does not match chunk")
	}
	return nil
}

func singleSnapshotCreationGTID(set gmysql.GTIDSet) bool {
	parsed, ok := set.(*gmysql.MysqlGTIDSet)
	if !ok || len(*parsed) != 1 {
		return false
	}
	for _, tags := range *parsed {
		if len(tags) != 1 {
			return false
		}
		for _, intervals := range tags {
			return len(intervals) == 1 && intervals[0].Stop-intervals[0].Start == 1 && intervals[0].Start > 0
		}
	}
	return false
}
