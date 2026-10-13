package mysqlout

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

// ValidateRecoveryTransferState checks the durable transaction journal without
// SQL access. Native GTIDs remain the recipient's authority for committed data.
func ValidateRecoveryTransferState(records []store.StateRecord, pipeline string, run uint64, allowed bool) error {
	prefix := fmt.Sprintf("%d:%s:%d.recovery_deferred.", len(pipeline), pipeline, run)
	indices := map[uint64]bool{}
	gtids := map[string]bool{}
	var bytes uint64
	for _, record := range records {
		if record.Collection != "snapshot_targets" {
			continue
		}
		suffix, match := strings.CutPrefix(record.ID, prefix)
		if !match {
			continue
		}
		index, err := strconv.ParseUint(suffix, 10, 64)
		if !allowed || err != nil || index == 0 || index > 100000 || suffix != strconv.FormatUint(index, 10) || indices[index] {
			return errors.New("mysqlout: invalid transferred recovery journal sequence or capability")
		}
		doc, err := xdoc.DecodeDocument(record.Data)
		if err != nil {
			return err
		}
		var saved recoveryDeferred
		if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
			return err
		}
		txn := new(iproto.Txn)
		if proto.Unmarshal(saved.Body, txn) != nil || !isSingleGTID(txn.Gtid) || iproto.ValidateTxn(txn) != nil || gtids[txn.Gtid] {
			return errors.New("mysqlout: invalid transferred recovery transaction")
		}
		indices[index], gtids[txn.Gtid] = true, true
		bytes += uint64(len(saved.Body))
		if bytes > 512<<20 {
			return errors.New("mysqlout: transferred recovery journal exceeds bound")
		}
	}
	for index := uint64(1); index <= uint64(len(indices)); index++ {
		if !indices[index] {
			return errors.New("mysqlout: transferred recovery journal has gaps")
		}
	}
	return nil
}
