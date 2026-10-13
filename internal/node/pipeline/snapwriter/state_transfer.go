package snapwriter

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

// ValidateTransferState checks archived completion before ownership changes.
// The recipient still verifies the actual target GTID prefix before resuming.
func ValidateTransferState(records []store.StateRecord, pipelineID string, run uint64, uuid string, extensions bool, recovery ...bool) error {
	allowed := len(recovery) == 1 && recovery[0]
	m := &Module{opts: Options{Run: mysqlout.SnapshotOptions{PipelineID: pipelineID, RunID: run, UUID: uuid}, AllowPlanExtensions: extensions, AllowTableRecovery: allowed}}
	seen := map[string]bool{}
	generations := map[uint64]recoveryRecord{}
	var original *iproto.SnapshotEnd
	for _, record := range records {
		if record.Collection != "snapshot_targets" {
			continue
		}
		if seen[record.ID] {
			return errors.New("snapwriter: duplicate transferred recovery document")
		}
		seen[record.ID] = true
		if strings.HasPrefix(record.ID, m.recordID()+".recovery_deferred.") {
			if !allowed {
				return errors.New("snapwriter: transferred recovery not negotiated")
			}
			continue
		}
		doc, err := xdoc.DecodeDocument(record.Data)
		if err != nil {
			return err
		}
		if suffix, ok := strings.CutPrefix(record.ID, m.recordID()+".recovery."); ok {
			sequence, err := strconv.ParseUint(suffix, 10, 64)
			if !allowed || err != nil || sequence == 0 || sequence > 4096 || suffix != strconv.FormatUint(sequence, 10) {
				return errors.New("snapwriter: invalid transferred recovery sequence")
			}
			var saved recoveryRecord
			if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
				return err
			}
			generations[sequence] = saved
			continue
		}
		var saved completion
		if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
			return err
		}
		if record.ID != m.recordID() || saved.UUID != uuid {
			return errors.New("snapwriter: transfer completion identity differs")
		}
		if err := m.validateEnd(saved.end()); err != nil {
			return err
		}
		original = saved.end()
	}
	if err := mysqlout.ValidateRecoveryTransferState(records, pipelineID, run, allowed); err != nil {
		return err
	}
	if len(generations) == 0 {
		return nil
	}
	if original == nil {
		return errors.New("snapwriter: recovery has no original completion")
	}
	previous := original
	uuids := map[string]bool{uuid: true}
	for sequence := uint64(1); sequence <= uint64(len(generations)); sequence++ {
		record, exists := generations[sequence]
		if !exists {
			return errors.New("snapwriter: recovery generations have gaps")
		}
		begin := new(iproto.SnapshotRecoveryBegin)
		if err := proto.Unmarshal(record.Begin, begin); err != nil {
			return err
		}
		if iproto.ValidateSnapshotRecoveryBegin(begin) != nil || begin.PipelineId != pipelineID || begin.RunId != run || begin.Recovery.Sequence != sequence || uuids[begin.Recovery.Uuid] || len(begin.Tables) != len(previous.RecoveryTables) {
			return errors.New("snapwriter: transferred recovery authority differs")
		}
		for i, table := range begin.Tables {
			if !proto.Equal(table, previous.RecoveryTables[i]) {
				return errors.New("snapwriter: transferred recovery scope differs")
			}
		}
		low, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, begin.LowGtidSet)
		high, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, previous.HighGtidSet)
		if !low.Contain(high) {
			return errors.New("snapwriter: recovery low precedes previous high")
		}
		uuids[begin.Recovery.Uuid] = true
		if len(record.End) == 0 {
			if sequence != uint64(len(generations)) {
				return errors.New("snapwriter: recovery continued before previous completion")
			}
			return nil
		}
		end := new(iproto.SnapshotEnd)
		if err := proto.Unmarshal(record.End, end); err != nil {
			return err
		}
		if end.PipelineId != pipelineID || end.RunId != run || !proto.Equal(end.Recovery, begin.Recovery) || end.LowGtidSet != begin.LowGtidSet || end.TotalChunks == 0 || end.TotalChunks >= 1<<63-4002 || end.InitialTotalChunks != 0 || end.PlanExtensionSequence != 0 || iproto.ValidateSnapshotRecoveryTables(end.RecoveryTables, true) != nil {
			return fmt.Errorf("snapwriter: recovery %d completion differs", sequence)
		}
		if err := validateEndWatermarks(end); err != nil {
			return err
		}
		previous = end
	}
	return nil
}
