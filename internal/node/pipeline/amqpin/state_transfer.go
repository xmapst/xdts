package amqpin

import (
	"errors"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

// ValidateTransferState validates durable replay decisions without consuming
// messages, acknowledging deliveries or assembling an SQL writer.
func ValidateTransferState(records []store.StateRecord, pipelineID string, run uint64) error {
	base := fmt.Sprintf("%d:%s:%d", len(pipelineID), pipelineID, run)
	for _, record := range records {
		if record.Collection != "publication_window_cursors" && record.Collection != "skipped_transactions" && record.Collection != "skipped_transaction_sets" {
			continue
		}
		doc, err := xdoc.DecodeDocument(record.Data)
		if err != nil {
			return err
		}
		invalid := errors.New("amqpin: invalid transferred replay decision")
		switch record.Collection {
		case "publication_window_cursors":
			var cursor publicationCursor
			if err := xdoc.UnmarshalValue(doc.Value(), &cursor); err != nil {
				return err
			}
			if cursor.WindowTimeoutMS < 0 || cursor.WindowTimeoutMS > 10*60*1000 {
				return invalid
			}
			limits, err := iproto.NormalizePublicationWindow(&iproto.IncrementalWindowPolicy{MaxBatches: cursor.WindowBatches, MaxBytes: cursor.WindowBytes, ReorderTimeoutMs: uint32(cursor.WindowTimeoutMS)})
			if err != nil || cursor.PipelineID != pipelineID || cursor.RunID != run || cursor.Epoch == 0 || cursor.Epoch >= 1<<63 || cursor.First == 0 || cursor.First >= 1<<63 || cursor.Last < cursor.First-1 || cursor.Last >= 1<<63 || cursor.WindowBatches != uint32(limits.Batches) || cursor.WindowBytes != limits.Bytes || cursor.WindowTimeoutMS != limits.Timeout.Milliseconds() || record.ID != fmt.Sprintf("%s:%d:%d", base, cursor.Epoch, cursor.First) {
				return invalid
			}
			if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, cursor.GTIDs); err != nil || cursor.Last >= cursor.First && cursor.GTIDs == "" {
				return invalid
			}
		case "skipped_transaction_sets":
			var saved skippedTransactions
			if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
				return err
			}
			if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, saved.GTIDs); err != nil || saved.GTIDs == "" || saved.PipelineID != pipelineID || saved.RunID != run || record.ID != base {
				return invalid
			}
		case "skipped_transactions":
			var saved skippedTransaction
			if err := xdoc.UnmarshalValue(doc.Value(), &saved); err != nil {
				return err
			}
			if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, saved.GTID); err != nil || saved.GTID == "" || saved.PipelineID != pipelineID || saved.RunID != run || saved.ConfigVersion == 0 || saved.ConfigVersion >= 1<<63 || saved.TsMS <= 0 || record.ID != base+":"+saved.GTID {
				return invalid
			}
		}
	}
	return nil
}
