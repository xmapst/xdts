package amqpin

import (
	"context"
	"errors"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/store"
)

type skippedTransactions struct {
	PipelineID string `bson:"pipeline_id"`
	RunID      uint64 `bson:"run_id"`
	GTIDs      string `bson:"gtids"`
}
type skippedTransaction struct {
	PipelineID    string `bson:"pipeline_id"`
	RunID         uint64 `bson:"run_id"`
	ConfigVersion uint64 `bson:"config_version"`
	GTID          string `bson:"gtid"`
	TsMS          int64  `bson:"ts_ms"`
	Reason        string `bson:"reason"`
}

func (m *Module) skippedRecordID() string {
	return fmt.Sprintf("%d:%s:%d", len(m.opts.PipelineID), m.opts.PipelineID, m.opts.RunID)
}

func (m *Module) loadSkippedTransactions() error {
	if m.opts.Store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(m.shutdownCtx, m.opts.ApplyAttemptTimeout)
	defer cancel()
	var record skippedTransactions
	err := m.opts.Store.Get(ctx, "skipped_transaction_sets", m.skippedRecordID(), &record)
	if errors.Is(err, xdoc.ErrNotFound) {
		return nil
	}
	if err != nil {
		return errors.New("amqpin: skipped transaction state unavailable")
	}
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, record.GTIDs)
	if err != nil || record.GTIDs == "" || record.PipelineID != m.opts.PipelineID || record.RunID != m.opts.RunID {
		return errors.New("amqpin: invalid skipped transaction state")
	}
	m.skippedSet = set
	m.appliedSet = withoutRejected(m.appliedSet, set)
	m.publishAppliedSnapshot()
	_ = m.rejectedSet.Update(set.String())
	rejected := m.rejectedSet.String()
	m.stats.rejectedGTIDSet.Store(&rejected)
	return nil
}

// The durable decision precedes acknowledgment. It survives run-local consumer
// reconstruction independently of throttled observation checkpoints.
func (m *Module) persistSkippedTransaction(gtid string) error {
	writer, ok := m.opts.Store.(iCriticalAsyncStore)
	if !ok {
		return errors.New("amqpin: skipping requires durable state")
	}
	base := m.shutdownCtx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithTimeout(base, m.opts.ApplyAttemptTimeout)
	defer cancel()
	next := emptyGTIDSet()
	if m.skippedSet != nil {
		next = m.skippedSet.Clone()
	}
	if err := next.Update(gtid); err != nil {
		return err
	}
	record := skippedTransaction{PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), GTID: gtid, TsMS: m.now().UnixMilli(), Reason: "explicit error policy skipped rolled back SQL transaction"}
	if err := m.persistSkipOp(ctx, writer, store.Op{Collection: "skipped_transactions", ID: m.skippedRecordID() + ":" + gtid, Doc: record, Critical: true}); err != nil {
		return errors.New("amqpin: cannot persist skipped transaction audit")
	}
	if err := m.persistSkipOp(ctx, writer, store.Op{Collection: "skipped_transaction_sets", ID: m.skippedRecordID(), Doc: skippedTransactions{PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, GTIDs: next.String()}, Critical: true}); err != nil {
		return errors.New("amqpin: cannot persist skipped transaction set")
	}
	m.skippedSet = next
	event := buildFreezeEvent(m.opts.PipelineID, "TARGET_TRANSACTION_SKIPPED", fmt.Sprintf("已按显式错误策略跳过已回滚事务，GTID=%s", gtid), m.now().UnixMilli())
	event.Level = "WARN"
	m.Cast(reporter.ModuleName, reporter.EventReport{Event: event})
	return nil
}

// The serial consumer worker owns the decision. Its bounded continuation
// receives the store actor's asynchronous durable reply; no actor waits on IO.
func (m *Module) persistSkipOp(ctx context.Context, writer iCriticalAsyncStore, op store.Op) error {
	result, err := writer.PutCriticalAsync(ctx, op)
	if err != nil {
		return err
	}
	select {
	case err, ok := <-result:
		if !ok {
			return errors.New("amqpin: durable store reply closed without commit result")
		}
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
