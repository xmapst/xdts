package snapwriter

import (
	"context"
	"errors"
	"fmt"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

type recoveryRecord struct {
	Begin []byte `bson:"begin"`
	End   []byte `bson:"end,omitempty"`
}

// Wire and disk generations have independent totals. The startup value sent
// to the incremental owner can carry the original run's extension progress.
func validateRecoveryEnd(end *iproto.SnapshotEnd) error {
	if end.GetRecovery() == nil || end.InitialTotalChunks != 0 || end.PlanExtensionSequence != 0 {
		return errors.New("snapwriter: invalid recovery completion extension")
	}
	return validateEndWatermarks(end)
}

func validateRecoveryBeginAuthority(begin *iproto.SnapshotRecoveryBegin, previous *iproto.SnapshotEnd, sequence uint64, uuids map[string]bool) error {
	if err := iproto.ValidateSnapshotRecoveryBegin(begin); err != nil {
		return err
	}
	if previous == nil || begin.PipelineId != previous.PipelineId || begin.RunId != previous.RunId || begin.Recovery.Sequence != sequence || uuids[begin.Recovery.Uuid] || len(begin.Tables) != len(previous.RecoveryTables) {
		return errors.New("snapwriter: recovery begin authority differs")
	}
	for index, table := range begin.Tables {
		if !proto.Equal(table, previous.RecoveryTables[index]) {
			return errors.New("snapwriter: recovery begin scope differs")
		}
	}
	low, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, begin.LowGtidSet)
	high, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, previous.HighGtidSet)
	if err != nil || !low.Contain(high) {
		return errors.New("snapwriter: recovery low precedes previous high")
	}
	return nil
}

// Check persisted predecessors before recording a new Begin or clearing data.
// The target owns this check even when its peer already validated the request.
func (m *Module) validateRecoveryAuthority(ctx context.Context, begin *iproto.SnapshotRecoveryBegin) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := iproto.ValidateSnapshotRecoveryBegin(begin); err != nil {
		return err
	}
	var original completion
	if err := m.opts.Store.Get(ctx, "snapshot_targets", m.recordID(), &original); err != nil {
		return err
	}
	previous := original.end()
	if original.UUID != m.opts.Run.UUID {
		return errors.New("snapwriter: recovery original UUID differs")
	}
	if err := m.validateEnd(previous); err != nil {
		return err
	}
	uuids := map[string]bool{original.UUID: true}
	for sequence := uint64(1); sequence < begin.Recovery.Sequence; sequence++ {
		var record recoveryRecord
		if err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), sequence), &record); err != nil {
			return err
		}
		prior := new(iproto.SnapshotRecoveryBegin)
		end := new(iproto.SnapshotEnd)
		if proto.Unmarshal(record.Begin, prior) != nil || proto.Unmarshal(record.End, end) != nil || !proto.Equal(end.Recovery, prior.Recovery) || end.LowGtidSet != prior.LowGtidSet || end.PipelineId != prior.PipelineId || end.RunId != prior.RunId {
			return errors.New("snapwriter: recovery predecessor completion differs")
		}
		if err := validateRecoveryBeginAuthority(prior, previous, sequence, uuids); err != nil {
			return err
		}
		if err := validateRecoveryEnd(end); err != nil {
			return err
		}
		uuids[prior.Recovery.Uuid] = true
		previous = end
	}
	return validateRecoveryBeginAuthority(begin, previous, begin.Recovery.Sequence, uuids)
}

func verifyRecoveryChunkRange(set, uuid string, total uint64) error {
	if total == 0 || total >= 1<<63-4002 {
		return errors.New("snapwriter: invalid recovery chunk total")
	}
	actual, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	want, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, fmt.Sprintf("%s:1-%d", uuid, total))
	if err != nil {
		return err
	}
	if !actual.Contain(want) {
		return errors.New("snapwriter: recovery end has missing chunks")
	}
	return nil
}

func (m *Module) runRecovery(ctx context.Context, first delivery, inbox <-chan delivery, w *mysqlout.Writer, limits mysqlout.SnapshotOptions) error {
	begin := first.begin
	if !m.opts.AllowTableRecovery || m.opts.RecoveryRequests == nil || m.opts.RecoveryCompletions == nil {
		return errors.New("snapwriter: table recovery not negotiated")
	}
	if err := m.validateRecoveryAuthority(ctx, begin); err != nil {
		return err
	}
	id := fmt.Sprintf("%s.recovery.%d", m.recordID(), begin.Recovery.Sequence)
	body, err := (proto.MarshalOptions{Deterministic: true}).Marshal(begin)
	if err != nil {
		return err
	}
	var record recoveryRecord
	commit, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = m.opts.Store.Get(commit, "snapshot_targets", id, &record)
	if err == nil {
		prior := new(iproto.SnapshotRecoveryBegin)
		if proto.Unmarshal(record.Begin, prior) != nil || !proto.Equal(prior, begin) {
			cancel()
			return errors.New("snapwriter: recovery begin changed")
		}
	} else if errors.Is(err, xdoc.ErrNotFound) {
		record.Begin = body
		if err := m.opts.Store.PutCritical(commit, store.Op{Collection: "snapshot_targets", ID: id, Doc: record, InsertOnly: true}); err != nil {
			cancel()
			return err
		}
	} else {
		cancel()
		return err
	}
	cancel()
	permit := make(chan error, 1)
	select {
	case m.opts.RecoveryRequests <- iproto.SnapshotRecoveryRequest{Begin: proto.Clone(begin).(*iproto.SnapshotRecoveryBegin), Permit: permit}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-permit:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	run := limits
	run.UUID = begin.Recovery.Uuid
	run.AllowTableCreation = false
	for index, table := range begin.Tables {
		prepare, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := w.PrepareRecoveryTable(prepare, begin.Recovery, table, uint64(index))
		cancel()
		if err != nil {
			return err
		}
	}
	if first.ack != nil {
		if err := first.ack.Ack(); err != nil {
			return err
		}
	}
	// The serial owner has exclusive Writer ownership; workers are all idle.
	for {
		var d delivery
		select {
		case d = <-inbox:
		case <-ctx.Done():
			return ctx.Err()
		}
		err := func() error {
			defer d.buffer.Release()
			old, err := m.checkDeliveryRun(d)
			if err != nil {
				return err
			}
			if old {
				return d.ack.Ack()
			}
			if handled, err := m.ackEarlierRecoveryDelivery(ctx, d, begin.Recovery.Sequence); handled || err != nil {
				return err
			}
			if d.begin != nil {
				if !proto.Equal(d.begin, begin) {
					return errors.New("snapwriter: nested recovery begin")
				}
				return d.ack.Ack()
			}
			if d.probe != nil {
				return d.ack.Ack()
			}
			if d.chunk != nil {
				if !proto.Equal(d.chunk.Recovery, begin.Recovery) || d.chunk.ChunkNo >= 1<<63-4002 {
					return errors.New("snapwriter: recovery chunk identity/range mismatch")
				}
				found := false
				for _, table := range begin.Tables {
					found = found || table.Schema == d.chunk.Schema && table.Table == d.chunk.Table
				}
				if !found {
					return errors.New("snapwriter: recovery chunk outside scope")
				}
				if _, err := m.applyChunkRun(ctx, w, run, d.chunk); err != nil {
					return err
				}
				return d.ack.Ack()
			}
			end := d.end
			if end == nil {
				return errors.New("snapwriter: recovery completion missing")
			}
			if !proto.Equal(end.GetRecovery(), begin.Recovery) || end.GetLowGtidSet() != begin.LowGtidSet {
				return errors.New("snapwriter: recovery end identity mismatch")
			}
			if err := validateRecoveryEnd(end); err != nil {
				return err
			}
			if err := iproto.ValidateSnapshotRecoveryTables(end.RecoveryTables, true); err != nil {
				return err
			}
			set, err := w.GTIDSet(ctx)
			if err != nil {
				return err
			}
			if err := verifyRecoveryChunkRange(set, run.UUID, end.TotalChunks); err != nil {
				return err
			}
			encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(end)
			if err != nil {
				return err
			}
			if len(record.End) > 0 {
				prior := new(iproto.SnapshotEnd)
				if proto.Unmarshal(record.End, prior) != nil || !proto.Equal(prior, end) {
					return errors.New("snapwriter: recovery completion changed")
				}
			} else {
				record.End = encoded
				commit, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := m.opts.Store.PutCritical(commit, store.Op{Collection: "snapshot_targets", ID: id, Doc: record})
				cancel()
				if err != nil {
					return err
				}
			}
			result := make(chan error, 1)
			select {
			case m.opts.RecoveryCompletions <- iproto.SnapshotRecoveryCompletion{End: proto.Clone(end).(*iproto.SnapshotEnd), Result: result}:
			case <-ctx.Done():
				return ctx.Err()
			}
			select {
			case err := <-result:
				if err != nil {
					return err
				}
			case <-ctx.Done():
				return ctx.Err()
			}
			return d.ack.Ack()
		}()
		if err != nil {
			return err
		}
		if d.end != nil && proto.Equal(d.end.Recovery, begin.Recovery) {
			return nil
		}
	}
}

// Source restart republishes earlier completed Ends before the pending Begin.
// A target that resumes its durable Begin first must validate and acknowledge
// those predecessors without reopening cleanup or finishing the current pass.
func (m *Module) ackEarlierRecoveryDelivery(ctx context.Context, d delivery, sequence uint64) (bool, error) {
	var identity *iproto.SnapshotRecoveryIdentity
	if d.begin != nil {
		identity = d.begin.Recovery
	} else if d.chunk != nil {
		identity = d.chunk.Recovery
	} else if d.end != nil {
		identity = d.end.Recovery
	} else {
		return false, nil
	}
	if identity != nil && identity.Sequence >= sequence {
		return false, nil
	}
	var end *iproto.SnapshotEnd
	if identity == nil {
		var saved completion
		if err := m.opts.Store.Get(ctx, "snapshot_targets", m.recordID(), &saved); err != nil {
			return true, err
		}
		end = saved.end()
		if err := m.validateEnd(end); err != nil {
			return true, err
		}
	} else {
		var saved recoveryRecord
		if err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), identity.Sequence), &saved); err != nil {
			return true, err
		}
		prior := new(iproto.SnapshotRecoveryBegin)
		end = new(iproto.SnapshotEnd)
		if proto.Unmarshal(saved.Begin, prior) != nil || proto.Unmarshal(saved.End, end) != nil || !proto.Equal(prior.Recovery, identity) || !proto.Equal(end.Recovery, identity) || d.begin != nil && !proto.Equal(d.begin, prior) {
			return true, errors.New("snapwriter: predecessor recovery authority differs")
		}
	}
	if d.end != nil && !proto.Equal(end, d.end) || d.chunk != nil && d.chunk.ChunkNo > end.TotalChunks {
		return true, errors.New("snapwriter: predecessor snapshot delivery differs")
	}
	return true, d.ack.Ack()
}
