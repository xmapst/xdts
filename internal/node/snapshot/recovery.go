package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
)

type iRecoverySink interface {
	SendSnapshotRecoveryBegin(context.Context, *iproto.SnapshotRecoveryBegin) error
}

func (r *Runner) SetRecovery(tables func() []*iproto.SnapshotRecoveryTable, begin func([]*iproto.SnapshotRecoveryTable), at func([]*iproto.SnapshotRecoveryTable, string) error) {
	r.opts.EnableTableRecovery = true
	r.opts.RecoveryTables = tables
	r.opts.BeginRecovery = begin
	r.opts.BeginRecoveryAt = at
}

type iRecoveryTableSource interface {
	Table(context.Context, string, string) (Table, error)
}
type recoverySource struct {
	IRunSource
	names   []*iproto.SnapshotRecoveryTable
	low     string
	initial bool
}

func (s *recoverySource) Tables(ctx context.Context) ([]Table, error) {
	source, ok := s.IRunSource.(iRecoveryTableSource)
	if !ok {
		return nil, errors.New("snapshot: recovery table metadata unavailable")
	}
	tables := make([]Table, 0, len(s.names))
	for _, name := range s.names {
		table, err := source.Table(ctx, name.Schema, name.Table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}
func (s *recoverySource) GTIDSet(ctx context.Context) (string, error) {
	if s.initial {
		s.initial = false
		return s.low, nil
	}
	return s.IRunSource.GTIDSet(ctx)
}
func (s *recoverySource) PlannedTables(ctx context.Context, plan Plan) ([]Table, error) {
	// Reuse the original baseline even if a later incompatible DDL changed it.
	var tables []Table
	for _, table := range plan.Tables {
		if table.Baseline == nil {
			return nil, errors.New("snapshot: recovery baseline missing")
		}
		tables = append(tables, *table.clone().Baseline)
	}
	return tables, nil
}
func (s *recoverySource) VerifyReinitTables(context.Context, *iproto.ReinitSourcePlan, []Table) error {
	return errors.New("snapshot: recovery cannot reuse whole-run reinitialization")
}

func endMessage(end End) *iproto.SnapshotEnd {
	msg := &iproto.SnapshotEnd{PipelineId: end.PipelineID, RunId: end.RunID, LowGtidSet: end.LowGTIDSet, HighGtidSet: end.HighGTIDSet, TotalChunks: end.TotalChunks, InitialTotalChunks: end.InitialTotalChunks, PlanExtensionSequence: end.PlanExtensionSequence}
	if end.Recovery != nil {
		msg.Recovery = proto.Clone(end.Recovery).(*iproto.SnapshotRecoveryIdentity)
	}
	for _, table := range end.RecoveryTables {
		msg.RecoveryTables = append(msg.RecoveryTables, proto.Clone(table).(*iproto.SnapshotRecoveryTable))
	}
	return msg
}

type recoveryBeginRecord struct {
	Body []byte `bson:"body"`
}

// RunAll owns every recovery generation in the same source workflow. Earlier
// generations are replayed with their durable identities before proceeding.
func (r *Runner) RunAll(ctx context.Context) (End, error) {
	end, err := r.Run(ctx)
	if err != nil || r.opts.RecoveryTables == nil {
		return end, err
	}
	sink, ok := r.sink.(iRecoverySink)
	if !ok {
		return End{}, errors.New("snapshot: recovery publisher unavailable")
	}
	tables := end.RecoveryTables
	uuids := map[string]bool{r.opts.SnapshotUUID: true}
	for sequence := uint64(1); ; sequence++ {
		if sequence > 4096 {
			return End{}, errors.New("snapshot: table recovery generations exceed safe bound")
		}
		suffix := "recovery." + strconv.FormatUint(sequence, 10) + ".begin"
		var record recoveryBeginRecord
		err := r.store.Get(ctx, "snapshot_runs", r.recordID(suffix), &record)
		var begin iproto.SnapshotRecoveryBegin
		if errors.Is(err, xdoc.ErrNotFound) {
			if len(tables) == 0 {
				if r.opts.FinishRecovery != nil {
					r.opts.FinishRecovery()
				}
				return end, nil
			}
			if r.opts.BeginRecovery == nil {
				return End{}, errors.New("snapshot: recovery guard unavailable")
			}
			r.opts.BeginRecovery(tables)
			low, err := r.source.GTIDSet(ctx)
			if err != nil {
				return End{}, err
			}
			begin = iproto.SnapshotRecoveryBegin{PipelineId: r.opts.PipelineID, RunId: r.opts.RunID, Recovery: &iproto.SnapshotRecoveryIdentity{Sequence: sequence, Uuid: uuidx.New().String()}, LowGtidSet: low, Tables: tables}
			record.Body, err = (proto.MarshalOptions{Deterministic: true}).Marshal(&begin)
			if err != nil {
				return End{}, err
			}
			if err := r.createRecord(ctx, suffix, record); err != nil {
				return End{}, err
			}
			if err := r.store.Get(ctx, "snapshot_runs", r.recordID(suffix), &record); err != nil {
				return End{}, err
			}
		} else if err != nil {
			return End{}, err
		}
		if err := proto.Unmarshal(record.Body, &begin); err != nil {
			return End{}, err
		}
		if err := iproto.ValidateSnapshotRecoveryBegin(&begin); err != nil {
			return End{}, err
		}
		if begin.PipelineId != r.opts.PipelineID || begin.RunId != r.opts.RunID || begin.Recovery.Sequence != sequence {
			return End{}, errors.New("snapshot: stored recovery authority changed")
		}
		if uuids[begin.Recovery.Uuid] || len(begin.Tables) != len(tables) {
			return End{}, errors.New("snapshot: stored recovery UUID or scope changed")
		}
		for index, table := range tables {
			if !proto.Equal(begin.Tables[index], table) {
				return End{}, errors.New("snapshot: stored recovery scope changed")
			}
		}
		uuids[begin.Recovery.Uuid] = true
		if r.opts.BeginRecoveryAt == nil {
			return End{}, errors.New("snapshot: recovery low guard unavailable")
		}
		if err := r.opts.BeginRecoveryAt(begin.Tables, begin.LowGtidSet); err != nil {
			return End{}, err
		}
		opts := r.opts
		opts.Recovery = proto.Clone(begin.Recovery).(*iproto.SnapshotRecoveryIdentity)
		opts.SnapshotUUID = begin.Recovery.Uuid
		opts.EnablePlanExtensions = false
		opts.EnableTableCreation = false
		opts.ReinitPlan = nil
		// Recovery ranges have a distinct UUID and cannot replace the original
		// run's immutable progress totals at Manager.
		opts.Report = nil
		scoped := &recoverySource{IRunSource: r.source, names: begin.Tables, low: begin.LowGtidSet, initial: true}
		child, err := NewRunner(opts, r.store, scoped, r.reader, r.sink)
		if err != nil {
			return End{}, err
		}
		if _, err := child.Prepare(ctx); err != nil {
			return End{}, err
		}
		// Prepare's catalog can race with a DDL after L. The incremental guard
		// must observe that interval before any recovery read gets published.
		if opts.WaitObserved != nil {
			high, err := r.source.GTIDSet(ctx)
			if err != nil {
				return End{}, err
			}
			wait, cancel := context.WithTimeout(ctx, opts.OperationTimeout)
			err = opts.WaitObserved(wait, high)
			cancel()
			if err != nil {
				return End{}, err
			}
		}
		send, cancel := context.WithTimeout(ctx, opts.OperationTimeout)
		err = sink.SendSnapshotRecoveryBegin(send, proto.Clone(&begin).(*iproto.SnapshotRecoveryBegin))
		cancel()
		if err != nil {
			return End{}, err
		}
		end, err = child.Run(ctx)
		if err != nil {
			return End{}, fmt.Errorf("snapshot: recovery %d failed: %w", sequence, err)
		}
		tables = end.RecoveryTables
	}
}
