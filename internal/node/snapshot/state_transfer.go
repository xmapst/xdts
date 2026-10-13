package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

// frozenStateArchive is confined to the export/import workflow. It exposes no
// writes and cannot query today's source database to repair an invalid plan.
type frozenStateArchive map[[2]string][]byte

func (a frozenStateArchive) Get(ctx context.Context, collection, id string, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, ok := a[[2]string{collection, id}]
	if !ok {
		return xdoc.ErrNotFound
	}
	doc, err := xdoc.DecodeDocument(data)
	if err != nil {
		return err
	}
	return xdoc.UnmarshalValue(doc.Value(), out)
}
func (a frozenStateArchive) PutCritical(context.Context, store.Op) error {
	return errors.New("snapshot: state archive is read only")
}

// ValidateTransferState reuses the released Planner/Runner invariants before
// ownership can move. Missing ranges, corrupt keys and rewritten L/H fail; no
// new boundary or GTID observation can make the transferred state appear valid.
func ValidateTransferState(ctx context.Context, records []store.StateRecord, opts RunOptions) error {
	base := fmt.Sprintf("%d:%s:%d", len(opts.PipelineID), opts.PipelineID, opts.RunID)
	groups := map[uint64][]store.StateRecord{}
	var original []store.StateRecord
	archive := frozenStateArchive{}
	for _, record := range records {
		key := [2]string{record.Collection, record.ID}
		if _, exists := archive[key]; exists {
			return errors.New("snapshot: duplicate transfer document")
		}
		archive[key] = record.Data
		if suffix, ok := strings.CutPrefix(record.ID, base+".recovery."); ok && (record.Collection == "snapshot_chunks" || record.Collection == "snapshot_runs") {
			parts := strings.SplitN(suffix, ".", 2)
			sequence, err := strconv.ParseUint(parts[0], 10, 64)
			if !opts.EnableTableRecovery || err != nil || sequence == 0 || sequence > 4096 || parts[0] != strconv.FormatUint(sequence, 10) {
				return errors.New("snapshot: invalid transferred recovery sequence or capability")
			}
			groups[sequence] = append(groups[sequence], record)
		} else {
			original = append(original, record)
		}
	}
	if err := validateSingleTransferState(ctx, original, opts); err != nil {
		return err
	}
	if len(groups) == 0 {
		return nil
	}
	var previous End
	if err := archive.Get(ctx, "snapshot_runs", base+".end", &previous); err != nil {
		return err
	}
	uuids := map[string]bool{opts.SnapshotUUID: true}
	for sequence := uint64(1); sequence <= uint64(len(groups)); sequence++ {
		group, present := groups[sequence]
		if !present {
			return errors.New("snapshot: transferred recovery generations have gaps")
		}
		var record recoveryBeginRecord
		if err := archive.Get(ctx, "snapshot_runs", fmt.Sprintf("%s.recovery.%d.begin", base, sequence), &record); err != nil {
			return err
		}
		begin := new(iproto.SnapshotRecoveryBegin)
		if err := proto.Unmarshal(record.Body, begin); err != nil {
			return err
		}
		if iproto.ValidateSnapshotRecoveryBegin(begin) != nil || begin.PipelineId != opts.PipelineID || begin.RunId != opts.RunID || begin.Recovery.Sequence != sequence || uuids[begin.Recovery.Uuid] || len(begin.Tables) != len(previous.RecoveryTables) {
			return errors.New("snapshot: transferred recovery authority differs")
		}
		for i, table := range begin.Tables {
			if !proto.Equal(table, previous.RecoveryTables[i]) {
				return errors.New("snapshot: transferred recovery scope differs")
			}
		}
		low, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, begin.LowGtidSet)
		high, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, previous.HighGTIDSet)
		if !low.Contain(high) {
			return errors.New("snapshot: transferred recovery low precedes previous high")
		}
		uuids[begin.Recovery.Uuid] = true
		child := opts
		child.Recovery, child.SnapshotUUID = begin.Recovery, begin.Recovery.Uuid
		child.EnablePlanExtensions, child.EnableTableCreation, child.ReinitPlan = false, false, nil
		// A crash can leave just the committed Begin before planning starts.
		if len(group) == 1 && group[0].ID == fmt.Sprintf("%s.recovery.%d.begin", base, sequence) {
			if sequence != uint64(len(groups)) {
				return errors.New("snapshot: recovery continued without a completed predecessor")
			}
			return nil
		}
		if err := validateSingleTransferState(ctx, group, child); err != nil {
			return err
		}
		var plan Plan
		if err := archive.Get(ctx, "snapshot_chunks", fmt.Sprintf("%s.recovery.%d", base, sequence), &plan); err != nil || len(plan.Tables) != len(begin.Tables) {
			return errors.New("snapshot: transferred recovery plan scope differs")
		}
		for index, table := range begin.Tables {
			if plan.Tables[index].Schema != table.Schema || plan.Tables[index].Table != table.Table {
				return errors.New("snapshot: transferred recovery plan scope differs")
			}
		}
		var start Start
		if err := archive.Get(ctx, "snapshot_runs", fmt.Sprintf("%s.recovery.%d.start", base, sequence), &start); err != nil || start.LowGTIDSet != begin.LowGtidSet {
			return errors.New("snapshot: transferred recovery planning low differs")
		}
		if err := archive.Get(ctx, "snapshot_runs", fmt.Sprintf("%s.recovery.%d.end", base, sequence), &previous); err != nil {
			if errors.Is(err, xdoc.ErrNotFound) && sequence == uint64(len(groups)) {
				return nil
			}
			return err
		}
	}
	return nil
}

func validateSingleTransferState(ctx context.Context, records []store.StateRecord, opts RunOptions) error {
	archive := frozenStateArchive{}
	for _, record := range records {
		key := [2]string{record.Collection, record.ID}
		if _, exists := archive[key]; exists {
			return errors.New("snapshot: duplicate transfer document")
		}
		archive[key] = record.Data
	}
	planRun := strconv.FormatUint(opts.RunID, 10)
	if opts.Recovery != nil {
		planRun += fmt.Sprintf(".recovery.%d", opts.Recovery.Sequence)
	}
	planner, err := NewPlanner(archive, opts.PipelineID, planRun)
	if err != nil {
		return err
	}
	if opts.ReinitPlan != nil {
		if err := ValidateReinitPlan(opts.ReinitPlan); err != nil {
			return err
		}
		planner.reinitDigest = opts.ReinitPlan.GetDigest()
	}
	loaded, err := planner.load(ctx)
	if err != nil {
		return err
	}
	if !loaded {
		return errors.New("snapshot: original transfer plan missing")
	}
	if planner.proposal.ChunkRows != opts.ChunkRows || planner.extensions > 0 && !opts.EnablePlanExtensions {
		return errors.New("snapshot: transfer plan configuration differs")
	}
	if opts.ReinitPlan != nil {
		if err := matchReinitScope(opts.ReinitPlan, planner.proposal.Tables); err != nil {
			return err
		}
	}
	if opts.OperationTimeout <= 0 {
		opts.OperationTimeout = DefaultOperationTimeout
	}
	runner := &Runner{opts: opts, store: archive, planner: planner, plan: planner.proposal.clone()}
	var start Start
	if err := archive.Get(ctx, "snapshot_runs", runner.recordID("start"), &start); err != nil {
		return err
	}
	if err := runner.validateStart(start); err != nil {
		return err
	}
	runner.start = &start
	var end End
	if err := archive.Get(ctx, "snapshot_runs", runner.recordID("end"), &end); err == nil {
		if len(end.RecoveryTables) > 0 && !opts.EnableTableRecovery {
			return errors.New("snapshot: transferred recovery completion requires support")
		}
		if err := runner.validateEnd(end); err != nil {
			return err
		}
	} else if !errors.Is(err, xdoc.ErrNotFound) {
		return err
	}
	for _, record := range records {
		if record.Collection != "snapshot_chunks" {
			if record.Collection == "snapshot_runs" && strings.HasPrefix(record.ID, runner.recordID("creation.")) {
				var saved tableCreationRecord
				if err := archive.Get(ctx, record.Collection, record.ID, &saved); err != nil {
					return err
				}
				if record.ID != runner.creationRecordID(saved.Schema, saved.Table) {
					return errors.New("snapshot: transfer creation record identity differs")
				}
				found := false
				for _, table := range runner.plan.Tables {
					if table.Schema == saved.Schema && table.Table == saved.Table {
						found = true
						break
					}
				}
				if !found {
					return errors.New("snapshot: transfer creation outside original plan")
				}
				txn, err := runner.decodeCreation(saved, saved.Schema, saved.Table)
				if err != nil {
					return err
				}
				if end.HighGTIDSet != "" {
					high, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.HighGTIDSet)
					one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.GetGtid())
					if !high.Contain(one) {
						return errors.New("snapshot: transfer creation exceeds original high")
					}
				}
			}
			continue
		}
		if suffix, ok := strings.CutPrefix(record.ID, planner.documentID()+".extension."); ok {
			number, err := strconv.ParseUint(suffix, 10, 64)
			if err != nil || number == 0 || number > planner.extensions || record.ID != planner.extensionID(number) {
				return errors.New("snapshot: transfer extension sequence has gaps")
			}
		}
		if suffix, ok := strings.CutPrefix(record.ID, planner.documentID()+".published."); ok {
			number, err := strconv.ParseUint(suffix, 10, 64)
			if err != nil || number == 0 || number > runner.plan.Total || record.ID != runner.chunkRecordID(number) {
				return errors.New("snapshot: transferred published chunk outside plan")
			}
			found := false
			for _, table := range runner.plan.Tables {
				if number < table.FirstChunk || number-table.FirstChunk >= table.ChunkCount {
					continue
				}
				chunk, err := table.Chunk(number - table.FirstChunk)
				if err != nil {
					return err
				}
				_, present, err := runner.published(ctx, chunk)
				if err != nil {
					return err
				}
				if !present {
					return fmt.Errorf("snapshot: published transfer record missing")
				}
				found = true
				break
			}
			if !found {
				return errors.New("snapshot: transferred published chunk has no table")
			}
		}
	}
	return nil
}
