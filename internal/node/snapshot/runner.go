package snapshot

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"
	"uuid"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/schemamig"
)

type IRunSource interface {
	IBoundaryReader
	Tables(context.Context) ([]Table, error)
	GTIDSet(context.Context) (string, error)
}

type IPlanReader interface {
	ReadPlan(context.Context, Plan, []Table, func(context.Context, ChunkData) error) error
}

type iExtensionSource interface {
	TablesForExtension(context.Context, Plan) ([]Table, error)
}

type ISnapshotSink interface {
	SendSnapshotChunk(context.Context, *iproto.SnapshotChunk) error
	SendSnapshotEnd(context.Context, *iproto.SnapshotEnd) error
}

type RunOptions struct {
	PipelineID       string
	RunID            uint64
	SnapshotUUID     string
	ChunkRows        uint64
	OperationTimeout time.Duration
	// Report receives ownership of an independent snapshot progress value.
	Report               func(*iproto.SnapshotProgress)
	RateReport           func(float64, float64)
	WaitHigh             func(context.Context, string) error
	UpdateObjects        func([]schemamig.SchemaTable)
	EnablePlanExtensions bool
	EnableTableCreation  bool
	EnableTableRecovery  bool
	WaitObserved         func(context.Context, string) error
	TableCreation        func(string, string) *iproto.Txn
	ReinitPlan           *iproto.ReinitSourcePlan
	ReadEndpointUUID     string
	ShadowTablePolicy    *iproto.ShadowTablePolicy
	Recovery             *iproto.SnapshotRecoveryIdentity
	RecoveryTables       func() []*iproto.SnapshotRecoveryTable
	BeginRecovery        func([]*iproto.SnapshotRecoveryTable)
	BeginRecoveryAt      func([]*iproto.SnapshotRecoveryTable, string) error
	FinishRecovery       func()
}

// Start and End are separate immutable records. Updating End must never replace
// the durable L, which remains authoritative throughout the run and its retries.
type Start struct {
	Version          int    `bson:"version"`
	PipelineID       string `bson:"pipeline_id"`
	RunID            uint64 `bson:"run_id"`
	SnapshotUUID     string `bson:"snapshot_uuid"`
	LowGTIDSet       string `bson:"low_gtid_set"`
	TotalChunks      uint64 `bson:"total_chunks"`
	ReadEndpointUUID string `bson:"read_endpoint_uuid"`
}

type End struct {
	Version               int                              `bson:"version"`
	PipelineID            string                           `bson:"pipeline_id"`
	RunID                 uint64                           `bson:"run_id"`
	LowGTIDSet            string                           `bson:"low_gtid_set"`
	HighGTIDSet           string                           `bson:"high_gtid_set"`
	TotalChunks           uint64                           `bson:"total_chunks"`
	InitialTotalChunks    uint64                           `bson:"initial_total_chunks,omitempty"`
	PlanExtensionSequence uint64                           `bson:"plan_extension_sequence,omitempty"`
	Recovery              *iproto.SnapshotRecoveryIdentity `bson:"recovery,omitempty"`
	RecoveryTables        []*iproto.SnapshotRecoveryTable  `bson:"recovery_tables,omitempty"`
}

// Runner is confined to one source background workflow. Prepare precedes
// incremental startup; Run must only be called once the incremental subscription
// is ready. The workflow that owns it cancels both streams on failure/stop.
type Runner struct {
	opts           RunOptions
	store          IStore
	source         IRunSource
	reader         IPlanReader
	sink           ISnapshotSink
	plan           Plan
	tables         []Table
	start          *Start
	end            *End
	prepared       bool
	done           uint64
	progress       []*iproto.TableProgress
	lastReport     time.Time
	tableDone      map[string]uint64
	rateChunks     uint64
	reportCounters func(uint64, uint64)
	rateRows       uint64
	rateBytes      uint64
	rateAt         time.Time
	tableReadRows  map[string]uint64
	planner        *Planner
	tableCreations map[[2]string]*iproto.Txn
}

func NewRunner(opts RunOptions, storage IStore, source IRunSource, reader IPlanReader, sink ISnapshotSink) (*Runner, error) {
	if opts.Recovery != nil {
		if err := iproto.ValidateSnapshotRecoveryIdentity(opts.Recovery); err != nil {
			return nil, err
		}
		if opts.SnapshotUUID != opts.Recovery.Uuid || !opts.EnableTableRecovery || opts.ReinitPlan != nil || opts.EnablePlanExtensions || opts.EnableTableCreation {
			return nil, errors.New("snapshot: invalid recovery run configuration")
		}
		opts.Recovery = proto.Clone(opts.Recovery).(*iproto.SnapshotRecoveryIdentity)
	}
	id, err := uuid.Parse(opts.SnapshotUUID)
	if err != nil || id == uuid.Nil() || id.String() != opts.SnapshotUUID || opts.PipelineID == "" ||
		opts.ChunkRows == 0 || opts.OperationTimeout <= 0 || storage == nil || source == nil || reader == nil || sink == nil {
		return nil, fmt.Errorf("snapshot: invalid run options or dependencies")
	}
	if opts.ReinitPlan != nil {
		if err := ValidateReinitPlan(opts.ReinitPlan); err != nil {
			return nil, err
		}
		opts.ReinitPlan = proto.Clone(opts.ReinitPlan).(*iproto.ReinitSourcePlan)
		if _, ok := source.(iReinitSource); !ok {
			return nil, fmt.Errorf("snapshot: source cannot verify confirmed scope")
		}
	}
	if opts.ReadEndpointUUID != "" {
		id, err := uuid.Parse(opts.ReadEndpointUUID)
		if err != nil || id == uuid.Nil() || id.String() != opts.ReadEndpointUUID {
			return nil, fmt.Errorf("snapshot: invalid read endpoint identity")
		}
	}
	return &Runner{opts: opts, store: storage, source: source, reader: reader, sink: sink}, nil
}

func (r *Runner) recordID(suffix string) string {
	if r.opts.Recovery != nil {
		return fmt.Sprintf("%d:%s:%d.recovery.%d.%s", len(r.opts.PipelineID), r.opts.PipelineID, r.opts.RunID, r.opts.Recovery.Sequence, suffix)
	}
	return fmt.Sprintf("%d:%s:%d.%s", len(r.opts.PipelineID), r.opts.PipelineID, r.opts.RunID, suffix)
}

// PlannedObjects is called after Prepare and before Run transfers ownership.
func (r *Runner) PlannedObjects() []schemamig.SchemaTable {
	objects := make([]schemamig.SchemaTable, 0, len(r.plan.Tables))
	for _, table := range r.plan.Tables {
		objects = append(objects, schemamig.SchemaTable{Schema: table.Schema, Table: table.Table})
	}
	return objects
}

func (r *Runner) CompletedHigh(ctx context.Context) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	var end End
	err := r.store.Get(ctx, "snapshot_runs", r.recordID("end"), &end)
	if errors.Is(err, xdoc.ErrNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if err := r.validateEnd(end); err != nil {
		return "", false, err
	}
	return end.HighGTIDSet, true, nil
}

// Prepare commits the plan and L before returning an explicit starting GTID.
// An empty GTID set is valid for a pristine server and is distinguished by the
// successful Prepare return, rather than an invented sentinel GTID.
func (r *Runner) Prepare(ctx context.Context) (Start, error) {
	if r.prepared {
		return *r.start, nil
	}
	if r.start == nil {
		var loaded Start
		err := r.store.Get(ctx, "snapshot_runs", r.recordID("start"), &loaded)
		if err == nil {
			if loaded.ReadEndpointUUID != r.opts.ReadEndpointUUID {
				return Start{}, fmt.Errorf("snapshot: stored read endpoint identity changed; reinitialize")
			}
			if loaded.TotalChunks == 0 {
				return Start{}, fmt.Errorf("snapshot: invalid stored start chunk count")
			}
			r.start = &loaded
		} else if !errors.Is(err, xdoc.ErrNotFound) {
			return Start{}, fmt.Errorf("snapshot: load start watermark failed: %w", err)
		} else {
			err = r.store.Get(ctx, "snapshot_runs", r.recordID("low"), &loaded)
			if err == nil {
				if loaded.Version != 1 || loaded.PipelineID != r.opts.PipelineID || loaded.RunID != r.opts.RunID || loaded.SnapshotUUID != r.opts.SnapshotUUID || loaded.TotalChunks != 0 || loaded.ReadEndpointUUID != r.opts.ReadEndpointUUID {
					return Start{}, fmt.Errorf("snapshot: invalid stored planning watermark")
				}
				if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, loaded.LowGTIDSet); err != nil {
					return Start{}, fmt.Errorf("snapshot: invalid stored planning GTID set")
				}
				r.start = &loaded
			} else if !errors.Is(err, xdoc.ErrNotFound) {
				return Start{}, fmt.Errorf("snapshot: load planning watermark failed: %w", err)
			}
		}
	}
	planRun := strconv.FormatUint(r.opts.RunID, 10)
	if r.opts.Recovery != nil {
		planRun += fmt.Sprintf(".recovery.%d", r.opts.Recovery.Sequence)
	}
	planner, _ := NewPlanner(r.store, r.opts.PipelineID, planRun)
	r.planner = planner
	planner.operationTimeout = r.opts.OperationTimeout
	if r.opts.ReinitPlan != nil {
		planner.reinitDigest = r.opts.ReinitPlan.GetDigest()
	}
	loaded, err := planner.load(ctx)
	if err != nil {
		return Start{}, err
	}
	if planner.extensions > 0 && !r.opts.EnablePlanExtensions {
		return Start{}, fmt.Errorf("snapshot: stored extensions require negotiated support")
	}
	if loaded && r.opts.ReinitPlan != nil {
		if err := matchReinitScope(r.opts.ReinitPlan, planner.proposal.Tables); err != nil {
			return Start{}, err
		}
	}
	if r.start == nil {
		if loaded {
			return Start{}, fmt.Errorf("snapshot: committed chunk plan has no durable low watermark; reinitialize")
		}
		// Establish L before catalog/metadata observation. Concurrent changes
		// after this point are replayed by the incremental DDL guard.
		low, err := r.source.GTIDSet(ctx)
		if err != nil {
			return Start{}, err
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, low); err != nil {
			return Start{}, fmt.Errorf("snapshot: invalid low GTID set")
		}
		r.start = &Start{Version: 1, PipelineID: r.opts.PipelineID, RunID: r.opts.RunID,
			SnapshotUUID: r.opts.SnapshotUUID, LowGTIDSet: low, ReadEndpointUUID: r.opts.ReadEndpointUUID}
	}
	var tables []Table
	if loaded && r.opts.RecoveryTables != nil {
		// An incompatible DDL may already have been repaired by a later
		// generation. Resume the original persisted ranges and baseline;
		// the ordered binlog guard isolates any unfinished stale reads.
		tables, err = (&recoverySource{}).PlannedTables(ctx, *planner.proposal)
	} else if source, ok := r.source.(iReinitSource); loaded && ok {
		tables, err = source.PlannedTables(ctx, *planner.proposal)
	} else {
		tables, err = r.source.Tables(ctx)
	}
	if err != nil {
		return Start{}, err
	}
	if r.opts.ReinitPlan != nil && !loaded {
		if err := r.source.(iReinitSource).VerifyReinitTables(ctx, r.opts.ReinitPlan, tables); err != nil {
			return Start{}, err
		}
	}
	if r.start.TotalChunks == 0 {
		// L must survive a crash after the plan commit and before the start
		// record commit. Never recapture a later L for an existing chunk plan.
		if err := r.createRecord(ctx, "low", *r.start); err != nil {
			return Start{}, err
		}
		var committed Start
		if err := r.store.Get(ctx, "snapshot_runs", r.recordID("low"), &committed); err != nil {
			return Start{}, err
		}
		if committed.Version != 1 || committed.PipelineID != r.opts.PipelineID || committed.RunID != r.opts.RunID || committed.SnapshotUUID != r.opts.SnapshotUUID || committed.TotalChunks != 0 || committed.ReadEndpointUUID != r.opts.ReadEndpointUUID {
			return Start{}, fmt.Errorf("snapshot: planning watermark identity changed")
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, committed.LowGTIDSet); err != nil {
			return Start{}, fmt.Errorf("snapshot: invalid planning GTID set")
		}
		r.start = &committed
	}
	plan, err := planner.Plan(ctx, tables, r.opts.ChunkRows, r.source)
	if err != nil {
		return Start{}, err
	}
	ordered, err := tablesForPlan(plan, tables)
	if err != nil {
		return Start{}, err
	}
	r.plan, r.tables = plan, ordered
	for i, planned := range plan.Tables {
		if planned.Baseline != nil {
			r.tables[i] = *planned.Baseline
		}
	}
	if r.start.TotalChunks == 0 {
		r.start.TotalChunks = plan.Total
	}
	if err := r.validateStart(*r.start); err != nil {
		return Start{}, err
	}
	if err := r.createRecord(ctx, "start", *r.start); err != nil {
		return Start{}, err
	}
	// Read back after the insert acknowledgement (including AlreadyExists). An
	// earlier timed-out writer may have won with a different observed watermark.
	var committed Start
	if err := r.store.Get(ctx, "snapshot_runs", r.recordID("start"), &committed); err != nil {
		return Start{}, err
	}
	if err := r.validateStart(committed); err != nil {
		return Start{}, err
	}
	r.start = &committed
	r.prepared = true
	for _, table := range r.tables {
		r.progress = append(r.progress, &iproto.TableProgress{Schema: table.Schema, Table: table.Name, EstimatedRows: int64(min(table.EstimatedRows, uint64(1<<63-1))), Status: iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_PENDING})
	}
	return committed, nil
}

func (r *Runner) createRecord(ctx context.Context, suffix string, doc any) error {
	ctx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	err := r.store.PutCritical(ctx, store.Op{Collection: "snapshot_runs", ID: r.recordID(suffix), Doc: doc, Critical: true, InsertOnly: true})
	if errors.Is(err, store.ErrAlreadyExists) {
		return nil
	}
	return err
}

// Run publishes every chunk with confirmation before taking H. A committed End
// is re-sent without re-reading tables: H and the completed plan must agree.
func (r *Runner) Run(ctx context.Context) (End, error) {
	if !r.prepared {
		return End{}, fmt.Errorf("snapshot: prepare before starting incremental and snapshot reads")
	}
	if r.end == nil {
		var loaded End
		err := r.store.Get(ctx, "snapshot_runs", r.recordID("end"), &loaded)
		if err == nil {
			if err := r.validateEnd(loaded); err != nil {
				return End{}, err
			}
			r.end = &loaded
		} else if !errors.Is(err, xdoc.ErrNotFound) {
			return End{}, fmt.Errorf("snapshot: load end watermark failed: %w", err)
		}
	}
	if r.end == nil {
		r.done = 0
		r.tableDone = make(map[string]uint64)
		r.rateAt = time.Now()
		r.rateRows, r.rateBytes, r.rateChunks = 0, 0, 0
		r.tableReadRows = make(map[string]uint64)
		for _, table := range r.progress {
			table.DoneRows = 0
			total := uint64(0)
			for _, planned := range r.plan.Tables {
				if planned.Schema == table.Schema && planned.Table == table.Table {
					total = planned.ChunkCount
					break
				}
			}
			done := uint64(0)
			table.TotalChunks, table.DoneChunks, table.RowsPerSec = &total, &done, nil
			table.Status = iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_PENDING
		}
		var readThrough uint64
		deliver := func(ctx context.Context, data ChunkData) error {
			if data.Chunk.Number <= readThrough {
				return nil
			}
			if data.Published {
				r.recordProgress(data.Chunk, data.PublishedRows)
				return nil
			}
			// Non-resumable readers remain supported for narrow test adapters.
			rows, completed, err := r.published(ctx, data.Chunk)
			if err != nil {
				return err
			}
			if completed {
				r.recordProgress(data.Chunk, rows)
				return nil
			}
			chunk := &iproto.SnapshotChunk{PipelineId: r.opts.PipelineID, RunId: r.opts.RunID, ChunkNo: data.Chunk.Number,
				Schema: data.Chunk.Schema, Table: data.Chunk.Table, ColumnNames: data.Columns, Rows: data.Rows,
				ShadowTablePolicy: iproto.CloneShadowTablePolicy(r.opts.ShadowTablePolicy)}
			if r.opts.Recovery != nil {
				chunk.Recovery = proto.Clone(r.opts.Recovery).(*iproto.SnapshotRecoveryIdentity)
			}
			if creation := r.tableCreations[[2]string{data.Chunk.Schema, data.Chunk.Table}]; creation != nil {
				chunk.TableCreation = proto.Clone(creation).(*iproto.Txn)
			}
			for _, col := range data.Chunk.Key.Columns {
				chunk.PrimaryKeyColumns = append(chunk.PrimaryKeyColumns, col.Name)
			}
			if err := iproto.ValidateSnapshotChunk(chunk); err != nil {
				return err
			}
			sendCtx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
			defer cancel()
			if err := r.sink.SendSnapshotChunk(sendCtx, chunk); err != nil {
				return err
			}
			record := publishedChunk{PipelineID: r.opts.PipelineID, RunID: r.opts.RunID, UUID: r.opts.SnapshotUUID, Number: data.Chunk.Number, Schema: data.Chunk.Schema, Table: data.Chunk.Table, Rows: uint64(len(data.Rows))}
			if err := r.store.PutCritical(sendCtx, store.Op{Collection: "snapshot_chunks", ID: r.chunkRecordID(data.Chunk.Number), Doc: record, InsertOnly: true, Critical: true}); err != nil && !errors.Is(err, store.ErrAlreadyExists) {
				return err
			}
			count, ok, err := r.published(ctx, data.Chunk)
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("snapshot: published record missing after commit")
			}
			r.rateRows += count
			r.rateChunks++
			if r.reportCounters != nil {
				r.reportCounters(r.rateRows, r.rateChunks)
			}
			r.tableReadRows[fmt.Sprintf("%d:%s%s", len(data.Chunk.Schema), data.Chunk.Schema, data.Chunk.Table)] += count
			r.rateBytes += data.Bytes
			r.recordProgress(data.Chunk, count)
			return nil
		}
		for {
			if err := r.prepareTableCreations(ctx); err != nil {
				return End{}, err
			}
			var err error
			if reader, ok := r.reader.(IResumePlanReader); ok {
				err = reader.ReadPendingPlan(ctx, r.plan, r.tables, r.published, deliver)
			} else {
				err = r.reader.ReadPlan(ctx, r.plan, r.tables, deliver)
			}
			if err != nil {
				return End{}, err
			}
			readThrough = r.plan.Total
			if r.opts.ReinitPlan != nil || !r.opts.EnablePlanExtensions {
				break
			}
			var current []Table
			if source, ok := r.source.(iExtensionSource); ok {
				current, err = source.TablesForExtension(ctx, r.plan.clone())
			} else {
				current, err = r.source.Tables(ctx)
			}
			if err != nil {
				return End{}, err
			}
			updated, added, err := r.planner.Extend(ctx, current, r.opts.ChunkRows, r.source)
			if err != nil {
				return End{}, err
			}
			if !added {
				break
			}
			ordered, err := tablesForPlan(updated, current)
			if err != nil {
				return End{}, err
			}
			r.plan, r.tables = updated, ordered
			for i, planned := range updated.Tables {
				if planned.Baseline != nil {
					r.tables[i] = *planned.clone().Baseline
				}
			}
			r.appendProgress(ordered)
			if r.opts.UpdateObjects != nil {
				r.opts.UpdateObjects(r.PlannedObjects())
			}
		}
		high, err := r.source.GTIDSet(ctx)
		if err != nil {
			return End{}, err
		}
		if r.opts.WaitHigh != nil {
			waitCtx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
			err := r.opts.WaitHigh(waitCtx, high)
			cancel()
			if err != nil {
				return End{}, err
			}
		}
		r.end = &End{Version: 1, PipelineID: r.opts.PipelineID, RunID: r.opts.RunID, LowGTIDSet: r.start.LowGTIDSet,
			HighGTIDSet: high, TotalChunks: r.plan.Total}
		if r.opts.Recovery != nil {
			r.end.Recovery = proto.Clone(r.opts.Recovery).(*iproto.SnapshotRecoveryIdentity)
		}
		if r.opts.RecoveryTables != nil {
			r.end.RecoveryTables = r.opts.RecoveryTables()
		}
		if r.planner.extensions > 0 {
			r.end.InitialTotalChunks = r.planner.initialTotal
			r.end.PlanExtensionSequence = r.planner.extensions
		}
		if err := r.validateEnd(*r.end); err != nil {
			r.end = nil
			return End{}, err
		}
	}
	if err := r.createRecord(ctx, "end", *r.end); err != nil {
		return End{}, err
	}
	var committed End
	if err := r.store.Get(ctx, "snapshot_runs", r.recordID("end"), &committed); err != nil {
		return End{}, err
	}
	if err := r.validateEnd(committed); err != nil {
		return End{}, err
	}
	r.end = &committed
	sendCtx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	if err := r.sink.SendSnapshotEnd(sendCtx, endMessage(committed)); err != nil {
		return End{}, err
	}
	return committed, nil
}

func (r *Runner) validateStart(start Start) error {
	if start.Version != 1 || start.PipelineID != r.opts.PipelineID || start.RunID != r.opts.RunID ||
		start.SnapshotUUID != r.opts.SnapshotUUID || start.TotalChunks != r.planner.initialTotal || start.ReadEndpointUUID != r.opts.ReadEndpointUUID {
		return fmt.Errorf("snapshot: stored start identity disagrees with run")
	}
	if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, start.LowGTIDSet); err != nil {
		return fmt.Errorf("snapshot: invalid stored low GTID set")
	}
	return nil
}

func (r *Runner) validateEnd(end End) error {
	if !proto.Equal(end.Recovery, r.opts.Recovery) {
		return fmt.Errorf("snapshot: recovery end identity mismatch")
	}
	if err := iproto.ValidateSnapshotRecoveryTables(end.RecoveryTables, true); err != nil {
		return err
	}
	if r.planner.extensions == 0 && (end.InitialTotalChunks != 0 || end.PlanExtensionSequence != 0) || r.planner.extensions > 0 && (end.InitialTotalChunks != r.planner.initialTotal || end.PlanExtensionSequence != r.planner.extensions) {
		return fmt.Errorf("snapshot: stored end extension identity mismatch")
	}
	if end.Version != 1 || end.PipelineID != r.opts.PipelineID || end.RunID != r.opts.RunID ||
		end.LowGTIDSet != r.start.LowGTIDSet || end.TotalChunks != r.plan.Total {
		return fmt.Errorf("snapshot: stored end identity disagrees with run")
	}
	lo, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.LowGTIDSet)
	if err != nil {
		return fmt.Errorf("snapshot: invalid end low GTID set")
	}
	hi, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.HighGTIDSet)
	if err != nil || !hi.Contain(lo) {
		return fmt.Errorf("snapshot: end GTID set does not contain low watermark")
	}
	return nil
}

// SetTelemetryReport transfers cumulative values to an immutable module mirror.
// The owner must attach the callback before Serve starts the Runner workflow.
// Restored chunks are excluded; only newly confirmed and durable chunks count.
func (r *Runner) SetTelemetryReport(report func(uint64, uint64)) { r.reportCounters = report }

func (r *Runner) appendProgress(tables []Table) {
	seen := make(map[[2]string]bool, len(r.progress))
	for _, progress := range r.progress {
		seen[[2]string{progress.GetSchema(), progress.GetTable()}] = true
	}
	for _, table := range tables {
		if seen[[2]string{table.Schema, table.Name}] {
			continue
		}
		var total uint64
		for _, planned := range r.plan.Tables {
			if planned.Schema == table.Schema && planned.Table == table.Name {
				total = planned.ChunkCount
				break
			}
		}
		done := uint64(0)
		r.progress = append(r.progress, &iproto.TableProgress{Schema: table.Schema, Table: table.Name, EstimatedRows: int64(min(table.EstimatedRows, uint64(1<<63-1))), TotalChunks: &total, DoneChunks: &done, Status: iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_PENDING})
	}
}
