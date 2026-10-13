// Package snapwriter owns target snapshot dispatch in one background workflow.
package snapwriter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"
	"google.golang.org/protobuf/proto"
)

type IStore interface {
	Get(context.Context, string, string, any) error
	PutCritical(context.Context, store.Op) error
}

type Options struct {
	AllowPlanExtensions bool
	InstanceID          string
	Budget              *resourcequota.Budget
	Run                 mysqlout.SnapshotOptions
	ConfigVersion       uint64
	ErrorPolicy         iproto.TargetErrorPolicy
	Conn                *transport.Conn
	Writers             []*mysqlout.Writer
	NewWriter           func(context.Context) (*mysqlout.Writer, error)
	Store               IStore
	AllowTableRecovery  bool
	RecoveryRequests    chan<- iproto.SnapshotRecoveryRequest
	RecoveryCompletions chan<- iproto.SnapshotRecoveryCompletion
}

type completion struct {
	UUID              string                          `bson:"uuid"`
	PipelineID        string                          `bson:"pipeline_id"`
	RunID             uint64                          `bson:"run_id"`
	Low               string                          `bson:"low"`
	High              string                          `bson:"high"`
	Total             uint64                          `bson:"total"`
	InitialTotal      uint64                          `bson:"initial_total,omitempty"`
	ExtensionSequence uint64                          `bson:"extension_sequence,omitempty"`
	RecoveryTables    []*iproto.SnapshotRecoveryTable `bson:"recovery_tables,omitempty"`
}

func (c completion) end() *iproto.SnapshotEnd {
	return &iproto.SnapshotEnd{PipelineId: c.PipelineID, RunId: c.RunID, LowGtidSet: c.Low, HighGtidSet: c.High, TotalChunks: c.Total, InitialTotalChunks: c.InitialTotal, PlanExtensionSequence: c.ExtensionSequence, RecoveryTables: c.RecoveryTables}
}

type delivery struct {
	buffer *resourcequota.Buffer
	bytes  uint64
	chunk  *iproto.SnapshotChunk
	end    *iproto.SnapshotEnd
	probe  *iproto.Probe
	ack    *transport.AckHandle
	err    error
	run    mysqlout.SnapshotOptions
	policy iproto.TargetErrorPolicy
	begin  *iproto.SnapshotRecoveryBegin
}
type result struct {
	workerID  int
	committed bool
	delivery  delivery
	err       error
}

type Module struct {
	*xhive.Skeleton
	opts                Options
	ready               chan *iproto.SnapshotEnd
	consumer            *transport.Consumer
	telemetryInstanceID string
	telemetryMirror     atomic.Pointer[iproto.PipelineTelemetry]
	runtimeFailed       atomic.Bool
	runtimeEnded        atomic.Bool
	runtimeVersion      atomic.Uint64
	runtimeUpdates      chan snapshotRuntimeUpdate
	// Assigned after the coordinator and workers terminate; Close then owns it.
	closeWriters []*mysqlout.Writer
}

func ModuleName(id string) string { return "snap_writer." + id }
func New(opts Options) *Module {
	m := &Module{Skeleton: xhive.NewSkeleton(ModuleName(opts.Run.PipelineID)), opts: opts, ready: make(chan *iproto.SnapshotEnd, 1), telemetryInstanceID: uuidx.New().String(), runtimeUpdates: make(chan snapshotRuntimeUpdate, 1)}
	m.runtimeVersion.Store(opts.ConfigVersion)
	m.closeWriters = append([]*mysqlout.Writer(nil), opts.Writers...)
	m.telemetryMirror.Store(&iproto.PipelineTelemetry{})
	return m
}
func (m *Module) SnapshotDone() <-chan *iproto.SnapshotEnd { return m.ready }
func (m *Module) Priority() uint                           { return 60 }
func (m *Module) OnInit() error {
	if err := iproto.ValidateTargetErrorPolicy(m.opts.ErrorPolicy); err != nil {
		return err
	}
	if err := m.opts.Run.Validate(); err != nil {
		return err
	}
	if m.opts.Conn == nil || m.opts.Store == nil || len(m.opts.Writers) == 0 {
		return fmt.Errorf("snapwriter: dependencies required")
	}
	if len(m.opts.Writers) > 128 {
		return errors.New("snapwriter: write concurrency exceeds 128")
	}
	if err := m.RegisterChanRPC(iproto.SnapshotTargetRuntimeUpdate{}, m.onRuntimeUpdate); err != nil {
		return err
	}
	m.RegisterTimer("snapwriter.telemetry", func(_ int64, _ map[string]string) { m.reportTelemetry() })
	m.NewTimer("snapwriter.telemetry", time.Second, timer.WithTicker())
	return nil
}
func (m *Module) recordID() string {
	return fmt.Sprintf("%d:%s:%d", len(m.opts.Run.PipelineID), m.opts.Run.PipelineID, m.opts.Run.RunID)
}

func (m *Module) Serve(ctx context.Context) {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := m.runSafely(work); err != nil && !errors.Is(err, context.Canceled) {
			m.runtimeFailed.Store(true)
			slog.Error("snapshot target failed", slog.String("pipeline_id", m.opts.Run.PipelineID), slog.String("reason", safeerror.Public(err)))
			m.Cast(reporter.ModuleName, reporter.StatusReport{Status: &iproto.PipelineStatus{PipelineId: m.opts.Run.PipelineID, RunId: m.opts.Run.RunID, ConfigVersion: m.runtimeVersion.Load(), Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, State: iproto.PipelineState_PIPELINE_STATE_ERROR, LastError: "全量写入失败，请检查节点日志后重试", TsMs: time.Now().UnixMilli(), RuntimeInstanceId: m.opts.InstanceID}})
			if m.opts.InstanceID != "" {
				m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.Run.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_TARGET, RunID: m.opts.Run.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: iproto.PipelineState_PIPELINE_STATE_ERROR})
			}
		}
	}()
	m.Skeleton.Serve(ctx)
	cancel()
	<-done
}

func (m *Module) runSafely(ctx context.Context) (runErr error) {
	defer func() {
		if recover() != nil {
			runErr = errors.New("snapwriter: pipeline workflow panicked")
		}
	}()
	return m.run(ctx)
}
func (m *Module) OnDestroy() {}
func (m *Module) Close() error {
	var errs []error
	for _, w := range m.closeWriters {
		errs = append(errs, w.Close())
	}
	errs = append(errs, m.Skeleton.Close())
	return errors.Join(errs...)
}

// run confines completion/inflight to this coordinator. Workers own individual
// connections; delivery values are decoded afresh, never shared mutable data.
func (m *Module) run(ctx context.Context) error {
	defer func() { m.runtimeEnded.Store(true); m.rejectRuntimeUpdates() }()
	ctx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var completed *iproto.SnapshotEnd
	var resumeBegin *iproto.SnapshotRecoveryBegin
	var stored completion
	err := m.opts.Store.Get(ctx, "snapshot_targets", m.recordID(), &stored)
	if err == nil {
		if stored.UUID != m.opts.Run.UUID {
			return fmt.Errorf("snapwriter: stored UUID mismatch")
		}
		completed = stored.end()
		if err := m.validateEnd(completed); err != nil {
			return err
		}
		if err := m.verifyChunks(ctx, completed); err != nil {
			return err
		}
		ready := proto.Clone(completed).(*iproto.SnapshotEnd)
		uuids := map[string]bool{m.opts.Run.UUID: true}
		if m.opts.AllowTableRecovery {
			for sequence := uint64(1); sequence <= 4096; sequence++ {
				var rec recoveryRecord
				err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), sequence), &rec)
				if errors.Is(err, xdoc.ErrNotFound) {
					break
				}
				if err != nil {
					return err
				}
				begin := new(iproto.SnapshotRecoveryBegin)
				if err := proto.Unmarshal(rec.Begin, begin); err != nil {
					return err
				}
				if err := validateRecoveryBeginAuthority(begin, ready, sequence, uuids); err != nil {
					return err
				}
				uuids[begin.Recovery.Uuid] = true
				if begin.Recovery.Sequence != sequence || begin.RunId != m.opts.Run.RunID || begin.PipelineId != m.opts.Run.PipelineID {
					return fmt.Errorf("snapwriter: stored recovery sequence mismatch")
				}
				if len(rec.End) == 0 {
					resumeBegin = begin
					break
				}
				end := new(iproto.SnapshotEnd)
				if err := proto.Unmarshal(rec.End, end); err != nil {
					return err
				}
				if !proto.Equal(end.Recovery, begin.Recovery) || end.LowGtidSet != begin.LowGtidSet || end.PipelineId != begin.PipelineId || end.RunId != begin.RunId {
					return fmt.Errorf("snapwriter: stored recovery end mismatch")
				}
				if err := validateRecoveryEnd(end); err != nil {
					return err
				}
				set, err := m.opts.Writers[0].GTIDSet(ctx)
				if err != nil {
					return err
				}
				if err := verifyRecoveryChunkRange(set, begin.Recovery.Uuid, end.TotalChunks); err != nil {
					return err
				}
				ready = end
			}
		}
		// This local startup value uses the latest recovery L/H, while its
		// progress totals stay those of the original initialization run. The
		// durable/wire recovery End retains its independent chunk total.
		ready.TotalChunks = completed.TotalChunks
		ready.InitialTotalChunks = completed.InitialTotalChunks
		ready.PlanExtensionSequence = completed.PlanExtensionSequence
		m.ready <- ready
	} else if !errors.Is(err, xdoc.ErrNotFound) {
		return err
	}
	owned := make(map[*resourcequota.Buffer]struct{})
	pool := m.newWorkerPool(ctx)
	defer func() {
		cancelWorkers()
		m.closeWriters = pool.stop()
		// Workers have returned; queued results and pending deliveries no
		// longer have an owner using their rows or encoding copies.
		for buffer := range owned {
			buffer.Release()
		}
	}()
	inbox := make(chan delivery)
	c, err := transport.NewConsumer(m.opts.Conn, transport.PipelineSnapshotQueue(m.opts.Run.PipelineID), transport.WithConcurrency(1), transport.WithPrefetch(129), transport.WithConsumerName(ModuleName(m.opts.Run.PipelineID)))
	if err != nil {
		return err
	}
	m.consumer = c
	// Cancel handler submissions before closing its consumer, even on failures.
	handlerCtx, stopHandler := context.WithCancel(ctx)
	defer func() { stopHandler(); c.Close() }()
	if err := c.Run(func(msg *transport.Message) {
		d := m.decode(msg)
		select {
		case inbox <- d:
		case <-handlerCtx.Done():
			d.buffer.Release()
		}
	}); err != nil {
		return err
	}
	var pending *delivery
	var recoveryBegin *delivery
	recoverySequence := uint64(0)
	if completed != nil {
		// ready may have advanced past the original completion on restart.
		if resumeBegin != nil {
			recoverySequence = resumeBegin.Recovery.Sequence - 1
		} else {
			for sequence := uint64(1); sequence <= 4096; sequence++ {
				var record recoveryRecord
				if err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), sequence), &record); errors.Is(err, xdoc.ErrNotFound) {
					break
				} else if err != nil {
					return err
				}
				if len(record.End) == 0 {
					break
				}
				recoverySequence = sequence
			}
		}
	}
	if resumeBegin != nil {
		recoveryBegin = &delivery{begin: resumeBegin}
	}
	inflight := 0
	runtimeRun := m.opts.Run
	runtimePolicy := m.opts.ErrorPolicy
	var update *snapshotRuntimeUpdate
	defer func() {
		if update != nil {
			_ = update.reply.Ret(&chanrpc.RetInfo{Err: errors.New("snapwriter: workflow ended before runtime update")})
		}
	}()
	telemetry := iproto.PipelineTelemetry{}
	for {
		if recoveryBegin != nil && inflight == 0 {
			if completed == nil {
				return fmt.Errorf("snapwriter: recovery before initial completion")
			}
			err := m.runRecovery(ctx, *recoveryBegin, inbox, pool.writers()[0], runtimeRun)
			recoverySequence = recoveryBegin.begin.Recovery.Sequence
			recoveryBegin.buffer.Release()
			delete(owned, recoveryBegin.buffer)
			recoveryBegin = nil
			if err != nil {
				return err
			}
		}
		if update != nil && inflight == 0 {
			limit := int(update.request.WriteConcurrency)
			if limit == 0 {
				limit = pool.limit
			}
			prepare, cancel := context.WithCancel(update.request.Update.Context)
			stop := context.AfterFunc(ctx, cancel)
			additional, err := m.prepareWorkers(prepare, pool, limit)
			if err == nil {
				err = m.updateRuntime(ctx, update.request, append(pool.writers(), additional...))
			}
			stop()
			cancel()
			if err == nil {
				for _, writer := range additional {
					pool.add(ctx, m, writer)
				}
				pool.limit = limit
				runtimeRun.MaxRows, runtimeRun.MaxBytes = update.request.MaxRows, update.request.MaxBytes
				runtimePolicy = update.request.Update.Config.ErrorPolicy
			} else {
				for _, writer := range additional {
					_ = writer.Close()
				}
			}
			_ = update.reply.Ret(&chanrpc.RetInfo{Err: err})
			update = nil
			if _, ok := errors.AsType[runtimeCommitFailure](err); ok {
				return err
			}
		}
		if pending != nil && inflight == 0 {
			end := pending.end
			if err := m.verifyChunks(ctx, end); err != nil {
				return err
			}
			if completed == nil {
				doc := completion{UUID: m.opts.Run.UUID, PipelineID: end.GetPipelineId(), RunID: end.GetRunId(), Low: end.GetLowGtidSet(), High: end.GetHighGtidSet(), Total: end.GetTotalChunks(), InitialTotal: end.GetInitialTotalChunks(), ExtensionSequence: end.GetPlanExtensionSequence(), RecoveryTables: end.RecoveryTables}
				commit, cancel := context.WithTimeout(ctx, 30*time.Second)
				err := m.opts.Store.PutCritical(commit, store.Op{Collection: "snapshot_targets", ID: m.recordID(), Doc: doc, InsertOnly: true, Critical: true})
				cancel()
				if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
					return err
				}
				var actual completion
				if err := m.opts.Store.Get(ctx, "snapshot_targets", m.recordID(), &actual); err != nil {
					return err
				}
				if actual.UUID != m.opts.Run.UUID || !proto.Equal(actual.end(), end) {
					return fmt.Errorf("snapwriter: competing completion mismatch")
				}
				completed = proto.Clone(end).(*iproto.SnapshotEnd)
				m.ready <- proto.Clone(completed).(*iproto.SnapshotEnd)
			}
			if err := pending.ack.Ack(); err != nil {
				return err
			}
			pending.buffer.Release()
			delete(owned, pending.buffer)
			pending = nil
		}
		var receive <-chan delivery = inbox
		if pool.idleWorker() < 0 || pending != nil || update != nil || recoveryBegin != nil {
			receive = nil
		}
		var updates <-chan snapshotRuntimeUpdate = m.runtimeUpdates
		if update != nil {
			updates = nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case u := <-updates:
			update = &u
		case r := <-pool.results:
			pool.workers[r.workerID].busy = false
			inflight--
			if r.err != nil {
				telemetry.Errors++
				m.captureTelemetry(telemetry)
				return r.err
			}
			if r.committed {
				telemetry.Rows += uint64(len(r.delivery.chunk.GetRows()))
				telemetry.Txns++
			}
			m.captureTelemetry(telemetry)
			if err := r.delivery.ack.Ack(); err != nil {
				return err
			}
			r.delivery.buffer.Release()
			delete(owned, r.delivery.buffer)
		case d := <-receive:
			if d.buffer != nil {
				owned[d.buffer] = struct{}{}
			}
			old, err := m.checkDeliveryRun(d)
			if err != nil {
				return err
			}
			if old {
				if err := d.ack.Ack(); err != nil {
					return err
				}
				d.buffer.Release()
				delete(owned, d.buffer)
				continue
			}
			telemetry.Bytes += d.bytes
			m.captureTelemetry(telemetry)
			if d.probe != nil {
				// A preflight cancelled after publish can leave a probe queued.
				// It carries no rows or completion state; acknowledge it only
				// after validating its local pipeline/run identity.
				if err := d.ack.Ack(); err != nil {
					return err
				}
				d.buffer.Release()
				delete(owned, d.buffer)
				continue
			}
			if d.begin != nil {
				if d.begin.Recovery.Sequence <= recoverySequence {
					var prior recoveryRecord
					if err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), d.begin.Recovery.Sequence), &prior); err != nil {
						return err
					}
					old := new(iproto.SnapshotRecoveryBegin)
					if proto.Unmarshal(prior.Begin, old) != nil || !proto.Equal(old, d.begin) || len(prior.End) == 0 {
						return fmt.Errorf("snapwriter: duplicate recovery authority mismatch")
					}
					if err := d.ack.Ack(); err != nil {
						return err
					}
					d.buffer.Release()
					delete(owned, d.buffer)
					continue
				}
				if d.begin.Recovery.Sequence != recoverySequence+1 {
					return fmt.Errorf("snapwriter: recovery sequence gap")
				}
				recoveryBegin = &d
				continue
			}
			if identity := d.chunk.GetRecovery(); identity != nil || d.end.GetRecovery() != nil {
				if identity == nil {
					identity = d.end.Recovery
				}
				var prior recoveryRecord
				if identity.Sequence > recoverySequence {
					return fmt.Errorf("snapwriter: recovery payload before begin")
				}
				if err := m.opts.Store.Get(ctx, "snapshot_targets", fmt.Sprintf("%s.recovery.%d", m.recordID(), identity.Sequence), &prior); err != nil {
					return err
				}
				begin := new(iproto.SnapshotRecoveryBegin)
				end := new(iproto.SnapshotEnd)
				if proto.Unmarshal(prior.Begin, begin) != nil || proto.Unmarshal(prior.End, end) != nil || !proto.Equal(identity, begin.Recovery) {
					return fmt.Errorf("snapwriter: duplicate recovery payload identity mismatch")
				}
				if d.end != nil && !proto.Equal(d.end, end) || d.chunk != nil && d.chunk.ChunkNo > end.TotalChunks {
					return fmt.Errorf("snapwriter: duplicate recovery payload range mismatch")
				}
				if err := d.ack.Ack(); err != nil {
					return err
				}
				d.buffer.Release()
				delete(owned, d.buffer)
				continue
			}
			if d.end != nil {
				if err := m.validateEnd(d.end); err != nil {
					return err
				}
				if completed != nil && !proto.Equal(completed, d.end) {
					return fmt.Errorf("snapwriter: different end after completion")
				}
				pending = &d
			} else {
				if completed != nil && d.chunk.GetChunkNo() > completed.GetTotalChunks() {
					return fmt.Errorf("snapwriter: chunk beyond completed plan")
				}
				d.run, d.policy = runtimeRun, runtimePolicy
				worker := pool.idleWorker()
				select {
				// Each worker receives its own immutable transaction limits.
				case pool.workers[worker].jobs <- d:
					pool.workers[worker].busy = true
					inflight++
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	}
}

func (m *Module) applyChunk(ctx context.Context, w *mysqlout.Writer, chunk *iproto.SnapshotChunk) (committed bool, applyErr error) {
	return m.applyChunkRun(ctx, w, m.opts.Run, chunk)
}

func (m *Module) applyChunkRun(ctx context.Context, w *mysqlout.Writer, run mysqlout.SnapshotOptions, chunk *iproto.SnapshotChunk) (committed bool, applyErr error) {
	return m.applyChunkPolicy(ctx, w, run, m.opts.ErrorPolicy, chunk)
}

func (m *Module) applyChunkPolicy(ctx context.Context, w *mysqlout.Writer, run mysqlout.SnapshotOptions, policy iproto.TargetErrorPolicy, chunk *iproto.SnapshotChunk) (committed bool, applyErr error) {
	defer func() {
		if recover() != nil {
			applyErr = errors.New("snapwriter: chunk writer panicked")
		}
	}()
	retries, initial, maximum := uint32(4), 100*time.Millisecond, 30*time.Second
	if policy.Configured {
		retries, initial, maximum = policy.Retries, policy.InitialBackoff, policy.MaxBackoff
	}
	delay := initial
	for attempt := uint32(0); ; attempt++ {
		committed, err := m.applyChunkRunAttempt(ctx, w, run, chunk)
		if err == nil || !mysqlout.IsRetryable(err) || attempt == retries {
			return committed, err
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
		if delay > maximum/2 {
			delay = maximum
		} else {
			delay *= 2
		}
	}
}

func (m *Module) applyChunkRunAttempt(ctx context.Context, w *mysqlout.Writer, run mysqlout.SnapshotOptions, chunk *iproto.SnapshotChunk) (bool, error) {
	release, err := m.opts.Budget.AcquireSnapshot(ctx)
	if err != nil {
		return false, err
	}
	defer release()
	return w.ApplySnapshotChunkWithResult(ctx, run, chunk)
}

func decode(msg *transport.Message) delivery {
	return (&Module{}).decode(msg)
}

func (m *Module) decode(msg *transport.Message) (d delivery) {
	d = delivery{ack: msg.Ack}
	defer func() {
		if recover() != nil {
			d.err = errors.New("snapwriter: message decoding panicked")
		}
		if d.err != nil {
			d.buffer.Release()
			d.buffer = nil
		}
	}()
	env, err := msg.Envelope()
	if err != nil {
		d.err = err
		return d
	}
	d.bytes = uint64(len(env.Body))
	switch env.Properties.Type {
	case iproto.MessageTypeDataProbe:
		d.probe = new(iproto.Probe)
		d.buffer, d.err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, d.probe)
	case iproto.MessageTypeSnapshotChunk:
		d.chunk = new(iproto.SnapshotChunk)
		d.buffer, d.err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, d.chunk)
	case iproto.MessageTypeSnapshotEnd:
		d.end = new(iproto.SnapshotEnd)
		d.buffer, d.err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, d.end)
	case iproto.MessageTypeSnapshotRecoveryBegin:
		d.begin = new(iproto.SnapshotRecoveryBegin)
		d.buffer, d.err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, d.begin)
	default:
		d.err = fmt.Errorf("snapwriter: invalid snapshot message type")
	}
	id := d.chunk.GetPipelineId()
	run := d.chunk.GetRunId()
	if d.end != nil {
		id = d.end.GetPipelineId()
		run = d.end.GetRunId()
	}
	if d.probe != nil {
		id, run = d.probe.GetPipelineId(), d.probe.GetRunId()
	}
	if d.begin != nil {
		id, run = d.begin.PipelineId, d.begin.RunId
	}
	if header, ok := env.HeaderString(iproto.HeaderPipelineID); ok && header != "" && header != id {
		d.err = fmt.Errorf("snapwriter: pipeline header mismatch")
	}
	if _, exists := env.Headers[iproto.HeaderRunID]; exists {
		if value, ok := iproto.HeaderUint64FromString(env.Headers, iproto.HeaderRunID); !ok || value != run {
			d.err = fmt.Errorf("snapwriter: run header mismatch")
		}
	}
	return d
}

func (m *Module) validateEnd(end *iproto.SnapshotEnd) error {
	if end.GetRecovery() != nil || len(end.GetRecoveryTables()) > 0 && !m.opts.AllowTableRecovery {
		return fmt.Errorf("snapwriter: original completion has unnegotiated recovery fields")
	}
	if err := iproto.ValidateSnapshotRecoveryTables(end.GetRecoveryTables(), true); err != nil {
		return err
	}
	if end.GetPlanExtensionSequence() > 0 && !m.opts.AllowPlanExtensions {
		return fmt.Errorf("snapwriter: snapshot extensions were not negotiated")
	}
	if end.GetPipelineId() != m.opts.Run.PipelineID || end.GetRunId() != m.opts.Run.RunID || end.GetTotalChunks() == 0 || end.GetTotalChunks() > 1<<63-2 {
		return fmt.Errorf("snapwriter: invalid end identity or total")
	}
	return validateEndWatermarks(end)
}
func (m *Module) verifyChunks(ctx context.Context, end *iproto.SnapshotEnd) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	release, err := m.opts.Budget.AcquireSnapshot(ctx)
	if err != nil {
		return err
	}
	defer release()
	set, err := m.opts.Writers[0].GTIDSet(ctx)
	if err != nil {
		return err
	}
	actual, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	want, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, fmt.Sprintf("%s:1-%d", m.opts.Run.UUID, end.GetTotalChunks()))
	if !actual.Contain(want) {
		return fmt.Errorf("snapwriter: end arrived with missing target chunks")
	}
	return nil
}
