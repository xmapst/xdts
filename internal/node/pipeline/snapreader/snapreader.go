package snapreader

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive/timer"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/snapshot"
	"github.com/xmapst/xhive"
)

type Options struct {
	InstanceID    string
	PipelineID    string
	RunID         uint64
	ConfigVersion uint64
	Runner        *snapshot.Runner
	Reader        *snapshot.Reader
	// DB is transferred to this module only after successful construction.
	DB *sql.DB
	// OnFailure cancels the companion incremental stream; it only signals its
	// context and does not access the other module's actor-owned state.
	OnFailure          func()
	IncrementalDone    <-chan struct{}
	IncrementalFailure func() error
}

type Module struct {
	*xhive.Skeleton
	opts                Options
	telemetryInstanceID string
	telemetryMirror     atomic.Pointer[iproto.PipelineTelemetry]
	runtimeFailed       atomic.Bool
	runtimeVersion      atomic.Uint64
	runtimeUpdating     bool
	runtimeCtx          context.Context
	runtimeCancel       context.CancelFunc
}

func ModuleName(id string) string { return "snap_reader." + id }

func New(opts Options) *Module {
	m := &Module{Skeleton: xhive.NewSkeleton(ModuleName(opts.PipelineID)), opts: opts, telemetryInstanceID: uuidx.New().String()}
	m.telemetryMirror.Store(&iproto.PipelineTelemetry{})
	m.runtimeVersion.Store(opts.ConfigVersion)
	m.runtimeCtx, m.runtimeCancel = context.WithCancel(context.Background())
	if opts.Runner != nil {
		opts.Runner.SetTelemetryReport(func(rows, chunks uint64) {
			v := iproto.PipelineTelemetry{Rows: rows, Txns: chunks}
			m.telemetryMirror.Store(&v)
		})
	}
	return m
}

func (m *Module) Priority() uint { return 60 }

func (m *Module) OnInit() error {
	if m.opts.PipelineID == "" || m.opts.Runner == nil || m.opts.DB == nil {
		return fmt.Errorf("snapreader: runner, source database and pipeline required")
	}
	if err := m.RegisterChanRPC(iproto.SnapshotSourceRuntimeUpdate{}, m.onRuntimeUpdate); err != nil {
		return err
	}
	m.RegisterTimer("snapreader.telemetry", func(_ int64, _ map[string]string) { m.reportTelemetry() })
	m.NewTimer("snapreader.telemetry", time.Second, timer.WithTicker())
	return nil
}

// The Runner owns state in one background goroutine. Its independent result
// returns to this module's event loop for reports. Serve cancellation stops all
// reads and waits for them before allowing DB.Close in the lifecycle cleanup.
func (m *Module) Serve(ctx context.Context) {
	stopRuntime := context.AfterFunc(ctx, m.runtimeCancel)
	defer stopRuntime()
	workCtx, cancel := context.WithCancel(m.runtimeCtx)
	defer cancel()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		select {
		case <-m.opts.IncrementalDone:
			cancel()
		case <-workCtx.Done():
		}
	}()
	done := make(chan struct{})
	var runErr error
	m.AsyncGo("snapreader.run", func() {
		defer close(done)
		runErr = m.runSafely(workCtx)
		if runErr != nil && m.opts.OnFailure != nil {
			m.opts.OnFailure()
		}
	}, func() {
		if errors.Is(runErr, context.Canceled) {
			if m.opts.IncrementalFailure != nil {
				runErr = m.opts.IncrementalFailure()
			}
			if runErr == nil || errors.Is(runErr, context.Canceled) {
				return
			}
		}
		// Runner waits for the source publisher to reach H before publishing
		// SnapshotEnd. This side has therefore finished its snapshot work and
		// continues live incremental publishing; target catchup is reported by
		// the target independently. Keeping the source in CATCHING_UP forever
		// would make the whole pipeline appear unfinished after convergence.
		state := iproto.PipelineState_PIPELINE_STATE_RUNNING
		message := ""
		if runErr != nil {
			m.runtimeFailed.Store(true)
			state = iproto.PipelineState_PIPELINE_STATE_ERROR
			message = "全量读取或发布失败，请检查节点日志后重试"
			if m.opts.IncrementalFailure != nil && m.opts.IncrementalFailure() != nil {
				message = "全量期间结构变更不兼容，请对受影响表重新全量"
			}
			slog.Error("snapshot run failed", slog.String("pipeline_id", m.opts.PipelineID), slog.Any("error", runErr))
		}
		m.Cast(reporter.ModuleName, reporter.StatusReport{Status: &iproto.PipelineStatus{
			PipelineId: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunId: m.opts.RunID,
			ConfigVersion: m.runtimeVersion.Load(), State: state, LastError: message,
			TsMs: time.Now().UnixMilli(), RuntimeInstanceId: m.opts.InstanceID,
		}})
		if m.opts.InstanceID != "" {
			m.Cast("controller", iproto.PipelineRuntimeStatus{PipelineID: m.opts.PipelineID, Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunID: m.opts.RunID, ConfigVersion: m.runtimeVersion.Load(), InstanceID: m.opts.InstanceID, State: state})
		}
	})
	m.Skeleton.Serve(ctx)
	cancel()
	<-done
	<-monitorDone
}

func (m *Module) runSafely(ctx context.Context) (runErr error) {
	defer func() {
		if recover() != nil {
			runErr = errors.New("snapreader: pipeline workflow panicked")
		}
	}()
	_, runErr = m.opts.Runner.RunAll(ctx)
	return runErr
}

func (m *Module) OnDestroy() { m.runtimeCancel() }

func (m *Module) Close() error {
	m.runtimeCancel()
	return errors.Join(m.opts.DB.Close(), m.Skeleton.Close())
}
