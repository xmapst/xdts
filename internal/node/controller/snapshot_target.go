package controller

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/pipeline/snapwriter"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xhive"
)

func targetNeedsSnapshot(item *iproto.PipelineConfigItem) bool {
	mode := item.GetTarget().GetInitMode()
	return mode == iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL || mode == iproto.InitMode_INIT_MODE_SNAPSHOT_ONLY
}

func (l *prodLoader) loadSnapshotTarget(ctx context.Context, item *iproto.PipelineConfigItem, opts mysqlout.Options, inOpts amqpin.Options) error {
	target := item.GetTarget()
	run := mysqlout.SnapshotOptions{PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(), UUID: target.GetSnapshotUuid(), MaxRows: uint64(target.GetSnapshotMaxTxnRows()), MaxBytes: uint64(target.GetSnapshotMaxTxnBytes())}
	run.AllowTableCreation = item.GetCommon().GetSnapshotTableCreationV1()
	if run.MaxRows == 0 {
		run.MaxRows = 10000
	}
	if run.MaxBytes == 0 {
		run.MaxBytes = 16 << 20
	}
	if err := run.Validate(); err != nil {
		return err
	}
	if l.nc.Store == nil {
		return fmt.Errorf("controller: snapshot target store required")
	}
	n := target.GetSnapshotWriteConcurrency()
	if n == 0 {
		n = 2
	}
	if n > 128 {
		return fmt.Errorf("controller: snapshot write concurrency exceeds 128")
	}
	if err := l.declareDataQueue(item); err != nil {
		return err
	}
	dc, err := transport.NewDeclarator(l.nc.Conn)
	if err != nil {
		return err
	}
	defer dc.Close()
	if err := dc.DeclareQueue(transport.PipelineSnapshotQueueOptions(run.PipelineID)); err != nil {
		return err
	}
	if err := dc.BindQueue(transport.ExchangeData, transport.PipelineSnapshotQueue(run.PipelineID), transport.RoutingKeyPipelineSnapshot(run.PipelineID)); err != nil {
		return err
	}
	workers := make([]*mysqlout.Writer, 0, n)
	fullOpts := opts
	fullOpts.WritePolicy = nil // full workers have their own single connection
	var incremental *mysqlout.Writer
	transferred := false
	defer func() {
		if !transferred {
			for _, w := range workers {
				_ = w.Close()
			}
			if incremental != nil {
				_ = incremental.Close()
			}
		}
	}()
	for range n {
		w, err := mysqlout.New(ctx, fullOpts)
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}
	incremental, err = mysqlout.New(ctx, opts)
	if err != nil {
		return err
	}
	var requests chan iproto.SnapshotRecoveryRequest
	var completions chan iproto.SnapshotRecoveryCompletion
	if item.GetCommon().GetSnapshotTableRecoveryV1() {
		requests = make(chan iproto.SnapshotRecoveryRequest, 1)
		completions = make(chan iproto.SnapshotRecoveryCompletion, 1)
		if err := incremental.ConfigureTableRecovery(l.nc.Store, run.PipelineID, run.RunID); err != nil {
			return err
		}
	}
	snap := snapwriter.New(snapwriter.Options{AllowPlanExtensions: item.GetCommon().GetSnapshotPlanExtensionsV1(), InstanceID: inOpts.InstanceID, Budget: l.nc.Quota.Budget(item.GetPipelineId()), Run: run, ConfigVersion: item.GetConfigVersion(), ErrorPolicy: inOpts.ErrorPolicy, Conn: l.nc.Conn, Writers: workers, Store: l.nc.Store,
		NewWriter:          func(ctx context.Context) (*mysqlout.Writer, error) { return mysqlout.New(ctx, fullOpts) },
		AllowTableRecovery: item.GetCommon().GetSnapshotTableRecoveryV1(), RecoveryRequests: requests, RecoveryCompletions: completions,
	})
	inOpts.Applier = incremental
	inOpts.AllowPlanExtensions = item.GetCommon().GetSnapshotPlanExtensionsV1()
	inOpts.SnapshotReady = snap.SnapshotDone()
	inOpts.RecoveryRequests = requests
	inOpts.RecoveryCompletions = completions
	inOpts.FullOnly = target.GetInitMode() == iproto.InitMode_INIT_MODE_SNAPSHOT_ONLY
	if target.GetSchemaPlan() != nil {
		inOpts.AfterCatchup = func(ctx context.Context) error { return l.executeSchemaPlan(ctx, item, opts, true) }
	}
	in := amqpin.New(inOpts)
	incremental.SetConflictSink(in.ReportConflict)
	results, err := xhive.AddDynamicModules(in, snap)
	if err != nil {
		_ = applyAddResults(results)
		return err
	}
	if err := applyAddResults(results); err != nil {
		return err
	}
	transferred = true
	l.mu.Lock()
	l.writers[run.PipelineID] = incremental
	l.targetSnapshots[run.PipelineID] = true
	l.mu.Unlock()
	return nil
}
