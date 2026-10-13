package main

import (
	"context"
	"log/slog"
	"sync"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/lifecycle"
	"github.com/xmapst/xdts/internal/manager/config"
	"github.com/xmapst/xdts/internal/manager/dispatch"
	"github.com/xmapst/xdts/internal/manager/reclaim"
	"github.com/xmapst/xdts/internal/manager/task"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type backgroundTasks struct {
	dispatcher *dispatch.Dispatcher
	reclaim    *reclaim.Loop
	inspection *task.InspectionLoop
}

func (b *backgroundTasks) start(cfg *config.Config, db *gorm.DB, resources, transportResources *lifecycle.Scope) {
	ctx := context.Background()
	partitionMu := &sync.Mutex{}
	partitions := task.New(task.Options{DB: db, PartitionMu: partitionMu, Interval: cfg.BackgroundTasks.PartitionCheckInterval})
	// Precreate observation partitions before ingest can acknowledge reports.
	if err := partitions.RunOnce(ctx); err != nil {
		slog.Error("partition precreation initial run failed", slog.Any("error", err))
	}
	resources.Go(ctx, partitions.Run)
	retention := task.NewRetentionLoop(task.RetentionOptions{
		DB: db, PartitionMu: partitionMu, RuntimeSettings: true,
		Logs: cfg.Retention.Logs, Metrics: cfg.Retention.Metrics,
		Events: cfg.Retention.Events, Conflicts: cfg.Retention.Conflicts,
	})
	resources.Go(ctx, retention.Run)
	b.reclaim = reclaim.New(reclaim.Options{DB: db, Interval: cfg.BackgroundTasks.ReclaimInterval})
	resources.Go(ctx, b.reclaim.Run)
	b.inspection = task.NewInspectionLoop(task.InspectionOptions{
		DB: db, Interval: cfg.BackgroundTasks.InspectionInterval,
		EndpointProbeEnabled: cfg.BackgroundTasks.EndpointProbeEnabled,
	})
	resources.Go(ctx, b.inspection.Run)
	alerts := task.NewAlertLoop(task.AlertOptions{DB: db, Interval: cfg.BackgroundTasks.AlertInterval})
	resources.Go(ctx, alerts.Run)
	b.dispatcher = dispatch.New(dispatch.Options{DB: db})
	b.reclaim.SetDispatcher(b.dispatcher)
	reconciler := dispatch.NewReconciler(b.dispatcher, dispatch.ReconcileOptions{Interval: cfg.Dispatch.ReconcileInterval})
	resources.Go(ctx, reconciler.Run)
	instanceID := uuidx.New().String()
	transportResources.Go(ctx, func(ctx context.Context) {
		boot := connectAMQPWithRetry(ctx, cfg.AMQP, db, instanceID, b.dispatcher)
		if boot == nil {
			return
		}
		// The retry worker owns the connection until shutdown cancels and joins it.
		defer boot.close(cfg.HTTP.ShutdownTimeout)
		b.reclaim.SetQueues(boot.reclaimQueues)
		b.inspection.SetDispatcher(task.NewRPCProbeDispatcher(boot.rpcClient))
		b.dispatcher.SetPublisher(boot.dispatchPub)
		b.dispatcher.SetRPCClient(boot.rpcClient)
		<-ctx.Done()
	})
}
