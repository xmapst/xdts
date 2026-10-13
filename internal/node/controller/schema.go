package controller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/pipeline/schemamigrator"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/pkg/uuidx"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
)

// executeSchemaPlan is invoked only from a load/catchup background workflow.
// Module completion is joined before its dedicated Writer is closed. Each
// phase owns a clone of the same persisted plan and executes disjoint sequences.
func (l *prodLoader) executeSchemaPlan(ctx context.Context, item *iproto.PipelineConfigItem, opts mysqlout.Options, afterSnapshot bool) error {
	if l.nc.Store == nil {
		return fmt.Errorf("controller: migration store required")
	}
	opts.WritePolicy = nil
	writer, err := mysqlout.New(ctx, opts)
	if err != nil {
		return err
	}
	mod, err := schemamigrator.New(schemamigrator.Options{PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(), ConfigVersion: item.GetConfigVersion(), Plan: item.GetTarget().GetSchemaPlan(), AfterSnapshot: afterSnapshot, Store: l.nc.Store, Applier: writer, OperationTimeout: 30 * time.Second, Report: func(seq uint64, result error) {
		message := ""
		level := "INFO"
		if result != nil {
			message = "结构迁移执行或校验失败，请检查节点日志"
			level = "ERROR"
		}
		event := &iproto.Event{EventId: uuidx.New().String(), Kind: iproto.EventKind_EVENT_KIND_DDL, PipelineId: item.GetPipelineId(), NodeId: l.nc.NodeID, TsMs: time.Now().UnixMilli(), Level: level, Message: "结构迁移语句执行结果", Detail: &iproto.Event_Ddl{Ddl: &iproto.DDLEventDetail{Migration: &iproto.SchemaMigrationStepResult{RunId: item.GetCommon().GetRunId(), SchemaUuid: item.GetTarget().GetSchemaPlan().GetSchemaUuid(), Seq: seq, Success: result == nil, Error: message}}}}
		modRPC := xhive.ChanRPC(reporter.ModuleName)
		if modRPC != nil {
			client := chanrpc.NewClient()
			client.Cast(modRPC, reporter.EventReport{Event: event})
			client.Close()
		}
	}})
	if err != nil {
		_ = writer.Close()
		return err
	}
	results, err := xhive.AddDynamicModules(mod)
	if err != nil {
		_ = applyAddResults(results)
		_ = writer.Close()
		return err
	}
	if err := applyAddResults(results); err != nil {
		_ = writer.Close()
		return err
	}
	select {
	case result := <-mod.Done():
		err = result
	case <-ctx.Done():
		err = ctx.Err()
	}
	if !xhive.RemoveDynamicModule(schemamigrator.ModuleName(item.GetPipelineId())) {
		return fmt.Errorf("controller: migration module shutdown not confirmed")
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("schema migration phase failed", slog.String("pipeline_id", item.GetPipelineId()), slog.Bool("after_snapshot", afterSnapshot), slog.Any("error", err))
	}
	return err
}
