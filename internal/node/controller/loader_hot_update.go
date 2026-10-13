package controller

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/pipeline/snapreader"
	"github.com/xmapst/xdts/internal/node/pipeline/snapwriter"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
)

func (l *prodLoader) CanHotUpdate(item *iproto.PipelineConfigItem) bool {
	switch item.GetRole() {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		return true
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		return true
	default:
		return false
	}
}

func (l *prodLoader) HotUpdate(ctx context.Context, item *iproto.PipelineConfigItem, instance string) error {
	if !l.CanHotUpdate(item) {
		return fmt.Errorf("controller: runtime update role is unsupported")
	}
	// Source translation is pure. Target candidates are prepared in this IO
	// worker and transferred only at the receiver's drained runtime boundary.
	var req any
	module := binlogin.ModuleName(item.GetPipelineId())
	if item.GetRole() == iproto.PipelineRole_PIPELINE_ROLE_SOURCE {
		opts, _, err := buildSourceOptions(item, l.nc, nil)
		if err != nil {
			return err
		}
		update := iproto.SourceRuntimeUpdate{Context: ctx, PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(), InstanceID: instance,
			Config: iproto.SourceRuntimeConfig{ConfigVersion: item.GetConfigVersion(), TableFilter: opts.TableFilter, ColumnFilter: opts.ColumnFilter, RowFilter: opts.RowFilter, Transforms: opts.Transforms, DDLColumnPolicy: iproto.CloneDDLColumnPolicy(opts.DDLColumnPolicy), OriginResolver: opts.OriginResolver, OriginFilter: opts.OriginFilter, TargetCenter: opts.TargetCenter, DatabaseRoute: opts.DatabaseRoute, MaxOriginHops: opts.MaxOriginHops, DropUnknownOrigin: opts.DropUnknownOrigin, RowsPerSec: opts.IncrementalRowsPerSec, BytesPerSec: opts.IncrementalBytesPerSec, PublicationWindow: iproto.ClonePublicationWindow(opts.PublicationWindow), ShadowTablePolicy: iproto.CloneShadowTablePolicy(opts.ShadowTablePolicy)}}
		req = update
		if needsSnapshot(item) {
			params := item.GetSource().GetSnapshot()
			if params == nil {
				return fmt.Errorf("controller: snapshot parameters required")
			}
			concurrency, rows, bytes := params.GetReadConcurrency(), uint64(params.GetMaxTxnRows()), uint64(params.GetMaxTxnBytes())
			if concurrency == 0 {
				concurrency = 2
			}
			if rows == 0 {
				rows = 10000
			}
			if bytes == 0 {
				bytes = 16 << 20
			}
			req = iproto.SnapshotSourceRuntimeUpdate{Update: update, ReadConcurrency: concurrency, MaxRows: rows, MaxBytes: bytes, RowsPerSec: params.GetRateLimit().GetRowsPerSec(), BytesPerSec: params.GetRateLimit().GetBytesPerSec()}
			module = snapreader.ModuleName(item.GetPipelineId())
		}
	} else {
		opts, inOpts, err := buildTargetOptions(item, l.nc)
		if err != nil {
			return err
		}
		config := iproto.TargetRuntimeConfig{ConfigVersion: item.GetConfigVersion(), Schemas: opts.Mapping.Schemas, Tables: make(map[iproto.TargetSourceTable]string), Columns: make(map[iproto.TargetSourceTable]map[string]string), MaxOriginHops: inOpts.MaxOriginHops, DatabaseRoute: inOpts.DatabaseRoute, ErrorPolicy: inOpts.ErrorPolicy, Transforms: opts.Transforms, SourceCaseSensitive: opts.SchemaOptions.SourceCaseSensitive, TargetCaseSensitive: item.GetTarget().GetEndpoint().GetLowerCaseTableNames() == iproto.LowerCaseTableNames_LOWER_CASE_TABLE_NAMES_SENSITIVE, ShadowTables: iproto.CloneShadowTables(item.GetTarget().GetShadowTables())}
		config.WritePolicy = item.GetTarget().GetWritePolicy()
		config.PublicationWindow = iproto.ClonePublicationWindow(inOpts.PublicationWindow)
		for from, to := range opts.Mapping.Tables {
			config.Tables[iproto.TargetSourceTable{Schema: from.Schema, Table: from.Table}] = to
		}
		for from, to := range opts.Mapping.Columns {
			config.Columns[iproto.TargetSourceTable{Schema: from.Schema, Table: from.Table}] = to
		}
		update := iproto.TargetRuntimeUpdate{Context: ctx, PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(), InstanceID: instance, Config: config}
		candidate, err := mysqlout.New(ctx, opts)
		if err != nil {
			return err
		}
		update.Resource = iproto.NewPreparedTargetResource(candidate)
		defer func(cleanup func() error) { _ = cleanup() }(update.Resource.Finish)
		req = update
		module = amqpin.ModuleName(item.GetPipelineId())
		if targetNeedsSnapshot(item) {
			maxRows, maxBytes := uint64(item.GetTarget().GetSnapshotMaxTxnRows()), uint64(item.GetTarget().GetSnapshotMaxTxnBytes())
			if maxRows == 0 {
				maxRows = 10000
			}
			if maxBytes == 0 {
				maxBytes = 16 << 20
			}
			concurrency := item.GetTarget().GetSnapshotWriteConcurrency()
			if concurrency == 0 {
				concurrency = 2
			}
			if concurrency > 128 {
				return fmt.Errorf("controller: snapshot write concurrency exceeds 128")
			}
			req = iproto.SnapshotTargetRuntimeUpdate{Update: update, MaxRows: maxRows, MaxBytes: maxBytes, WriteConcurrency: concurrency}
			module = snapwriter.ModuleName(item.GetPipelineId())
		}
	}
	client := chanrpc.NewClient()
	defer client.Close()
	// This method runs only in Controller's AsyncGo worker. Delivery uses the
	// framework's asynchronous request path and drains its own response queue.
	response := make(chan error, 1)
	if err := client.AsyncCall(xhive.ChanRPC(module), req, func(ret *chanrpc.RetInfo) { response <- ret.Err }); err != nil {
		return err
	}
	for {
		select {
		case <-client.NotEmpty():
			if ret, ok := client.Pop(); ok {
				client.AsyncCallback(ret)
				return <-response
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
