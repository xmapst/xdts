package task

import (
	"context"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
)

type precheckSideOutcome struct {
	result            *iproto.PrecheckResult
	err               error
	started, finished time.Time
}

// RunPipelinePrecheck is a bounded cold path. The pending batch is committed
// before external IO; only the owning request combines independently owned
// results. A cancellation leaves the pending batch closed to authorization.
func (l *InspectionLoop) RunPipelinePrecheck(ctx context.Context, id, user uuidx.UUID) (*repo.PrecheckState, error) {
	if l.db == nil || l.precheckSlots == nil || user.IsZero() {
		return nil, ErrProbeUnavailable
	}
	if _, ok := l.dispatcherSnapshot().(iPrecheckDispatcher); !ok {
		return nil, ErrProbeUnavailable
	}
	select {
	case l.precheckSlots <- struct{}{}:
		defer func() { <-l.precheckSlots }()
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return nil, ErrProbeQueueFull
	}
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	p, err := repo.GetPipeline(ctx, l.db, id)
	if err != nil {
		return nil, err
	}
	// A paused pipeline also has no active data consumers. It needs fresh
	// checks when startup allocated identities or configuration changed;
	// otherwise RESUME is gated forever by an obsolete precheck digest.
	inactive := func(state model.ActualState) bool {
		if p.DesiredState == model.DesiredStatePaused {
			return state == model.ActualStatePaused
		}
		return p.DesiredState == model.DesiredStateStopped && (state == model.ActualStateStopped || state == model.ActualStateDraft)
	}
	if p.ReinitPending || !inactive(p.SourceActualState) || !inactive(p.TargetActualState) {
		return nil, ErrPipelinePrecheckInput
	}
	if err := repo.EnsurePipelineSnapshotRun(ctx, l.db, id); err != nil {
		return nil, err
	}
	config, err := repo.GetPrecheckConfig(ctx, l.db, id)
	if err != nil {
		return nil, err
	}
	p = &config.Pipeline
	scope, err := l.loadPrecheckScope(ctx, *p)
	if err != nil {
		return nil, err
	}
	batch, err := repo.BeginPrecheckRun(ctx, l.db, id, config.Digest, scope.required, user, time.Now())
	if err != nil {
		return nil, err
	}
	finishFailed := func() (*repo.PrecheckState, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		items := []model.PipelinePrecheck{}
		for _, req := range scope.required {
			items = append(items, model.PipelinePrecheck{Item: req.Item, Level: req.Level, Status: model.PrecheckStatusFail, Detail: model.JSON(`{"message":"检查所需范围或元数据未能完整取得，请重新预检查","code":"precheck_preparation_failed"}`)})
		}
		if err := repo.CompletePrecheckRun(ctx, l.db, id, batch.ID, items, time.Now()); err != nil {
			return nil, err
		}
		state, err := repo.GetPrecheckState(ctx, l.db, id, time.Now())
		if err == nil && state.Stored.Run.ID != batch.ID {
			return nil, repo.ErrPrecheckConfigChanged
		}
		return state, err
	}
	catalog, err := l.assignedPrecheckCatalog(ctx, scope.sourceEndpoint, scope.sourceNode)
	if err != nil {
		return finishFailed()
	}
	tables, err := precheckCatalogTables(catalog, scope.rules.Source.ObjectFilters, scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE")
	if err != nil {
		return finishFailed()
	}
	if err := scope.setTables(tables); err != nil {
		return finishFailed()
	}
	if scope.source.GetSchemaMigration() {
		scope.migration = l.precheckMigration(ctx, scope)
	}
	if scope.source.GetStartKind() == iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS {
		var pos iproto.StartPosition
		if protojson.Unmarshal(p.StartPosition, &pos) != nil {
			return finishFailed()
		}
		resolved, err := l.GetPipelinePosition(ctx, id, &iproto.PositionProbeArgs{Kind: iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS, BinlogFile: pos.SourceFile, BinlogOffset: pos.SourceOffset})
		if err != nil {
			return finishFailed()
		}
		scope.source.StartKind = iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET
		scope.source.StartGtidSet = resolved.GetGtidSet()
	}
	// Two channels preserve side ownership without concurrent writes to one map.
	sourceChannel, targetChannel := make(chan precheckSideOutcome, 1), make(chan precheckSideOutcome, 1)
	runSide := func(side iproto.PipelineRole, args *iproto.PrecheckProbeArgs, ep, node uuidx.UUID, output chan<- precheckSideOutcome) {
		started := time.Now()
		result, err := l.GetEndpointPrecheck(ctx, ep, node, id, side, args)
		output <- precheckSideOutcome{result: result, err: err, started: started, finished: time.Now()}
	}
	go runSide(iproto.PipelineRole_PIPELINE_ROLE_SOURCE, scope.source, p.SourceEndpointID, p.SourceNodeID, sourceChannel)
	go runSide(iproto.PipelineRole_PIPELINE_ROLE_TARGET, scope.target, p.TargetEndpointID, p.TargetNodeID, targetChannel)
	source, target := <-sourceChannel, <-targetChannel
	items := append(nodePrecheckItems("source", scope.required, source.result, source.err), nodePrecheckItems("target", scope.required, target.result, target.err)...)
	manager, err := l.managerPrecheckItems(ctx, *p, scope, source, target)
	if err != nil {
		return nil, err
	}
	link, linkErr := l.pipelineLink(ctx, *p, scope)
	for i := range manager {
		if manager[i].Item == "manager/PRE-09" {
			message := "目标实际数据队列收到并确认本次源端探测消息"
			if linkErr != nil {
				message = "未取得本次实际数据队列证明，请检查队列积压、消费者与联邦连接后重试"
			}
			manager[i] = precheckManagerItem("PRE-09", manager[i].Level, linkErr == nil, message, link)
		}
	}
	items = append(items, manager...)
	if err := repo.CompletePrecheckRun(ctx, l.db, id, batch.ID, items, time.Now()); err != nil {
		return nil, err
	}
	state, err := repo.GetPrecheckState(ctx, l.db, id, time.Now())
	if err != nil {
		return nil, err
	}
	if state.Stored.Run.ID != batch.ID {
		return nil, repo.ErrPrecheckConfigChanged
	}
	return state, nil
}
