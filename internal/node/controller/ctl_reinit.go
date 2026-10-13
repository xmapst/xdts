package controller

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"uuid"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/node/snapshot"
)

func reinitUUID(value string, optional bool) bool {
	if value == "" {
		return optional
	}
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil() && id.String() == value
}

func validReinitIdentity(identity *iproto.ReinitIdentity, run uint64) bool {
	if identity == nil || run == 0 || run > math.MaxInt64 || identity.GetFromRunId() != run-1 || identity.GetNewRunId() != run || len(identity.GetDigest()) != 64 || !reinitUUID(identity.GetSnapshotUuid(), false) || !reinitUUID(identity.GetSchemaUuid(), true) || identity.GetSchemaUuid() == identity.GetSnapshotUuid() {
		return false
	}
	_, err := hex.DecodeString(identity.GetDigest())
	return err == nil
}

func sameReinitIdentity(a, b *iproto.ReinitIdentity) bool {
	if a == nil || b == nil {
		return a == b
	}
	a = proto.Clone(a).(*iproto.ReinitIdentity)
	b = proto.Clone(b).(*iproto.ReinitIdentity)
	a.CmdSeq, b.CmdSeq = 0, 0
	return proto.Equal(a, b)
}

func validateReinitItem(item *iproto.PipelineConfigItem, cmd *iproto.PipelineCmd) error {
	identity, args := item.GetCommon().GetReinitIdentity(), cmd.GetReinit()
	if args == nil || !validReinitIdentity(identity, args.GetNewRunId()) || identity.GetCmdSeq() != cmd.GetCmdSeq() || item.GetDeleted() || item.GetPipelineId() != cmd.GetPipelineId() || item.GetCommon().GetPipelineId() != cmd.GetPipelineId() || item.GetConfigVersion() != cmd.GetConfigVersion() || item.GetCommon().GetRunId() != args.GetNewRunId() || item.GetCommon().GetConfigVersion() != cmd.GetConfigVersion() || identity.GetDigest() != args.GetDigest() || identity.GetSnapshotUuid() != args.GetSnapshotUuid() || identity.GetSchemaUuid() != args.GetSchemaUuid() || !reinitUUID(args.GetPrepareUuid(), true) || (args.GetPrepareUuid() != "" && (args.GetPrepareUuid() == args.GetSnapshotUuid() || args.GetPrepareUuid() == args.GetSchemaUuid())) {
		return fmt.Errorf("reinit command and configuration identities differ")
	}
	switch item.GetRole() {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		if item.GetSource() == nil || item.GetTarget() != nil || item.GetSource().GetSnapshotUuid() != args.GetSnapshotUuid() || item.GetSource().GetInitMode() != iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL {
			return fmt.Errorf("invalid reinit source projection")
		}
		plan := item.GetSource().GetReinitPlan()
		if plan == nil || plan.GetDigest() != args.GetDigest() {
			return fmt.Errorf("invalid reinit source scope identity")
		}
		if err := snapshot.ValidateReinitPlan(plan); err != nil {
			return err
		}
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		target, plan := item.GetTarget(), item.GetTarget().GetReinitPlan()
		if target == nil || item.GetSource() != nil || target.GetSnapshotUuid() != args.GetSnapshotUuid() || target.GetInitMode() != iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL || target.GetSchemaPlan().GetSchemaUuid() != args.GetSchemaUuid() || plan == nil || plan.GetDigest() != args.GetDigest() || plan.GetPrepareUuid() != args.GetPrepareUuid() || len(plan.GetTables()) == 0 || len(plan.GetTables()) > 4000 || (plan.GetHandling() != iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_KEEP && plan.GetHandling() != iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_CLEAR) || (plan.GetHandling() == iproto.ReinitTargetHandling_REINIT_TARGET_HANDLING_CLEAR) != (plan.GetPrepareUuid() != "") {
			return fmt.Errorf("invalid reinit target projection")
		}
	default:
		return fmt.Errorf("invalid reinit role")
	}
	return nil
}

func (m *Module) replyReinitFailure(cmd *iproto.PipelineCmd, ack iAckHandle, err error) {
	key := cmdPipelineKey{ConfigVersion: cmd.GetConfigVersion(), CmdSeq: cmd.GetCmdSeq(), Type: cmd.GetType()}
	result := &iproto.CmdResult{CmdSeq: key.CmdSeq, Error: &iproto.Error{Code: "REINIT_CONFIG_INVALID", Message: safeerror.Public(err)}, Target: &iproto.CmdResult_Pipeline{Pipeline: &iproto.PipelineCmdResult{PipelineId: cmd.GetPipelineId(), ConfigVersion: key.ConfigVersion, CmdType: key.Type, Retryable: new(true), ResultingState: m.bestEffortState(cmd.GetPipelineId())}}}
	m.publishCmdResultThenAck("controller.reinitreply."+cmd.GetPipelineId(), cmd.GetPipelineId(), key, result, ack, true)
}

func (m *Module) execReinit(cmd *iproto.PipelineCmd, ack iAckHandle) {
	cmd = proto.Clone(cmd).(*iproto.PipelineCmd)
	id := cmd.GetPipelineId()
	if cmd.GetReinit() == nil || cmd.GetCmdSeq() == 0 || cmd.GetConfigVersion() == 0 {
		m.replyReinitFailure(cmd, ack, fmt.Errorf("reinit identity missing"))
		return
	}
	m.pendingPull[id] = true
	var reply *iproto.ConfigPullReply
	var pullErr error
	m.AsyncGo("controller.reinitpull."+id, func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTargetedPullTimeout)
		defer cancel()
		reply, pullErr = m.pullConfig(ctx, []string{id})
	}, func() {
		delete(m.pendingPull, id)
		m.applyReinitPullResult(cmd, reply, pullErr, ack)
	})
}

func (m *Module) applyReinitPullResult(cmd *iproto.PipelineCmd, reply *iproto.ConfigPullReply, pullErr error, ack iAckHandle) {
	if m.identityRetired {
		m.discard(ack)
		return
	}
	if pullErr != nil {
		m.replyReinitFailure(cmd, ack, pullErr)
		return
	}
	if reply == nil || reply.GetError() != nil && reply.GetError().GetCode() != "" {
		m.replyReinitFailure(cmd, ack, fmt.Errorf("reinit configuration pull failed"))
		return
	}
	var items []*iproto.PipelineConfigItem
	seen := map[iproto.PipelineRole]bool{}
	for _, item := range reply.GetConfigs() {
		if item.GetPipelineId() != cmd.GetPipelineId() {
			continue
		}
		if seen[item.GetRole()] {
			m.replyReinitFailure(cmd, ack, fmt.Errorf("duplicate reinit role"))
			return
		}
		seen[item.GetRole()] = true
		if err := validateReinitItem(item, cmd); err != nil {
			m.replyReinitFailure(cmd, ack, err)
			return
		}
		if prior, ok := m.applied[keyOf(item)]; ok && (prior.GetConfigVersion() > item.GetConfigVersion() || prior.GetCommon().GetRunId() > item.GetCommon().GetRunId()) {
			m.replyReinitFailure(cmd, ack, fmt.Errorf("reinit would regress current identity"))
			return
		}
		items = append(items, proto.Clone(item).(*iproto.PipelineConfigItem))
	}
	if len(items) == 0 {
		m.replyReinitFailure(cmd, ack, fmt.Errorf("reinit configuration missing"))
		return
	}
	key := cmdPipelineKey{ConfigVersion: cmd.GetConfigVersion(), CmdSeq: cmd.GetCmdSeq(), Type: cmd.GetType()}
	f := &cmdFanout{ack: ack, pipelineID: cmd.GetPipelineId(), key: key, pendingSides: len(items)}
	for _, item := range items {
		ctx := &cmdContext{fanout: f, role: item.GetRole()}
		prior, existed := m.applied[keyOf(item)]
		if existed && prior.running && !prior.unloadUncertain && prior.GetCommon().GetRunId() == item.GetCommon().GetRunId() && sameReinitIdentity(prior.GetCommon().GetReinitIdentity(), item.GetCommon().GetReinitIdentity()) && prior.GetConfigVersion() == item.GetConfigVersion() {
			m.completeCmdSide(ctx, nil, iproto.PipelineState_PIPELINE_STATE_RUNNING)
			continue
		}
		kind := changeLoad
		if existed {
			kind = changeRebuild
		}
		m.startApply(pipelinePlan{item: item, kind: kind, cmd: ctx})
	}
}
