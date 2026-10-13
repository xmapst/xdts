package controller

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xhive/chanrpc"
)

func (m *Module) onRuntimeStatus(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	status, ok := ci.Request.(iproto.PipelineRuntimeStatus)
	if ok {
		m.recordRuntimeStatus(status)
	}
	return nil
}

func (m *Module) recordRuntimeStatus(status iproto.PipelineRuntimeStatus) {
	key := appliedKey{pipelineID: status.PipelineID, role: status.Role}
	current, ok := m.runtimeInstances[key]
	// A live session can emit ERROR immediately after switching its plan,
	// before the Controller's completion callback is scheduled. Both versions
	// belong to this same instance during the authorized transition.
	versionMatches := current.ConfigVersion == status.ConfigVersion
	if updating, exists := m.runtimeUpdateVersions[key]; exists && updating == status.ConfigVersion {
		versionMatches = true
	}
	if !ok || current.InstanceID == "" || current.InstanceID != status.InstanceID || current.RunID != status.RunID || !versionMatches || m.identityRetired {
		return
	}
	if _, deleted := m.deletedRoleVersions[key]; deleted || m.deletingRoles[key] {
		return
	}
	if prior, exists := m.applied[key]; exists && !prior.running && !m.pending[key] {
		return
	}
	switch status.State {
	case iproto.PipelineState_PIPELINE_STATE_ERROR:
		m.runtimeFaults[key] = true
		delete(m.cmdPipelineResolved, cmdDedupSlot{pipelineID: status.PipelineID, cmdType: iproto.PipelineCmdType_PIPELINE_CMD_TYPE_START})
		delete(m.cmdPipelineResolved, cmdDedupSlot{pipelineID: status.PipelineID, cmdType: iproto.PipelineCmdType_PIPELINE_CMD_TYPE_RESUME})
	case iproto.PipelineState_PIPELINE_STATE_RUNNING:
		delete(m.runtimeFaults, key)
	}
}

func (m *Module) pipelineRuntimeFailed(id string) bool {
	for key, failed := range m.runtimeFaults {
		if key.pipelineID == id && failed {
			return true
		}
	}
	return false
}
