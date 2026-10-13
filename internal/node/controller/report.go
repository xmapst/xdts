package controller

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/safeerror"
)

// reportStatus 使用配置的 run_id 标识运行轮次；只有重新初始化才递增。
// 运行状态由 state 参数独立表达。
func buildPipelineStatus(item *iproto.PipelineConfigItem, state iproto.PipelineState, lastError string, nowMs int64) *iproto.PipelineStatus {
	return &iproto.PipelineStatus{
		PipelineId:    item.GetPipelineId(),
		Role:          item.GetRole(),
		State:         state,
		ConfigVersion: item.GetConfigVersion(),
		RunId:         item.GetCommon().GetRunId(),
		LastError:     safeerror.PublicText(lastError),
		TsMs:          nowMs,
	}
}

// reportPipelineStatus reports the current observed state of each local role.
// Only completed lifecycle transitions establish running, stopped or paused state.
func (m *Module) reportPipelineStatus(item *iproto.PipelineConfigItem, state iproto.PipelineState, lastError string) {
	status := buildPipelineStatus(item, state, lastError, m.now().UnixMilli())
	status.RuntimeInstanceId = m.runtimeInstances[keyOf(item)].InstanceID
	m.statusReporter(status)
}

// prodReportPipelineStatus 是 Module.statusReporter 字段的生产实现（New()
// 在 m.statusReporter 为空时绑定，见该函数），真正把状态投递给 reporter 模块。
func prodReportPipelineStatus(m *Module) func(status *iproto.PipelineStatus) {
	return func(status *iproto.PipelineStatus) {
		m.Cast(reporter.ModuleName, reporter.StatusReport{Status: status})
	}
}
