package controller

import (
	"log/slog"
	"sort"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
)

// 窗口不短：启动定向拉取超时 30s（defaultStartupPullTimeout）而心跳周期 5s，
// Manager 不可达时启动期会发出约 6 个心跳。在这里提前推一次空镜像，等于把那 6 条
// 心跳全部渲染成"承载 0 条线路"——而那正是运维盯着面板的时刻（Manager 不可达 +
// 节点刚重启）。
//
// 跨 goroutine 传切片必须让接收方拿到独立所有权。这里每次都新建
// 切片与其中每个 entry，投递后不再触碰；reporter 直接把上一份交给心跳编码，所以
// 原地改写旧切片会与正在进行的编码竞争。
//
// 顺序按 (pipeline_id, role) 排定：map 迭代顺序是随机的，不排的话每条心跳的数组顺序
// 都在变——消费方 diff 会看到不存在的变化，测试也会间歇性红。
func (m *Module) pushPipelinesMirror() {
	if !m.mirrorReady {
		return
	}

	entries := make([]*iproto.PipelineHeartbeatEntry, 0, len(m.applied))
	for key, ap := range m.applied {
		entries = append(entries, &iproto.PipelineHeartbeatEntry{
			PipelineId:    key.pipelineID,
			ConfigVersion: ap.GetConfigVersion(),
			Role:          key.role,
		})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].GetPipelineId() != entries[j].GetPipelineId() {
			return entries[i].GetPipelineId() < entries[j].GetPipelineId()
		}
		return entries[i].GetRole() < entries[j].GetRole()
	})

	m.mirrorReporter(entries)
}

// markMirrorReady 在启动配置解析完成后开启镜像推送，并立刻推一次。
//
// 立刻推一次的作用是让第一个心跳就带上镜像：心跳周期 5s、镜像重推周期
// defaultMirrorPushInterval，不推这一次的话第一条心跳有可能落在两者之间。
//
// 注意此刻 m.applied 可能仍然是空的或不完整的——启动装配是异步的（restoreStartupPlans
// 提交的 AsyncGo 还在跑）。那不是问题：镜像的语义是"现在已经装配好的是哪些"，
// 装配过程中如实报少量条目是真话，而启动解析之前报 0 条是假话。两者的区别就是
// mirrorReady 这一位。
func (m *Module) markMirrorReady() {
	m.mirrorReady = true
	m.pushPipelinesMirror()
	changes := m.startupConfigChanges
	m.startupConfigChanges = make(map[string]*iproto.ConfigChanged)
	for _, changed := range changes {
		m.onConfigChanged(changed)
	}
}

// prodReportPipelinesMirror 是生产环境的镜像出口。抽成可替换字段的理由与
// prodReportPipelineStatus 完全一致（见 internal/node/controller/report.go 该函数文档：真实 m.Cast 经 xhive
// 包级单例解析目标，测试里不能用真实注册来拦截）。
func prodReportPipelinesMirror(m *Module) func(entries []*iproto.PipelineHeartbeatEntry) {
	return func(entries []*iproto.PipelineHeartbeatEntry) {
		configs := make([]iproto.PipelineRuntimeStatus, 0, len(entries))
		for _, entry := range entries {
			key := appliedKey{pipelineID: entry.GetPipelineId(), role: entry.GetRole()}
			current := m.runtimeInstances[key]
			if m.applied[key].running && current.InstanceID != "" && current.ConfigVersion == entry.GetConfigVersion() {
				configs = append(configs, current)
			}
		}
		m.Cast(reporter.ModuleName, reporter.PipelinesMirror{Entries: entries, ConfirmedConfigs: configs})
	}
}

// onMirrorTimer 是镜像重推定时器的处理函数。
func (m *Module) onMirrorTimer(_ int64, _ map[string]string) {
	m.pushPipelinesMirror()
	slog.Debug("controller: pipelines mirror pushed",
		slog.Int("entry_count", len(m.applied)), slog.Bool("ready", m.mirrorReady))
}
