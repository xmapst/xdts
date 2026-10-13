package controller

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
)

// onConfigChanged 处理一条 ConfigChanged 通知（NODE-CFG-01"运行中收到 config.changed
// 后按 pipeline 增量拉取"）：发起一次定向 config.pull（只问这一个 pipeline_id），
// 用结果对本地 m.applied 做增量 diff（isFullPull=false，见 diffConfigs 文档——绝不能
// 在定向拉取之后把 m.applied 里其它未被问到的 pipeline_id 当作"已消失"处理，那会
// 误卸载其它正常运行的 pipeline）。
//
// changed.deleted 只是 Manager 的预告，本函数不据此直接判定卸载——真正的卸载决定
// 交给 diffConfigs 对定向拉取回来的 PipelineConfigItem.deleted 字段处理。两者理应
// 一致，但权威来源永远是 ConfigPullReply，不是 ConfigChanged 本身：后者只携带
// "发生了变化"与一个粗粒度版本号，不是完整配置，不足以单独作为卸载依据。
func (m *Module) onConfigChanged(changed *iproto.ConfigChanged) {
	if m.identityRetired {
		return
	}
	if !m.mirrorReady {
		m.startupConfigChanges[changed.GetPipelineId()] = changed
		return
	}
	id := changed.GetPipelineId()
	if id == "" {
		slog.Warn("received config.changed with empty pipeline_id, ignoring")
		return
	}

	var reply *iproto.ConfigPullReply
	var pullErr error
	m.AsyncGo("controller.pull."+id, func() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultTargetedPullTimeout)
		defer cancel()
		reply, pullErr = m.pullConfig(ctx, []string{id})
	}, func() {
		if m.identityRetired {
			return
		}
		if pullErr != nil {
			slog.Warn("targeted config.pull failed after config.changed",
				slog.String("pipeline_id", id), slog.Any("error", pullErr))
			return
		}
		m.applyPlans(diffConfigs(m.applied, reply.GetConfigs(), false))
	})
}

// onConfigChangedMessage 是 onConfigChanged 的信封解码入口：解析 + 解码一条
// transport.Message 为 *iproto.ConfigChanged 后转交 onConfigChanged。
//
// 调用方是 internal/node/controller/handler.go 的 dispatchEnvelope——本包现在独占消费控制队列
// （transport.NodeControlQueue，见 internal/node/controller/controller.go OnInit、doc.go"与 probe 的边界"
// 一节），按 Properties.Type 分派到这里。
//
// 不校验签名，理由与 pullConfig 里"入站回复签名校验：不做"一节完全相同（ed25519
// 材料未分发，P1/NODE-CFG-05 范围），此处不重复展开。
func (m *Module) onConfigChangedMessage(msg *transport.Message) error {
	var changed iproto.ConfigChanged
	if _, err := transport.ParseAndDecode(msg, &changed, nil); err != nil {
		return fmt.Errorf("controller: 解析 config.changed 失败: %w", err)
	}
	// Delivery runs outside the controller loop. Check retirement and schedule
	// the pull in that loop, where the identity state has its sole owner.
	m.AsyncGo("controller.configchanged."+changed.GetPipelineId(), func() {}, func() {
		m.onConfigChanged(&changed)
	})
	return nil
}
