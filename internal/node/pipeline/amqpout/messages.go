package amqpout

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// 与 binlogin.reportDDLEvent 同一先例：Send* 方法运行在调用方自己的 goroutine 里
// （不是本模块的事件循环），直接跨模块 Cast 到 reporter 是已确立的安全模式（reporter
// 的 ChanRPC 处理器本身就是异步、无锁的事件循环入口，接受任意 goroutine 的 Cast），
// 不需要绕经本模块的事件循环或 selfClient——本包甚至没有 selfClient/事件循环私有
// 状态需要保护。
//
// reason 是给运维排障看的简短说明（如"发布 3 次未确认"），不是稳定枚举值——
// BackpressureEventDetail.reason 是 proto 里的自由文本字段，允许携带上下文。
func (m *Module) reportBackpressure(reason string) {
	m.stats.backpressureEvents.Add(1)
	m.Cast(reporter.ModuleName, reporter.EventReport{
		Event: &iproto.Event{
			EventId:    uuidx.New().String(),
			Kind:       iproto.EventKind_EVENT_KIND_BACKPRESSURE,
			PipelineId: m.opts.PipelineID,
			NodeId:     m.opts.NodeID,
			TsMs:       m.now().UnixMilli(),
			Level:      "WARN",
			Message:    "amqp_out 发布退避重试",
			Detail: &iproto.Event_Backpressure{
				Backpressure: &iproto.BackpressureEventDetail{
					Module: ModuleName(m.opts.PipelineID),
					Reason: reason,
				},
			},
		},
	})
}
