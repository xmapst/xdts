package reporter

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

type pendingEvent struct {
	event    *iproto.Event
	enqueued time.Time
}

// statsCounters 是 reporter 的原子计数器，可从任意 goroutine 安全读取
// （Stats() 的实现依据）。
type statsCounters struct {
	published          atomic.Int64
	failed             atomic.Int64
	eventsDroppedByCap atomic.Int64
	eventsDroppedByTTL atomic.Int64
}

// enqueueEvent 把一个校验通过的事件追加到 pendingEvents 末尾，并立即执行一次容量/TTL
// 治理（NODE-XDOC-07："outbox 设条数上限"）。只在事件循环里调用，不需要加锁。
func (m *Module) enqueueEvent(ev *iproto.Event) {
	m.pendingEvents = append(m.pendingEvents, &pendingEvent{event: ev, enqueued: m.now()})
	m.governEvents()
}

// governEvents 先按 TTL 清理最旧的过期条目，再按 EventMaxCount 从最旧的开始丢弃
// 超出部分。两者都不是设计默认路径（正常情况下 outbox 应该很快被发布清空），只是
// Broker 长时间不可达时的容量兜底，因此丢弃时记录计数与 WARN 日志，供告警排查
// （NODE-RPT-07"受 TTL 与条数上限约束"）。
func (m *Module) governEvents() {
	now := m.now()
	kept := m.pendingEvents[:0:0]
	for _, pe := range m.pendingEvents {
		if now.Sub(pe.enqueued) > m.opts.EventTTL {
			m.stats.eventsDroppedByTTL.Add(1)
			slog.Warn("event outbox entry exceeded ttl, discarding",
				slog.String("event_id", pe.event.EventId),
				slog.Duration("ttl", m.opts.EventTTL),
				slog.Duration("age", now.Sub(pe.enqueued)))
			continue
		}
		kept = append(kept, pe)
	}
	m.pendingEvents = kept

	if over := len(m.pendingEvents) - m.opts.EventMaxCount; over > 0 {
		for _, pe := range m.pendingEvents[:over] {
			m.stats.eventsDroppedByCap.Add(1)
			slog.Warn("event outbox exceeded max count, discarding oldest entry",
				slog.String("event_id", pe.event.EventId),
				slog.Int("max_count", m.opts.EventMaxCount))
		}
		m.pendingEvents = m.pendingEvents[over:]
	}
}
