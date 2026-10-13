package logship

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// statsCounters 是 logship 的原子计数器，可从任意 goroutine 安全读取（Stats() 的
// 实现依据）。
type statsCounters struct {
	published            atomic.Int64
	queueFullDropped     atomic.Int64
	publishFailedDropped atomic.Int64
}

func (m *Module) flush() {
	if len(m.pending) == 0 {
		return
	}
	batch := m.pending
	m.pending = nil
	m.linear.Go(func() { m.publishBatch(batch) }, nil)
}

// publishBatch 在 LinearContext 的后台 goroutine 里执行：按 BatchMaxRecords 分片，
// 每片打成一个 LogBatch 压缩发布（NODE-RPT-04"批量压缩发布"）。某一分片发布失败时
// 直接丢弃该分片并计数，继续尝试后面的分片——分片之间彼此独立，不像 reporter 的事件
// 队列那样需要保序（日志允许丢、允许乱序到达 Manager，NODE-RPT-07）。
func (m *Module) publishBatch(records []*iproto.LogRecord) {
	ctx := context.Background()
	for start := 0; start < len(records); start += m.opts.BatchMaxRecords {
		end := min(start+m.opts.BatchMaxRecords, len(records))
		chunk := records[start:end]

		batch := &iproto.LogBatch{NodeId: m.opts.NodeID, Records: chunk}
		if err := m.publishMessage(ctx, batch); err != nil {
			m.stats.publishFailedDropped.Add(int64(len(chunk)))
			slog.Warn("publish log batch failed, discarding",
				slog.Int("records", len(chunk)), slog.Any("error", err))
			continue
		}
		m.stats.published.Add(int64(len(chunk)))
	}
}

// publishMessage 组装信封（压缩编码 → 签名）并通过 Publisher 发布，投递结果判定与
// reporter 完全一致：只信 internal/transport.Publisher.PublishEnvelope 返回的 Outcome，
// 不重新实现 confirm/return 关联簿记（集成点 1）。
func (m *Module) publishMessage(ctx context.Context, batch *iproto.LogBatch) error {
	body, encoding, err := iproto.Encode(batch, iproto.EncodeOptions{Compress: true})
	if err != nil {
		return fmt.Errorf("logship: 编码日志批次失败: %w", err)
	}

	env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
		MessageID:       uuidx.New().String(),
		Type:            iproto.MessageTypeLogBatch,
		TimestampMS:     m.now().UnixMilli(),
		ProtocolVersion: m.opts.ProtocolVersion,
		Center:          m.opts.Center,
		NodeID:          m.opts.NodeID,
		ContentEncoding: encoding,
	})

	sig := iproto.Sign(m.signKey, env.SignParamsFor())
	env.WithSignature(sig)

	pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
	defer cancel()
	routingKey := transport.RoutingKeyLog(m.opts.Center, m.opts.NodeID)
	outcome, err := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeReport, routingKey, env)
	if err != nil {
		return fmt.Errorf("logship: 发布消息 %s 失败: %w", env.Properties.MessageID, err)
	}
	if outcome != transport.OutcomeConfirmed {
		return fmt.Errorf("logship: 消息 %s 未被判定为已投递，outcome=%s", env.Properties.MessageID, outcome)
	}
	return nil
}
