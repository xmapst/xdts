package amqpout

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
	"github.com/xmapst/xdts/internal/transport"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// SendBatch 投递一个 DataBatch（NODE-MQO-01~03）。方法签名与 binlogin.ISink.SendBatch
// 逐字一致，见包文档"与 binlog_in 的接线关系"。
//
// 调用方（binlogin 会话 goroutine）在收到下一个 DataBatch 完整内容前不会发起下一次
// SendBatch 调用——"单批次在途"（NODE-MQO-03）由这条调用纪律自然保证，本方法不做
// 任何内部串行化。
//
// ctx 预算是重试能否收敛的唯一边界：本方法内部的退避重试没有独立的最大次数/总
// 时长上限，未投递成功时会一直按指数退避重试直到 ctx 取消/超时（见包文档"退避
// 重试的边界完全交给调用方的 ctx"）。调用方必须自己施加一个有 deadline 的 ctx
// （当前生产路径下由 binlogin 的 SendTimeout 承担这个职责），否则持续未投递会
// 无限重试——ctx 没有 deadline 时本方法只会打一条 Warn 日志提示这个依赖，不会
// 拒绝执行。
func (m *Module) SendBatch(ctx context.Context, batch *iproto.DataBatch) error {
	if batch == nil {
		return errors.New("amqpout: batch 不能为 nil")
	}
	headers := map[string]any{
		iproto.HeaderPipelineID: batch.PipelineId,

		iproto.HeaderRunID:    iproto.HeaderUint64String(batch.RunId),
		iproto.HeaderEpoch:    iproto.HeaderUint64String(batch.Epoch),
		iproto.HeaderBatchSeq: iproto.HeaderUint64String(batch.BatchSeq),
	}
	err := m.publishWithRetry(ctx, iproto.MessageTypeDataBatch, transport.RoutingKeyPipelineData(m.opts.PipelineID), batch, headers)
	if err != nil {
		return fmt.Errorf("amqpout: 投递 DataBatch 失败: %w", err)
	}
	m.stats.batchesSent.Add(1)
	return nil
}

// SendWatermark 投递一个空闲水位消息（NODE-BIN-07 的下游落点）。方法签名与
// binlogin.ISink.SendWatermark 逐字一致。水位与 DataBatch 共用同一路由
// （pl.<pipeline_id>），不单独限流或走不同重试策略。
//
// ctx 预算依赖与 SendBatch 完全一致：本方法内部退避重试没有独立上限，完全依赖
// 调用方 ctx 的 deadline 才会收敛，见 SendBatch 文档的完整说明。
func (m *Module) SendWatermark(ctx context.Context, wm *iproto.Watermark) error {
	if wm == nil {
		return errors.New("amqpout: watermark 不能为 nil")
	}
	headers := map[string]any{
		iproto.HeaderPipelineID: wm.PipelineId,
		iproto.HeaderRunID:      iproto.HeaderUint64String(wm.RunId),
	}
	err := m.publishWithRetry(ctx, iproto.MessageTypeWatermark, transport.RoutingKeyPipelineData(m.opts.PipelineID), wm, headers)
	if err != nil {
		return fmt.Errorf("amqpout: 投递 Watermark 失败: %w", err)
	}
	m.stats.watermarksSent.Add(1)
	return nil
}

// SendSnapshotChunk 投递一个全量分块（NODE-MQO-04"全量分块之间无顺序要求，可多
// 分块并行在途"）：本方法不做任何内部串行化，调用方可以从多个 goroutine 并发调用它，
// 各自独立重试、独立结算，互不阻塞。
//
// ctx 预算依赖与 SendBatch 完全一致：本方法内部退避重试没有独立上限，完全依赖
// 调用方 ctx 的 deadline 才会收敛，见 SendBatch 文档的完整说明。
func (m *Module) SendSnapshotChunk(ctx context.Context, chunk *iproto.SnapshotChunk) error {
	if chunk == nil {
		return errors.New("amqpout: chunk 不能为 nil")
	}
	headers := map[string]any{
		iproto.HeaderPipelineID: chunk.PipelineId,
		// 见 SendBatch 同一处注释：uint64 必须用 HeaderUint64String 编码。
		iproto.HeaderRunID: iproto.HeaderUint64String(chunk.RunId),
		iproto.HeaderChunk: iproto.HeaderUint64String(chunk.ChunkNo),
	}
	err := m.publishWithRetry(ctx, iproto.MessageTypeSnapshotChunk, transport.RoutingKeyPipelineSnapshot(m.opts.PipelineID), chunk, headers)
	if err != nil {
		return fmt.Errorf("amqpout: 投递 SnapshotChunk 失败: %w", err)
	}
	m.stats.snapshotChunksSent.Add(1)
	return nil
}

// SendSnapshotEnd 投递全量分块规划的终点标记，路由与 SnapshotChunk 相同（同一个
// pl.<pipeline_id>.snap 队列，NODE-SNAP-01/05 的消费方按到达顺序以外的手段——分块
// 计数——判定全量是否完整，不依赖 SnapshotEnd 一定最后到达，因此这里同样不做
// 串行化等待）。
//
// ctx 预算依赖与 SendBatch 完全一致：本方法内部退避重试没有独立上限，完全依赖
// 调用方 ctx 的 deadline 才会收敛，见 SendBatch 文档的完整说明。
func (m *Module) SendSnapshotEnd(ctx context.Context, end *iproto.SnapshotEnd) error {
	if end == nil {
		return errors.New("amqpout: end 不能为 nil")
	}
	headers := map[string]any{
		iproto.HeaderPipelineID: end.PipelineId,
		// 见 SendBatch 同一处注释：uint64 必须用 HeaderUint64String 编码。
		iproto.HeaderRunID: iproto.HeaderUint64String(end.RunId),
	}
	err := m.publishWithRetry(ctx, iproto.MessageTypeSnapshotEnd, transport.RoutingKeyPipelineSnapshot(m.opts.PipelineID), end, headers)
	if err != nil {
		return fmt.Errorf("amqpout: 投递 SnapshotEnd 失败: %w", err)
	}
	m.stats.snapshotEndsSent.Add(1)
	return nil
}

func (m *Module) SendSnapshotRecoveryBegin(ctx context.Context, begin *iproto.SnapshotRecoveryBegin) error {
	if err := iproto.ValidateSnapshotRecoveryBegin(begin); err != nil {
		return err
	}
	return m.publishWithRetry(ctx, iproto.MessageTypeSnapshotRecoveryBegin, transport.RoutingKeyPipelineSnapshot(m.opts.PipelineID), begin, map[string]any{
		iproto.HeaderPipelineID: begin.PipelineId, iproto.HeaderRunID: iproto.HeaderUint64String(begin.RunId),
	})
}

// publishWithRetry encodes and bounds messages before confirmed mandatory publication.
// The caller supplies cancellation and deadline; retries cannot outlive that budget.
func (m *Module) publishWithRetry(ctx context.Context, msgType, routingKey string, msg proto.Message, extraHeaders map[string]any) error {
	body, encoding, err := iproto.Encode(msg, iproto.EncodeOptions{Compress: true})
	if err != nil {
		return fmt.Errorf("amqpout: 编码消息失败: %w", err)
	}
	if len(body) > m.opts.MaxMessageBytes {
		m.stats.oversizeRejected.Add(1)
		m.stats.publishErrors.Add(1)
		return fmt.Errorf("%w: 编码后 %d 字节，上限 %d 字节", errMessageTooLarge, len(body), m.opts.MaxMessageBytes)
	}
	// One immutable encoded message is outstanding throughout confirm/retry.
	m.stats.inflightBatches.Add(1)
	m.stats.inflightBytes.Add(uint64(len(body)))
	defer m.stats.inflightBatches.Add(^uint64(0))
	defer m.stats.inflightBytes.Add(^uint64(len(body) - 1))

	// 退避重试完全依赖调用方 ctx 的 deadline 才会收敛（见 SendBatch 文档"ctx 预算
	// 是重试能否收敛的唯一边界"）。ctx 没有 deadline 是一种容易被忽视的误用——
	// 调用方可能没意识到自己传了个 context.Background()，本方法本身对此不做任何
	// 硬性限制（那会破坏"边界完全交给调用方"的设计），只把这个依赖从静默变成可见：
	// 打一条 Warn，不拒绝执行（与 ColumnFilter/OriginResolver 未接线时的 Warn 守卫
	// 同一思路，见 internal/node/pipeline/binlogin/binlogin.go 对应注释）。
	if _, ok := ctx.Deadline(); !ok {
		slog.Warn("publish ctx has no deadline",
			slog.String("pipeline_id", m.opts.PipelineID),
			slog.String("type", msgType))
	}

	attempt := 0
	for {
		attempt++
		env := iproto.NewEnvelope(body, iproto.NewEnvelopeParams{
			MessageID:       uuidx.New().String(),
			Type:            msgType,
			TimestampMS:     m.now().UnixMilli(),
			ProtocolVersion: m.opts.ProtocolVersion,
			Center:          m.opts.Center,
			NodeID:          m.opts.NodeID,
			ContentEncoding: encoding,
			HeaderExtra:     extraHeaders,
		})
		sig := iproto.Sign(m.signKey, env.SignParamsFor())
		env.WithSignature(sig)

		pubCtx, cancel := context.WithTimeout(ctx, m.opts.PublishTimeout)
		outcome, pubErr := m.opts.Publisher.PublishEnvelope(pubCtx, transport.ExchangeData, routingKey, env)
		cancel()

		if pubErr == nil && outcome == transport.OutcomeConfirmed {
			m.stats.confirmedBytes.Add(uint64(len(body)))
			if attempt > 1 {
				m.stats.retriedCalls.Add(1)
			}
			return nil
		}
		m.stats.publishErrors.Add(1)

		// 未投递成功：记录原因，第一次失败时立即上报一次背压事件（判定口径见包
		// 文档），然后退避重试。ctx（调用方原始的，不是刚才用于单次尝试的 pubCtx）
		// 一旦结束，停止重试并把原因返回给调用方。
		if pubErr != nil {
			slog.Warn("publish attempt failed, retrying with backoff",
				slog.String("pipeline_id", m.opts.PipelineID),
				slog.String("type", msgType),
				slog.Int("attempt", attempt),
				slog.Any("error", pubErr))
		} else {
			slog.Warn("message not confirmed as delivered, retrying with backoff",
				slog.String("pipeline_id", m.opts.PipelineID),
				slog.String("type", msgType),
				slog.Int("attempt", attempt),
				slog.String("outcome", outcome.String()))
		}
		if attempt == 1 {
			var reason string
			if pubErr != nil {
				reason = safeerror.Public(pubErr)
			} else {
				reason = "outcome=" + outcome.String()
			}
			m.reportBackpressure(fmt.Sprintf("%s 发布未确认（%s），开始退避重发", msgType, reason))
		}

		if ctx.Err() != nil {
			m.stats.failedCalls.Add(1)
			return fmt.Errorf("%w（已重试 %d 次）", ctx.Err(), attempt)
		}
		delay := backoffDelay(attempt-1, m.opts.PublishInitialBackoff, m.opts.PublishMaxBackoff, m.opts.PublishMultiplier)
		if sleepErr := m.sleep(ctx, delay); sleepErr != nil {
			m.stats.failedCalls.Add(1)
			return fmt.Errorf("%w（已重试 %d 次）", sleepErr, attempt)
		}
	}
}

// errMessageTooLarge 是编码后超过 MaxMessageBytes 时返回的哨兵错误，供调用方按
// errors.Is 判断"这是一个不会通过重试恢复的永久性失败"（与未投递/网络类失败区分）。
var errMessageTooLarge = errors.New("amqpout: 消息体积超过上限")

// backoffDelay 计算第 attempt 次重试（0-based）应等待的时长：initial * multiplier^attempt，
// 上限 max。与 binlogin 包内同名函数是同一个纯函数模式（不同包各自定义，避免为了
// 复用几行算术在两个本来就独立的包之间制造依赖）。
func backoffDelay(attempt int, initial, max time.Duration, multiplier float64) time.Duration {
	d := float64(initial)
	for range attempt {
		d *= multiplier
		if d >= float64(max) {
			return max
		}
	}
	return time.Duration(d)
}
