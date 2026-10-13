package amqpin

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
)

// errApplyNotRetryable 标记"这次应用失败是永久性的，重投/重试都不会变"。
//
// 为什么要一个哨兵而不是在 applyAndFinish 里再调一次 mysqlout.IsRetryable：
// **分类只在做出决定的那一处做一次**。applyWithRetry 已经是唯一根据
// IsRetryable 决定"放弃还是退避"的地方，把结论以哨兵形式带出去，调用方就不需要
// 复述一遍同一条判断——否则两处各有一份"要不要重试"的读法，其中一处被改时另一处
// 不会报错（红线「契约改全」的同一形状）。
//
// 它只区分"永久失败"与"重试预算耗尽"这两种终局，不携带原因：原因由被包裹的
// 原始错误自己说，本哨兵只回答"要不要让 pipeline 可见地停住"。
var errApplyNotRetryable = errors.New("amqpin: 应用失败且不可重试")

// applyWithRetry 调用 m.opts.Applier.ApplyBatch，失败且 mysqlout.IsRetryable
// 判定可恢复时按退避重试，最多 m.opts.MaxApplyAttempts 次；不可重试的错误第一次
// 就放弃，不浪费重试预算（NODE-MQI-04"重试超限则停止 pipeline"要求这个上限必须
// 存在，与 amqpout.publishWithRetry 完全依赖调用方 ctx 无上限不同——那里的调用方
// 是 binlogin 的会话 goroutine、有天然的 SendTimeout 包住；这里的调用方是
// transport.Consumer 的 Handler，没有任何外部传入的 ctx/deadline，必须由本包自己
// 提供一个有限的退避预算，否则会无限重试下去）。
//
// 每次尝试各自建一个 ApplyAttemptTimeout 超时的 ctx，避免单次写入卡死导致整个
// 重试循环失去响应；退避等待本身可被 m.shutdownCtx 取消（OnDestroy 时），让
// 关闭流程不必等一整轮退避才能收尾。
func (m *Module) applyWithRetry(batch *iproto.DataBatch) (iproto.BatchApplyResult, error) {
	policy := m.runtimeErrorPolicy
	var all iproto.BatchApplyResult
	remaining := batch
	for {
		result, err := m.applyWithRetryBudget(remaining, policy)
		all.AppliedGTIDs = append(all.AppliedGTIDs, result.AppliedGTIDs...)
		all.RejectedGTIDs = append(all.RejectedGTIDs, result.RejectedGTIDs...)
		all.DeferredGTIDs = append(all.DeferredGTIDs, result.DeferredGTIDs...)
		if err == nil {
			return all, nil
		}
		var failed *mysqlout.BatchTxnError
		if !policy.SkipUnretryable || !errors.As(err, &failed) || failed.Index < 0 || failed.Index >= len(remaining.GetTxns()) || remaining.Txns[failed.Index].GetDdl() != nil || !mysqlout.CanSkipFailedTransaction(err) {
			return all, err
		}
		gtid := remaining.Txns[failed.Index].GetGtid()
		if skipErr := m.persistSkippedTransaction(gtid); skipErr != nil {
			return all, skipErr
		}
		all.RejectedGTIDs = append(all.RejectedGTIDs, gtid)
		remaining = &iproto.DataBatch{PipelineId: batch.GetPipelineId(), RunId: batch.GetRunId(), Epoch: batch.GetEpoch(), BatchSeq: batch.GetBatchSeq(), Txns: remaining.Txns[failed.Index+1:]}
		if len(remaining.Txns) == 0 {
			return all, nil
		}
	}
}

func (m *Module) applyWithRetryBudget(batch *iproto.DataBatch, policy iproto.TargetErrorPolicy) (iproto.BatchApplyResult, error) {
	var result iproto.BatchApplyResult
	committed := make(map[string]struct{})
	deferred := make(map[string]struct{})
	var lastErr error
	attempts, initial, maxDelay := uint64(m.opts.MaxApplyAttempts), m.opts.ApplyInitialBackoff, m.opts.ApplyMaxBackoff
	if policy.Configured {
		attempts = uint64(policy.Retries) + 1
		initial, maxDelay = policy.InitialBackoff, policy.MaxBackoff
	}
	for attempt := uint64(1); attempt <= attempts; attempt++ {
		base := m.shutdownCtx
		if base == nil {
			base = context.Background()
		}
		ctx, cancel := context.WithTimeout(base, m.opts.ApplyAttemptTimeout)
		attemptResult, err := m.applyAttempt(ctx, batch)
		cancel()
		// A later attempt can fail before reaching an earlier committed GTID.
		// Preserve those irreversible commits across the entire retry budget.
		for _, gtid := range attemptResult.AppliedGTIDs {
			if _, exists := committed[gtid]; !exists {
				committed[gtid] = struct{}{}
				result.AppliedGTIDs = append(result.AppliedGTIDs, gtid)
			}
		}
		result.RejectedGTIDs = nil
		for _, gtid := range attemptResult.DeferredGTIDs {
			if _, exists := deferred[gtid]; !exists {
				deferred[gtid] = struct{}{}
				result.DeferredGTIDs = append(result.DeferredGTIDs, gtid)
			}
		}
		for _, gtid := range attemptResult.RejectedGTIDs {
			if _, exists := committed[gtid]; !exists {
				result.RejectedGTIDs = append(result.RejectedGTIDs, gtid)
			}
		}
		if err == nil {
			return result, nil
		}
		lastErr = err

		if !mysqlout.IsRetryable(err) {
			// 两个 %w：errApplyNotRetryable 供 applyAndFinish 用 errors.Is 判终局，
			// err 保留原始错误链（调用方仍可 errors.Is 到底层的具体错误）。
			return result, fmt.Errorf("amqpin: ApplyBatch 失败且不可重试（第 %d 次尝试）: %w: %w",
				attempt, errApplyNotRetryable, err)
		}
		if attempt == attempts {
			break
		}

		delay := backoffDelay(int(attempt-1), initial, maxDelay, m.opts.ApplyMultiplier)
		if sleepErr := m.sleep(base, delay); sleepErr != nil {
			return result, fmt.Errorf("amqpin: ApplyBatch 重试过程中被中断（已尝试 %d 次）: %w", attempt, sleepErr)
		}
	}
	return result, fmt.Errorf("amqpin: ApplyBatch 重试 %d 次后仍失败: %w", attempts, lastErr)
}

func (m *Module) applyAttempt(ctx context.Context, batch *iproto.DataBatch) (result iproto.BatchApplyResult, applyErr error) {
	defer func() {
		if recover() != nil {
			applyErr = errors.New("amqpin: target writer panicked")
		}
	}()
	if applier, ok := m.opts.Applier.(iResultApplier); ok {
		return applier.ApplyBatchResult(ctx, batch)
	}
	applyErr = m.opts.Applier.ApplyBatch(ctx, batch)
	if applyErr == nil {
		for _, txn := range batch.GetTxns() {
			result.AppliedGTIDs = append(result.AppliedGTIDs, txn.GetGtid())
		}
	}
	return result, applyErr
}

// backoffDelay 计算第 attempt 次重试（0-based）应等待的时长：
// initial * multiplier^attempt，上限 max。与 amqpout/binlogin 同名函数是同一个
// 纯函数模式，各包独立定义，不为了复用几行算术在本来就独立的包之间制造依赖。
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
