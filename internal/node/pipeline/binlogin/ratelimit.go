package binlogin

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
)

// Incremental rate limits account for rows and bytes before publication.
// Zero disables a limit; waits obey the source session's cancellation context.

// tokenBucket 是单路令牌桶的纯计算部分：不持有时钟、不睡觉，只回答"再等多久才够"。
// 时钟与睡眠由 rateLimiter 注入/执行，这样这里的算术可以完全脱离时间被单测。
type tokenBucket struct {
	// perSec 是配置的速率（行/秒 或 字节/秒）。<=0 表示不限速。
	perSec float64

	burst float64

	tokens float64
	// last 是上一次结算时刻。
	last time.Time
}

func newTokenBucket(perSec uint64, now time.Time) *tokenBucket {
	r := float64(perSec)
	return &tokenBucket{perSec: r, burst: r, tokens: r, last: now}
}

// limited 回答这一路是否配置了限速。0 表示不限速——这是协议语义（RateLimit 消息
// 缺省时两个字段都是 0，绝大多数 pipeline 都不配限流），一旦被读成"0 行/秒"，
// 所有既有 pipeline 会立刻全部停止投递且不报任何错。
func (b *tokenBucket) limited() bool { return b != nil && b.perSec > 0 }

// reserve 结算到 now，扣掉 n 个令牌，返回"必须等多久才算把欠款补齐"。
// 返回 0 表示无需等待。调用方负责真正睡眠。
func (b *tokenBucket) reserve(now time.Time, n uint64) time.Duration {
	if !b.limited() {
		return 0
	}
	// 先按经过的时间补充令牌，再扣款。elapsed<=0 时不补充：时钟回拨或同一时刻内的
	// 连续调用都不应该产生额外配额（也不应该倒扣——那会让一次时钟回拨变成一次长
	// 睡眠）。
	if elapsed := now.Sub(b.last); elapsed > 0 {
		b.last = now
		b.tokens += elapsed.Seconds() * b.perSec
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
	}
	b.tokens -= float64(n)
	if b.tokens >= 0 {
		return 0
	}
	// 欠款除以速率就是需要的等待时长。睡完之后本函数不主动把 tokens 归零：下一次
	// reserve 的补充项恰好等于这次睡掉的时长 × 速率，正好把欠款抹平。两处用同一个
	// 时钟，所以长期平均速率严格等于 perSec，不需要额外的对账。
	return time.Duration(-b.tokens / b.perSec * float64(time.Second))
}

// rateLimiter 把行、字节两路桶与时钟/睡眠绑在一起，对外只暴露 waitForBatch。
type rateLimiter struct {
	rows  *tokenBucket
	bytes *tokenBucket

	now   func() time.Time
	sleep func(context.Context, time.Duration) error

	// onWaited 在每次真的睡过之后被调用（传入实际等待时长），供 Module 累计
	// Stats.RateLimitWait。做成回调而不是让本类型直接引用 statsCounters：
	// 限流器不需要知道统计口径，Module 也不必把计数器暴露给它。
	onWaited func(time.Duration)
}

// newRateLimiter 恒返回非 nil：两路都是 0（未配置限流）时 waitForBatch 是一次
// 廉价的空操作。不让它返回 nil 是为了让调用点只有一种形状（永远直接调
// m.limiter.waitForBatch），否则"限流器为 nil 时跳过"这个分支会成为一个可以被
// 误改成"永远跳过"的地方，而那种错法在测试里看起来一切正常。
func newRateLimiter(rowsPerSec, bytesPerSec uint64, now func() time.Time,
	sleep func(context.Context, time.Duration) error, onWaited func(time.Duration)) *rateLimiter {
	start := now()
	return &rateLimiter{
		rows:     newTokenBucket(rowsPerSec, start),
		bytes:    newTokenBucket(bytesPerSec, start),
		now:      now,
		sleep:    sleep,
		onWaited: onWaited,
	}
}

// enabled 回答是否有任意一路限速生效（只用于 OnInit 决定打不打那条说明日志）。
func (l *rateLimiter) enabled() bool { return l.rows.limited() || l.bytes.limited() }

// Preserve accrued credit/debt when changing a live limit. Reapplying an
// unchanged limit must never refill the bucket or forgive outstanding debt.
func (l *rateLimiter) reconfigure(rows, bytes uint64) {
	for _, change := range []struct {
		bucket *tokenBucket
		rate   uint64
	}{{l.rows, rows}, {l.bytes, bytes}} {
		b := change.bucket
		if b.perSec == float64(change.rate) {
			continue
		}
		now := l.now()
		_ = b.reserve(now, 0)
		wasLimited := b.limited()
		b.perSec, b.burst, b.last = float64(change.rate), float64(change.rate), now
		if !wasLimited || change.rate == 0 {
			b.tokens = b.burst
		} else {
			b.tokens = min(b.tokens, b.burst)
		}
	}
}

// waitForBatch 为一个即将投递的 DataBatch 结算两路配额并按需等待。
//
// 行数取批内所有 Txn 的行数之和：DDL 事务没有行，因此不消耗行配额（rows_per_sec
// 说的就是行），但它的字节数照样计入字节配额。
//
// proto.Size 只在真的配置了字节限速时才算——它是一次完整的 wire 尺寸计算，对一个
// 几 MB 的批次不是零成本，而绝大多数 pipeline 两路都不配。
func (l *rateLimiter) waitForBatch(ctx context.Context, batch *iproto.DataBatch) error {
	if !l.enabled() {
		return nil
	}
	now := l.now()
	var wait time.Duration
	if l.rows.limited() {
		wait = max(wait, l.rows.reserve(now, batchRowCount(batch)))
	}
	if l.bytes.limited() {
		wait = max(wait, l.bytes.reserve(now, uint64(proto.Size(batch))))
	}
	if wait <= 0 {
		return nil
	}
	if err := l.sleep(ctx, wait); err != nil {
		return err
	}
	if l.onWaited != nil {
		l.onWaited(wait)
	}
	return nil
}

// batchRowCount 统计一个 DataBatch 携带的行变更总数。
func batchRowCount(batch *iproto.DataBatch) uint64 {
	var n uint64
	for _, txn := range batch.GetTxns() {
		n += uint64(len(txn.GetRows()))
	}
	return n
}

// sleepCtx 是生产用的睡眠实现：必须可被 ctx 取消。低速率 + 大批次下单次等待可以长达
// 数分钟，如果睡眠不响应 ctx，OnDestroy 取消 sessionCtx 之后会话 goroutine 仍会一直
// 睡下去，模块关闭要么超时要么直接卡住。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
