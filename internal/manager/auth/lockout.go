package auth

import (
	"context"
	"sync"
	"time"
)

// LockoutScope 标识登录失败计数的维度。 要求按「用户名 + 客户端
// IP」两个维度分别计数：只按用户名计数挡不住同一个 IP 对大量不同用户名
// 各试几次的密码喷洒；只按 IP 计数则在多用户共享出口 IP（NAT、企业网关）
// 时会互相拖累。两个维度独立计数、独立判定锁定，互不影响。
type LockoutScope string

const (
	// LockoutScopeUsername 按登录请求里的用户名计数。
	LockoutScopeUsername LockoutScope = "username"
	// LockoutScopeClientIP 按客户端 IP（c.ClientIP()）计数。
	LockoutScopeClientIP LockoutScope = "client_ip"
)

// LockoutPolicy 是连续失败锁定的可配置参数（需求 MGR-USR-01a）。
type LockoutPolicy struct {
	// Threshold 是窗口内允许的最大失败次数；达到即锁定。
	Threshold int

	Window time.Duration
	// LockDuration 是锁定的持续时长。
	LockDuration time.Duration
}

// DefaultLockoutPolicy 返回一组有依据的默认值，供没有显式配置时使用
// （例如测试、或 config.LockoutConfig 字段为零值的防御性兜底）。
func DefaultLockoutPolicy() LockoutPolicy {
	return LockoutPolicy{
		Threshold:    5,
		Window:       15 * time.Minute,
		LockDuration: 15 * time.Minute,
	}
}

// ILockoutStore stores login failures and lock deadlines. Callers supply one time
// value per request so checks and updates share the same boundary.
type ILockoutStore interface {
	// Check 报告 (scope, subject) 在 now 时刻是否处于锁定状态。
	Check(ctx context.Context, scope LockoutScope, subject string, now time.Time) (locked bool, err error)
	// RecordFailure 记一次失败，按 policy 判断窗口内失败次数是否达到阈值、
	// 是否需要（继续）锁定，返回记录后的锁定状态。已经处于锁定状态时不
	// 会延长锁定期、也不会继续累加失败次数——否则攻击者靠不停重试就能把
	// 锁定时间无限续期，变成事实上的永久锁定，这本身也是一种拒绝服务。
	RecordFailure(ctx context.Context, scope LockoutScope, subject string, now time.Time, policy LockoutPolicy) (locked bool, err error)
	// Reset 清除 (scope, subject) 的失败计数与锁定状态（登录成功后调用）。
	Reset(ctx context.Context, scope LockoutScope, subject string) error
}

type lockoutRecord struct {
	failureCount    int
	windowStartedAt time.Time
	lockedUntil     time.Time // 零值 = 未锁定
}

// MemoryLockoutStore 是 ILockoutStore 的内存实现，供独立测试使用；
// 进程重启后计数归零。
type MemoryLockoutStore struct {
	mu      sync.Mutex
	records map[string]lockoutRecord
}

// NewMemoryLockoutStore 构造一个内存锁定状态存储。
func NewMemoryLockoutStore() *MemoryLockoutStore {
	return &MemoryLockoutStore{records: make(map[string]lockoutRecord)}
}

var _ ILockoutStore = (*MemoryLockoutStore)(nil)

func lockoutKey(scope LockoutScope, subject string) string {
	return string(scope) + "|" + subject
}

func (s *MemoryLockoutStore) Check(_ context.Context, scope LockoutScope, subject string, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.records[lockoutKey(scope, subject)]
	if !ok {
		return false, nil
	}
	return !rec.lockedUntil.IsZero() && rec.lockedUntil.After(now), nil
}

func (s *MemoryLockoutStore) RecordFailure(_ context.Context, scope LockoutScope, subject string, now time.Time, policy LockoutPolicy) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := lockoutKey(scope, subject)
	rec := s.records[key]

	if !rec.lockedUntil.IsZero() && rec.lockedUntil.After(now) {
		return true, nil
	}

	// Reset at the exact window boundary so an expired lock cannot immediately reuse
	// the old failure count.
	if rec.windowStartedAt.IsZero() || now.Sub(rec.windowStartedAt) >= policy.Window {
		rec.windowStartedAt = now
		rec.failureCount = 0
		rec.lockedUntil = time.Time{}
	}
	rec.failureCount++

	locked := rec.failureCount >= policy.Threshold
	if locked {
		rec.lockedUntil = now.Add(policy.LockDuration)
	}
	s.records[key] = rec
	return locked, nil
}

func (s *MemoryLockoutStore) Reset(_ context.Context, scope LockoutScope, subject string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, lockoutKey(scope, subject))
	return nil
}
