// 登录失败计数与锁定仓储方法（需求 MGR-USR-01a/b）。
//
// 只做仓储层：查、原子计数 +1（含窗口过期重置）、清零、按保留期清理。
// 「locked_until > now 时拒绝登录，且必须返回与其它失败情形完全相同的
// 通用错误」（MGR-USR-01b）是 HTTP 登录 handler 的职责，本层只负责如实
// 存取这些字段，不做任何错误信息拼接或掩饰。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func GetLoginLockout(ctx context.Context, db *gorm.DB, scope model.LoginLockoutScope, subject string) (*model.LoginLockout, error) {
	var row model.LoginLockout
	err := db.WithContext(ctx).First(&row, "scope = ? AND subject = ?", scope, subject).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("repo: 查询登录锁定记录失败: %w", err)
	}
	return &row, nil
}

// RecordLoginFailure 记录一次登录失败：若计数窗口
// 已过期（now 距 window_started_at ≥ window）则重新开始计数（计数=1，
// 窗口起点=now，清除旧的 locked_until），否则计数 +1；计数达到 threshold
// 时置 locked_until = now + lockoutDuration。scope/subject 首次出现时
// 自动创建记录。
//
// 原子性（要求「并发登录失败不能丢计数」）分两个阶段，不能合并成一个
// 事务：
//
// 1. 单独一条自动提交的 INSERT IGNORE，保证行存在。
//  2. 单独一个事务：SELECT ... FOR UPDATE 拿行锁、读最新值、算出新值、
//     写回。
func RecordLoginFailure(ctx context.Context, db *gorm.DB, scope model.LoginLockoutScope, subject string, now time.Time, window, lockoutDuration time.Duration, threshold int) (*model.LoginLockout, error) {
	if !scope.Valid() {
		return nil, fmt.Errorf("model: 未知的登录锁定维度 %q", scope)
	}

	if err := db.WithContext(ctx).Exec(
		`INSERT IGNORE INTO login_lockouts
			(id, created_at, updated_at, scope, subject, failure_count, window_started_at, locked_until)
		 VALUES (?, ?, ?, ?, ?, 0, ?, NULL)`,
		uuidx.New(), now, now, scope, subject, now,
	).Error; err != nil {
		return nil, fmt.Errorf("repo: 初始化登录失败计数行失败: %w", err)
	}

	var result model.LoginLockout
	err := db.Transaction(func(tx *gorm.DB) error {
		var row model.LoginLockout
		if err := tx.WithContext(ctx).Clauses(clause.Locking{Strength: "UPDATE"}).
			First(&row, "scope = ? AND subject = ?", scope, subject).Error; err != nil {
			return fmt.Errorf("repo: 查询登录失败计数行失败: %w", err)
		}

		if now.Sub(row.WindowStartedAt) >= window {
			// 窗口已过期：重新开始，不在旧计数上累加。
			row.FailureCount = 1
			row.WindowStartedAt = now
			row.LockedUntil = nil
		} else {
			row.FailureCount++
		}
		if row.FailureCount >= threshold {
			until := now.Add(lockoutDuration)
			row.LockedUntil = &until
		}

		if err := tx.WithContext(ctx).Model(&model.LoginLockout{}).Where("id = ?", row.ID).
			Updates(map[string]any{
				"failure_count":     row.FailureCount,
				"window_started_at": row.WindowStartedAt,
				"locked_until":      row.LockedUntil,
			}).Error; err != nil {
			return fmt.Errorf("repo: 更新登录失败计数失败: %w", err)
		}
		result = row
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// ResetLoginLockout 清零一条计数记录（登录成功后清零两个维度
// 各自的记录）。不存在时视为幂等成功（从未失败过，没有需要清零的东西）。
func ResetLoginLockout(ctx context.Context, db *gorm.DB, scope model.LoginLockoutScope, subject string, now time.Time) error {
	res := db.WithContext(ctx).Model(&model.LoginLockout{}).
		Where("scope = ? AND subject = ?", scope, subject).
		Updates(map[string]any{
			"failure_count":     0,
			"window_started_at": now,
			"locked_until":      nil,
		})
	if res.Error != nil {
		return fmt.Errorf("repo: 清零登录失败计数失败: %w", res.Error)
	}
	return nil
}

// CleanupExpiredLoginLockouts 按保留期清理登录锁定记录：只
// 删除「当前不处于锁定状态」且「窗口起点早于保留期」的行——即使窗口已经
// 过期，仍在锁定中的记录也不会被清理，避免清理任务意外提前解除一个正在
// 生效的锁定。
func CleanupExpiredLoginLockouts(ctx context.Context, db *gorm.DB, now time.Time, retention time.Duration) (int64, error) {
	if retention <= 0 {
		return 0, fmt.Errorf("repo: 清理登录锁定记录失败: retention 必须为正数，实际 %s", retention)
	}
	cutoff := now.Add(-retention)
	return DeleteInBatches(ctx, db, "login_lockouts",
		"(locked_until IS NULL OR locked_until <= ?) AND window_started_at < ?",
		[]any{now, cutoff},
		DefaultBatchDeleteSize, DefaultBatchDeleteInterval,
	)
}
