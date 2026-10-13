package http

import (
	"context"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
)

type DBLockoutStore struct {
	db *gorm.DB
}

// NewDBLockoutStore 构造落库版本的 ILockoutStore。db 必须是已经完成迁移、
// login_lockouts 表已存在的连接。
func NewDBLockoutStore(db *gorm.DB) *DBLockoutStore {
	return &DBLockoutStore{db: db}
}

var _ auth.ILockoutStore = (*DBLockoutStore)(nil)

// toModelLockoutScope 把 auth.LockoutScope 转成 model.LoginLockoutScope。
// 两边底层字符串取值（"username"/"client_ip"）逐一对应，是同一个概念在
// 两个不共享依赖的包里的平行定义，转换只是类型层面的搬运，不改变语义。
func toModelLockoutScope(scope auth.LockoutScope) model.LoginLockoutScope {
	return model.LoginLockoutScope(scope)
}

// Check 报告 (scope, subject) 在 now 时刻是否处于锁定状态。
func (s *DBLockoutStore) Check(ctx context.Context, scope auth.LockoutScope, subject string, now time.Time) (bool, error) {
	row, err := repo.GetLoginLockout(ctx, s.db, toModelLockoutScope(scope), subject)
	if err != nil {
		return false, err
	}
	if row == nil {
		// 从未失败过：等价于未锁定，不是错误（repo.GetLoginLockout 的
		// 注释里也是这个约定）。
		return false, nil
	}
	return row.Locked(now), nil
}

// RecordFailure 记一次失败，policy 拆成仓储函数需要的三个参数
// （window/lockoutDuration/threshold）。
func (s *DBLockoutStore) RecordFailure(ctx context.Context, scope auth.LockoutScope, subject string, now time.Time, policy auth.LockoutPolicy) (bool, error) {
	row, err := repo.RecordLoginFailure(ctx, s.db, toModelLockoutScope(scope), subject, now, policy.Window, policy.LockDuration, policy.Threshold)
	if err != nil {
		return false, err
	}
	return row.Locked(now), nil
}

func (s *DBLockoutStore) Reset(ctx context.Context, scope auth.LockoutScope, subject string) error {
	return repo.ResetLoginLockout(ctx, s.db, toModelLockoutScope(scope), subject, time.Now())
}
