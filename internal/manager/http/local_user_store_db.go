package http

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// DBLocalUserStore persists local accounts through auth.ILocalUserStore.
type DBLocalUserStore struct {
	db *gorm.DB
}

// NewDBLocalUserStore 构造落库版本的 ILocalUserStore。db 必须是已经完成
// 迁移、users 表已存在的连接。
func NewDBLocalUserStore(db *gorm.DB) *DBLocalUserStore {
	return &DBLocalUserStore{db: db}
}

var _ auth.ILocalUserStore = (*DBLocalUserStore)(nil)

// GetByUsername 实现 auth.ILocalUserStore。
func (s *DBLocalUserStore) GetByUsername(ctx context.Context, username string) (auth.LocalCredential, error) {
	u, err := repo.GetUserByUsername(ctx, s.db, username)
	if err != nil {
		if errors.Is(err, repo.ErrUserNotFound) {
			return auth.LocalCredential{}, auth.ErrLocalUserNotFound
		}
		return auth.LocalCredential{}, err
	}
	if u.AuthSource != model.AuthSourceLocal {
		return auth.LocalCredential{}, auth.ErrLocalUserNotFound
	}
	return auth.LocalCredential{
		UserID:             u.ID.String(),
		Username:           u.Username,
		PasswordHash:       u.PasswordHash,
		Role:               auth.Role(u.Role),
		Disabled:           disabledFromStatus(u.Status), // 决策 3，见 user_store_db.go
		MustChangePassword: u.MustChangePassword,
	}, nil
}

// Any 实现 auth.ILocalUserStore：报告本地账号表是否非空，供 BootstrapAdmin
// 判断要不要创建初始管理员。
func (s *DBLocalUserStore) Any(ctx context.Context) (bool, error) {
	us, err := repo.ListUsers(ctx, s.db)
	if err != nil {
		return false, err
	}
	for _, u := range us {
		if u.AuthSource == model.AuthSourceLocal {
			return true, nil
		}
	}
	return false, nil
}

// Create 实现 auth.ILocalUserStore。
//
// 与 DBUserStore.Create 不同，这里的 AuthSource 取值没有歧义：
// ILocalUserStore 的文档注释本身就限定了它只服务本地账号（local_user_
// internal/node/store/store.go 的包注释），落 model.AuthSourceLocal 是这个接口方法唯一合理
// 的取值，不是占位。
func (s *DBLocalUserStore) Create(ctx context.Context, cred auth.LocalCredential) error {
	uid, err := uuidx.Parse(cred.UserID)
	if err != nil {
		return fmt.Errorf("http: 用户 ID 不是合法 UUID: %w", err)
	}
	m := &model.User{
		ID:                 uid,
		Username:           cred.Username,
		AuthSource:         model.AuthSourceLocal,
		PasswordHash:       cred.PasswordHash,
		Role:               model.UserRole(cred.Role),
		Status:             statusFromDisabled(cred.Disabled),
		MustChangePassword: cred.MustChangePassword,
	}
	return repo.CreateUser(ctx, s.db, m)
}
