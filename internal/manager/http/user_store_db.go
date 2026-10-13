package http

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

type DBUserStore struct {
	db *gorm.DB
}

// NewDBUserStore 构造落库版本的 IUserStore。db 必须是已经完成迁移、users
// 表已存在的连接。
func NewDBUserStore(db *gorm.DB) *DBUserStore {
	return &DBUserStore{db: db}
}

var _ auth.IUserStore = (*DBUserStore)(nil)

// ErrUserStoreCreateNotImplemented 表示 DBUserStore.Create 目前拒绝执行。
var ErrUserStoreCreateNotImplemented = errors.New("http: DBUserStore.Create 暂不可用：auth.User 未携带 AuthSource，无法确定这是本地/LDAP/OIDC 哪一种账号，需要先扩展调用契约")

// disabledFromStatus 把 model.UserStatus 的三态折叠成 auth.User.Disabled
// 这一个布尔字段。
func disabledFromStatus(status model.UserStatus) bool {
	return status != model.UserStatusActive
}

// statusFromDisabled 把 Disabled 转换为持久化的 active/disabled。
// 临时锁定由 login_lockouts 计算，不写入用户账号状态。
func statusFromDisabled(disabled bool) model.UserStatus {
	if disabled {
		return model.UserStatusDisabled
	}
	return model.UserStatusActive
}

// GetByID 实现 auth.IUserStore。
func (s *DBUserStore) GetByID(ctx context.Context, id string) (auth.User, error) {
	uid, err := uuidx.Parse(id)
	if err != nil {
		// 不是合法 UUID，结构上不可能存在于表里——MemoryUserStore 对任何
		// 查不到的字符串 key 都统一返回 ErrUserNotFound，这里保持同一种
		// 行为，不额外造一个"格式错误"错误类型（调用方只关心"存不存在"，
		// SessionAuth/resolveAuthenticatedUser 拿到非 nil error 就统一按
		// 未认证处理，不区分原因）。
		return auth.User{}, auth.ErrUserNotFound
	}
	u, err := repo.GetUser(ctx, s.db, uid)
	if err != nil {
		if errors.Is(err, repo.ErrUserNotFound) {
			return auth.User{}, auth.ErrUserNotFound
		}
		return auth.User{}, err
	}
	return auth.User{
		ID:                 u.ID.String(),
		Username:           u.Username,
		Role:               auth.Role(u.Role),
		MustChangePassword: u.MustChangePassword,
		Disabled:           disabledFromStatus(u.Status),
	}, nil
}

// Create 实现 auth.IUserStore。
func (s *DBUserStore) Create(_ context.Context, _ auth.User) error {
	return ErrUserStoreCreateNotImplemented
}
