package model

import (
	"fmt"
	"time"

	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

// AuthSource 是用户的认证来源（需求 MGR-USR-02/03）。
type AuthSource string

const (
	AuthSourceLocal AuthSource = "local" // 本地账号密码
	AuthSourceLDAP  AuthSource = "ldap"  // LDAP / LDAPS 绑定验证
	AuthSourceOIDC  AuthSource = "oidc"  // OIDC 授权码 + PKCE
)

// Valid 校验取值是否为已定义的认证来源，未知取值必须显式拒绝。
func (s AuthSource) Valid() bool {
	switch s {
	case AuthSourceLocal, AuthSourceLDAP, AuthSourceOIDC:
		return true
	default:
		return false
	}
}

// UserRole 是后台用户角色。首版仅提供管理员与只读两种（需求 MGR-USR-05）。
type UserRole string

const (
	UserRoleAdmin    UserRole = "admin"    // 管理员：全部读写
	UserRoleReadOnly UserRole = "readonly" // 只读：写接口一律 403（MGR-USR-08）
)

// Valid 校验角色取值。
func (r UserRole) Valid() bool {
	switch r {
	case UserRoleAdmin, UserRoleReadOnly:
		return true
	default:
		return false
	}
}

// UserStatus 是账号状态。
type UserStatus string

const (
	UserStatusActive   UserStatus = "active"   // 正常
	UserStatusDisabled UserStatus = "disabled" // 管理员禁用
	UserStatusLocked   UserStatus = "locked"   // 连续登录失败触发锁定（MGR-USR-01）
)

// Valid 校验账号状态取值。
func (s UserStatus) Valid() bool {
	switch s {
	case UserStatusActive, UserStatusDisabled, UserStatusLocked:
		return true
	default:
		return false
	}
}

// User 是后台登录用户：本地账号、LDAP 或 OIDC 首次登录自动建档
// （需求 MGR-USR-01/02/03/05）。
type User struct {
	Base

	Username string `gorm:"column:username;type:varchar(128);not null;uniqueIndex:uk_users_username" json:"username"`

	AuthSource AuthSource `gorm:"column:auth_source;type:varchar(16);not null" json:"auth_source"`
	// ExternalID 是 LDAP DN 或 OIDC subject，仅外部账号使用，本地账号为空。
	ExternalID string `gorm:"column:external_id;type:varchar(255);not null;default:''" json:"external_id"`

	// PasswordHash 仅本地账号使用（argon2id / bcrypt），只出现在鉴权路径，
	// 不通过任何查询接口返回，日志与审计一律脱敏（MGR-USR-01）。
	PasswordHash       string `gorm:"column:password_hash;type:varchar(255);not null;default:''" json:"-"`
	MustChangePassword bool   `gorm:"column:must_change_password;not null;default:false" json:"-"`

	Role   UserRole   `gorm:"column:role;type:varchar(16);not null" json:"role"`
	Status UserStatus `gorm:"column:status;type:varchar(16);not null" json:"status"`

	// LastLoginAt 为 nil 表示从未登录过；用指针而非零值 time.Time，避免
	// Go 零值（0001-01-01）落库时超出 MySQL DATETIME 取值范围报错。
	LastLoginAt *time.Time `gorm:"column:last_login_at" json:"last_login_at"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名，不依赖 gorm 的复数推导。
func (User) TableName() string { return "users" }

// Validate 校验写入管理库前的枚举字段，边界输入必须显式拒绝。
func (u *User) Validate() error {
	if !u.AuthSource.Valid() {
		return fmt.Errorf("model: 未知的 auth_source %q", u.AuthSource)
	}
	if !u.Role.Valid() {
		return fmt.Errorf("model: 未知的用户角色 %q", u.Role)
	}
	if !u.Status.Valid() {
		return fmt.Errorf("model: 未知的用户状态 %q", u.Status)
	}
	return nil
}

// Redacted 返回可安全对外序列化的副本：password_hash 置空。
//
// PasswordHash 已标注 json:"-"，本方法用于日志、审计等不经过标准 JSON
// 序列化、但仍可能整体打印结构体的场景，避免上层不小心带出哈希。
func (u User) Redacted() User {
	u.PasswordHash = redactedString
	return u
}

// UserSession 是登录会话。
type UserSession struct {
	Base

	UserID uuidx.UUID `gorm:"column:user_id;type:binary(16);not null;index:idx_user_sessions_user_id" json:"user_id"`

	// TokenHash 是会话令牌的 SHA-256 哈希；Cookie 中只放明文令牌，管理库不
	// 落明文。不得出现在任何查询接口或日志中。
	TokenHash string `gorm:"column:token_hash;type:varchar(64);not null;uniqueIndex:uk_user_sessions_token_hash" json:"-"`

	ExpiresAt time.Time `gorm:"column:expires_at;not null;index:idx_user_sessions_expires_at" json:"expires_at"`
	ClientIP  string    `gorm:"column:client_ip;type:varchar(64);not null;default:''" json:"client_ip"`
	UserAgent string    `gorm:"column:user_agent;type:varchar(255);not null;default:''" json:"user_agent"`

	DeletedAt gorm.DeletedAt `gorm:"column:deleted_at;index" json:"-"`
}

// TableName 显式指定表名。
func (UserSession) TableName() string { return "user_sessions" }

// Redacted 返回可安全对外序列化的副本：token_hash 置空。
func (s UserSession) Redacted() UserSession {
	s.TokenHash = redactedString
	return s
}
