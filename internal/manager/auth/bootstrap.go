package auth

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/pkg/uuidx"
)

// BootstrapAdminResult 描述一次 BootstrapAdmin 调用实际做了什么，供调用方
// （cmd/manager/main.go）决定日志怎么写——日志只能说“创建了”或“跳过了”，
// 不能带出凭据本身。
type BootstrapAdminResult int

const (
	// BootstrapSkipped 表示没有创建账号，而且这是正常状态：本地账号表
	// 已经非空，按设计忽略配置（不会覆盖、也不会额外创建）。这样运维在
	// 账号已经建好之后把 bootstrap_password 从配置文件里删掉——密码不
	// 应该永久留在配置文件里，这是正确的运维动作——不会导致下次启动
	// 异常，与 BootstrapNoCredentials 是两种完全不同的状态，不能合并。
	BootstrapSkipped BootstrapAdminResult = iota
	// BootstrapCreated 表示成功创建了初始管理员账号。
	BootstrapCreated
	// BootstrapNoCredentials means the empty local account store cannot create its first
	// administrator. The caller must fail startup after persisted accounts are checked.
	BootstrapNoCredentials
)

// BootstrapAdmin 在本地账号表为空时，从配置里的初始管理员用户名/密码创建
// 账号。username/password 来自
// config.LocalAuthConfig.BootstrapUser/BootstrapPassword——两者要么同时
// 非空、要么同时为空，由 config.LocalAuthConfig.validate() 在配置加载阶段
// 保证，本函数不重复这层校验。
func BootstrapAdmin(ctx context.Context, localUsers ILocalUserStore, users IUserStore, username, password string, argon2Params Argon2Params) (BootstrapAdminResult, error) {
	has, err := localUsers.Any(ctx)
	if err != nil {
		return BootstrapSkipped, fmt.Errorf("auth: 检查本地账号是否为空失败: %w", err)
	}
	if has {
		return BootstrapSkipped, nil
	}
	if username == "" || password == "" {
		return BootstrapNoCredentials, nil
	}

	hash, err := HashPassword(password, argon2Params)
	if err != nil {
		return BootstrapSkipped, fmt.Errorf("auth: 生成初始管理员密码哈希失败: %w", err)
	}

	userID := uuidx.New().String()
	if err := localUsers.Create(ctx, LocalCredential{
		UserID:             userID,
		Username:           username,
		PasswordHash:       hash,
		Role:               RoleAdmin,
		MustChangePassword: true,
	}); err != nil {
		return BootstrapSkipped, fmt.Errorf("auth: 写入初始管理员本地凭据失败: %w", err)
	}

	return BootstrapCreated, nil
}
