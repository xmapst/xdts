package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/config"
	mgrhttp "github.com/xmapst/xdts/internal/manager/http"
)

func argon2ParamsFromConfig(c config.Argon2Config) auth.Argon2Params {
	return auth.Argon2Params{
		MemoryKiB:   c.MemoryKiB,
		Iterations:  c.Iterations,
		Parallelism: c.Parallelism,
		SaltLength:  c.SaltLength,
		KeyLength:   c.KeyLength,
	}
}

func bootstrapLocalAdmin(cfg *config.Config, server *mgrhttp.Server) error {
	result, err := auth.BootstrapAdmin(context.Background(), server.LocalUsers(), server.Users(),
		cfg.Auth.Local.BootstrapUser, cfg.Auth.Local.BootstrapPassword, argon2ParamsFromConfig(cfg.Auth.Local.Argon2))
	if err != nil {
		return err
	}

	switch result {
	case auth.BootstrapCreated:
		slog.Info("initial local admin account created")
	case auth.BootstrapSkipped:
		slog.Info("skipped initial admin bootstrap")
	case auth.BootstrapNoCredentials:
		return errors.New("auth: 本地账号表为空，且未配置 auth.local.bootstrap_user/bootstrap_password" +
			"（或对应的 XDTS_AUTH_LOCAL_BOOTSTRAP_USER/XDTS_AUTH_LOCAL_BOOTSTRAP_PASSWORD 环境变量），" +
			"系统当前没有任何账号可以登录：请配置这两项后重启")
	}
	return nil
}

func checkLoginMethodsUsable(a config.AuthConfig) error {
	enabled := mgrhttp.LoginMethods{
		Local: a.Local.Enabled,
		LDAP:  a.LDAP.Enabled,
		OIDC:  a.OIDC.Enabled,
	}
	unimplemented := mgrhttp.UnimplementedEnabledMethods(enabled)
	if mgrhttp.AnyEnabledMethodImplemented(enabled) {
		// 还有可用的方式，不阻塞启动；但启用了却没实现的那些要说出来，
		// 否则运维会以为自己已经把 LDAP 接上了。
		if len(unimplemented) > 0 {
			slog.Warn("configured login method not implemented in this build",
				slog.Any("unimplemented", unimplemented))
		}
		return nil
	}
	return fmt.Errorf(
		"config: 已启用的登录方式（%s）在当前版本中尚未实现，没有任何可用的登录入口——"+
			"请启用 auth.local 或改用已实现该方式的版本",
		strings.Join(unimplemented, ", "))
}
