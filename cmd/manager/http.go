package main

import (
	"github.com/xmapst/xdts/internal/manager/auth"
	mgrhttp "github.com/xmapst/xdts/internal/manager/http"
)

func (a *application) newHTTPServer() (*mgrhttp.Server, error) {
	cfg, db := a.config, a.db
	return mgrhttp.New(mgrhttp.Options{
		Listen:          cfg.HTTP.Listen,
		ShutdownTimeout: cfg.HTTP.ShutdownTimeout,
		CookieSecure:    cfg.HTTP.CookieSecure,
		SessionTTL:      cfg.Auth.SessionTTL,
		LDAPConfig:      cfg.Auth.LDAP.AuthenticatorConfig(cfg.Auth.DefaultExternalRole),
		OIDCConfig:      cfg.Auth.OIDC.AuthenticatorConfig(cfg.Auth.DefaultExternalRole),
		Argon2Params:    argon2ParamsFromConfig(cfg.Auth.Local.Argon2),
		AuthMethods: mgrhttp.LoginMethods{
			Local: cfg.Auth.Local.Enabled,
			LDAP:  cfg.Auth.LDAP.Enabled,
			OIDC:  cfg.Auth.OIDC.Enabled,
		},
		LockoutPolicy: auth.LockoutPolicy{
			Threshold:    cfg.Auth.Lockout.Threshold,
			Window:       cfg.Auth.Lockout.Window,
			LockDuration: cfg.Auth.Lockout.LockDuration,
		},
		DB:         db,
		Sessions:   mgrhttp.NewDBSessionStore(db),
		Users:      mgrhttp.NewDBUserStore(db),
		LocalUsers: mgrhttp.NewDBLocalUserStore(db),

		Lockouts: mgrhttp.NewDBLockoutStore(db),

		Audit: mgrhttp.NewDBAuditRecorder(db),

		Dispatcher:      a.background.dispatcher,
		CleanupWake:     a.background.reclaim.Wake,
		EndpointProber:  a.background.inspection,
		LogRetention:    cfg.Retention.Logs,
		RuntimeSettings: true,

		BinlogServerIDRangeMin: cfg.Pipeline.BinlogServerIDRangeMin,
		BinlogServerIDRangeMax: cfg.Pipeline.BinlogServerIDRangeMax,
	})
}
