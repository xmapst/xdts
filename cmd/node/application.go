package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/xmapst/xdts/internal/lifecycle"
	"github.com/xmapst/xdts/internal/node/bootstrap"
	"github.com/xmapst/xdts/internal/node/config"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/internal/transport"
)

type application struct {
	options     config.LoadOptions
	startedAt   time.Time
	config      *config.Config
	identity    bootstrap.Identity
	resources   lifecycle.Scope
	logging     logging
	store       *store.Module
	storeClient *store.Client
	conn        *transport.Conn
	publisher   *transport.Publisher
}

func newApplication(options config.LoadOptions, startedAt time.Time) *application {
	return &application{options: options, startedAt: startedAt}
}

func (a *application) Run(ctx context.Context) (err error) {
	defer func() { err = errors.Join(err, a.resources.Close(), a.logging.Close()) }()
	if err := a.load(); err != nil {
		return err
	}
	if err = a.prepare(); err == nil {
		err = a.resolveIdentity(ctx)
	}
	if err == nil {
		err = a.runModules()
	}
	// The framework has flushed modules before their shared transport is released.
	if closeErr := a.resources.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if errors.Is(err, bootstrap.ErrIdentityRetired) {
		return a.waitForRegistration(ctx)
	}
	if err == nil {
		slog.Info("node exited")
	}
	return err
}

func (a *application) load() error {
	cfg, err := config.Load(a.options)
	if err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}
	a.config = cfg
	if err := a.logging.setup(cfg.Log); err != nil {
		return fmt.Errorf("初始化日志失败: %w", err)
	}
	slog.Info("config loaded", slog.Any("config", cfg))
	return nil
}
