package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/lifecycle"
	"github.com/xmapst/xdts/internal/manager/config"
	mgrhttp "github.com/xmapst/xdts/internal/manager/http"
	"github.com/xmapst/xdts/internal/manager/repo"
)

type application struct {
	options    config.LoadOptions
	config     *config.Config
	resources  lifecycle.Scope
	db         *gorm.DB
	server     *mgrhttp.Server
	background backgroundTasks
	logging    logging
}

func newApplication(options config.LoadOptions) *application {
	return &application{options: options}
}

func (a *application) Run(ctx context.Context) (err error) {
	defer func() { err = errors.Join(err, a.resources.Close(), a.logging.Close()) }()
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	if err := a.load(); err != nil {
		return err
	}
	if err := checkLoginMethodsUsable(a.config.Auth); err != nil {
		return err
	}
	if err := a.openDatabase(); err != nil {
		return err
	}
	if err := repo.NewMigrator(a.db).Migrate(ctx, time.Now()); err != nil {
		return fmt.Errorf("管理库迁移失败: %w", err)
	}
	if err := a.initializeSettings(ctx); err != nil {
		return err
	}
	slog.Info("database connected and migrated")

	// AMQP is released after every task that can publish or query it.
	var transportResources lifecycle.Scope
	a.resources.Defer(transportResources.Close)
	a.background.start(a.config, a.db, &a.resources, &transportResources)
	a.server, err = a.newHTTPServer()
	if err != nil {
		return fmt.Errorf("初始化 http 服务失败: %w", err)
	}
	a.resources.Defer(a.shutdownHTTP)
	if a.config.Auth.Local.Enabled {
		if err := bootstrapLocalAdmin(a.config, a.server); err != nil {
			return fmt.Errorf("初始化初始管理员失败: %w", err)
		}
	}

	err = a.wait(ctx, a.server.Start())
	if closeErr := a.resources.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	if err == nil {
		slog.Info("manager exited")
	}
	return err
}

func (a *application) Migrate(ctx context.Context) (err error) {
	defer func() { err = errors.Join(err, a.resources.Close(), a.logging.Close()) }()
	if err := a.load(); err != nil {
		return err
	}
	if err := a.openDatabase(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	return repo.NewMigrator(a.db).Migrate(ctx, time.Now())
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
	slog.Info("manager config loaded", slog.Any("config", cfg))
	return nil
}

func (a *application) openDatabase() error {
	db, err := openManagerDB(a.config.Database)
	if err != nil {
		return fmt.Errorf("连接管理库失败: %w", err)
	}
	pool, err := db.DB()
	if err != nil {
		return err
	}
	a.db = db
	a.resources.Defer(pool.Close)
	return nil
}

func (a *application) shutdownHTTP() error {
	ctx, cancel := context.WithTimeout(context.Background(), a.config.HTTP.ShutdownTimeout+5*time.Second)
	defer cancel()
	return a.server.Shutdown(ctx)
}

func (a *application) wait(ctx context.Context, serveErrors <-chan error) error {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	for {
		select {
		case <-ctx.Done():
			slog.Info("received shutdown signal")
			return nil
		case <-hup:
			a.logging.reload(a.options)
		case err := <-serveErrors:
			slog.Error("http server exited unexpectedly", slog.Any("error", err))
			return err
		}
	}
}
