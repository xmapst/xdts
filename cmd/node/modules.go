package main

import (
	"fmt"
	"log/slog"
	"os"
	"syscall"

	"github.com/xmapst/xhive"

	"github.com/xmapst/xdts/internal/buildinfo"
	"github.com/xmapst/xdts/internal/node/bootstrap"
	"github.com/xmapst/xdts/internal/node/controller"
	"github.com/xmapst/xdts/internal/node/logship"
	"github.com/xmapst/xdts/internal/node/probe"
	"github.com/xmapst/xdts/internal/node/reporter"
)

type iInitErrReporter interface{ InitErr() error }

func collectInitErr(modules []xhive.IModule) error {
	for _, mod := range modules {
		initErrReporter, ok := mod.(iInitErrReporter)
		if !ok {
			continue
		}
		if err := initErrReporter.InitErr(); err != nil {
			return fmt.Errorf("模块 %s 初始化失败: %w", mod.Name(), err)
		}
	}
	return nil
}

func (a *application) runModules() error {
	cfg, mod, storeClient, conn, publisher := a.config, a.store, a.storeClient, a.conn, a.publisher

	reporterMod := reporter.New(reporter.Options{
		ProcessStartedAt:  a.startedAt,
		Center:            a.identity.Center,
		NodeID:            a.identity.NodeID,
		Token:             a.identity.Token,
		Publisher:         publisher,
		QueueInspector:    conn,
		Store:             storeClient,
		StatsFunc:         mod.Stats,
		RebuildEventsFunc: mod.RebuildEvents,
		NodeVersion:       buildinfo.Version(),
	})
	logshipMod := logship.New(logship.Options{
		Center:    a.identity.Center,
		NodeID:    a.identity.NodeID,
		Token:     a.identity.Token,
		Publisher: publisher,
	})

	if err := bootstrapControlPlane(conn, a.identity.NodeID, a.identity.Center); err != nil {
		return fmt.Errorf("接入控制面（controller/probe）失败: %w", err)
	}
	vhost, err := amqpVhost(cfg.AMQP.URL)
	if err != nil {
		return fmt.Errorf("解析 AMQP vhost 失败: %w", err)
	}
	brokerHost, err := amqpHost(cfg.AMQP.URL)
	if err != nil {
		return fmt.Errorf("解析 AMQP broker 主机名失败: %w", err)
	}
	probeMod := probe.New(probe.Options{
		Store:      storeClient,
		Center:     a.identity.Center,
		NodeID:     a.identity.NodeID,
		Token:      a.identity.Token,
		Publisher:  publisher,
		Conn:       conn,
		Vhost:      vhost,
		BrokerHost: brokerHost,
	})

	controllerMod := controller.New(controller.Options{
		Center:    a.identity.Center,
		NodeID:    a.identity.NodeID,
		Token:     a.identity.Token,
		Publisher: publisher,
		Conn:      conn,
		Store:     storeClient,
		Resources: cfg.Resources,

		ShutdownTimeout:       cfg.Controller.ShutdownTimeout,
		RequestRetirementStop: func() { _ = syscall.Kill(os.Getpid(), syscall.SIGTERM) },
	})

	a.logging.attach(logship.NewHandler(logshipMod, nil))
	a.resources.Defer(a.logging.detach)
	if err := xhive.RegisterSignal(func() { a.logging.reload(a.options) }, syscall.SIGHUP); err != nil {
		return fmt.Errorf("注册 SIGHUP 处理器失败: %w", err)
	}

	modules := []xhive.IModule{mod, reporterMod, logshipMod, probeMod, controllerMod}
	slog.Info("node starting",
		slog.Uint64("store_priority", uint64(mod.Priority())),
		slog.Uint64("reporter_priority", uint64(reporterMod.Priority())),
		slog.Uint64("logship_priority", uint64(logshipMod.Priority())),
		slog.Uint64("probe_priority", uint64(probeMod.Priority())),
		slog.Uint64("controller_priority", uint64(controllerMod.Priority())))
	xhive.Run(modules...)
	if err := collectInitErr(modules); err != nil {
		return fmt.Errorf("node 启动失败: %w", err)
	}
	if controllerMod.IdentityRetired() {
		return bootstrap.ErrIdentityRetired
	}
	return nil
}
