package main

import (
	"context"
	"fmt"

	"github.com/xmapst/xdts/internal/node/bootstrap"
	"github.com/xmapst/xdts/internal/node/store"
)

func (a *application) prepare() error {
	cfg := a.config

	mod := store.New(store.Options{
		DataDir:         cfg.Node.DataDir,
		RegisterCode:    cfg.Node.RegisterCode,
		BatchInterval:   cfg.Store.BatchInterval,
		BatchMaxOps:     cfg.Store.BatchMaxOps,
		ShutdownTimeout: cfg.Store.ShutdownTimeout,
	})
	if err := mod.Prepare(); err != nil {
		return fmt.Errorf("准备本地状态库失败: %w", err)
	}
	a.resources.Defer(mod.Close)
	var storedIdentity bootstrap.Identity
	if err := mod.Get(context.Background(), "identity", bootstrap.IdentityRecordID, &storedIdentity); err == nil {
		if err := bootstrap.CheckRetirement(context.Background(), mod, storedIdentity.NodeID); err != nil {
			return err
		}
	}

	// Bind the client's RPC endpoint without mutating the framework registry.
	storeClient := store.NewClient(mod.ChanRPC(), mod)
	a.resources.Defer(func() error { storeClient.Close(); return nil })

	conn, publisher, err := bootstrapReportingModules(cfg.AMQP.URL)
	if err != nil {
		return fmt.Errorf("接入上报面（reporter/logship）失败: %w", err)
	}
	a.resources.Defer(conn.Close)
	a.resources.Defer(func() error { publisher.Close(); return nil })
	a.store, a.storeClient, a.conn, a.publisher = mod, storeClient, conn, publisher
	return nil
}
