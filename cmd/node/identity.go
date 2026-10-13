package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/xmapst/xdts/internal/buildinfo"
	"github.com/xmapst/xdts/internal/node/bootstrap"
)

func (a *application) resolveIdentity(ctx context.Context) error {
	cfg, mod, conn := a.config, a.store, a.conn

	startupCtx, stopStartupSignals := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancelStartup := context.WithTimeout(startupCtx, 45*time.Second)
	defer cancelStartup()
	defer stopStartupSignals()
	hostname, _ := os.Hostname()
	var addresses []string
	if localAddresses, err := net.InterfaceAddrs(); err == nil {
		for _, address := range localAddresses {
			if address, ok := address.(*net.IPNet); ok && !address.IP.IsLoopback() {
				addresses = append(addresses, address.IP.String())
			}
		}
	}
	identity, identityErr := bootstrap.New(bootstrap.Options{
		Store: mod, Registrar: bootstrap.NewAMQPRegistrar(conn),
		RegisterCode: cfg.Node.RegisterCode,
		Hostname:     hostname,
		IPs:          addresses,
		OSArch:       runtime.GOOS + "/" + runtime.GOARCH,
		Version:      buildinfo.Version(),
	}).Resolve(ctx)
	if identityErr != nil {
		return identityErr
	}
	a.identity = identity
	slog.Info("node identity ready", slog.String("node_id", identity.NodeID), slog.String("center_code", identity.Center))

	return nil
}

func (a *application) waitForRegistration(ctx context.Context) error {
	slog.Warn("node waiting for registration", slog.String("reason", "NODE_IDENTITY_RETIRED"))
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	return nil
}
