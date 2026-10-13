package main

import (
	"io"
	"log/slog"

	"github.com/xmapst/xdts/internal/manager/config"
)

type logging struct {
	closer io.Closer
}

func (l *logging) setup(cfg config.LogConfig) error {
	logger, closer, err := config.SetupLogger(cfg)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	previous := l.closer
	l.closer = closer
	if previous != nil {
		_ = previous.Close()
	}
	return nil
}

func (l *logging) reload(options config.LoadOptions) {
	slog.Info("received sighup, reloading log config")
	cfg, err := config.Load(options)
	if err == nil {
		err = l.setup(cfg.Log)
	}
	if err != nil {
		slog.Error("log config reload failed", slog.Any("error", err))
		return
	}
	slog.Info("log config reloaded")
}

func (l *logging) Close() error {
	if l.closer == nil {
		return nil
	}
	closer := l.closer
	l.closer = nil
	return closer.Close()
}
