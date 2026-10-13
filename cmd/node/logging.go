package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/xmapst/xdts/internal/node/config"
	"github.com/xmapst/xdts/internal/node/logship"
)

type fanoutHandler struct {
	handlers []slog.Handler
}

func newFanoutHandler(handlers ...slog.Handler) *fanoutHandler {
	return &fanoutHandler{handlers: handlers}
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, sub := range h.handlers {
		if sub.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Clone each record before passing it to an independent handler.
func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, sub := range h.handlers {
		if !sub.Enabled(ctx, r.Level) {
			continue
		}
		if err := sub.Handle(ctx, r.Clone()); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, sub := range h.handlers {
		next[i] = sub.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: next}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	next := make([]slog.Handler, len(h.handlers))
	for i, sub := range h.handlers {
		next[i] = sub.WithGroup(name)
	}
	return &fanoutHandler{handlers: next}
}

// Logging owns both sinks; signal callbacks and final cleanup serialize here.
type logging struct {
	mu     sync.Mutex
	local  *slog.Logger
	closer io.Closer
	ship   *logship.Handler
	closed bool
}

func (l *logging) setup(cfg config.LogConfig) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return errors.New("node: logging is closed")
	}
	logger, closer, err := config.SetupLogger(cfg)
	if err != nil {
		return err
	}
	previous := l.closer
	l.local, l.closer = logger, closer
	l.install()
	if previous != nil {
		_ = previous.Close()
	}
	return nil
}

func (l *logging) attach(ship *logship.Handler) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ship = ship
	l.install()
}

func (l *logging) install() {
	logger := l.local
	if l.ship != nil {
		logger = slog.New(newFanoutHandler(logger.Handler(), l.ship))
	}
	slog.SetDefault(logger)
}

func (l *logging) detach() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ship != nil {
		l.ship.Close()
		l.ship = nil
		l.install()
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
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if l.ship != nil {
		l.ship.Close()
		l.ship = nil
	}
	if l.closer != nil {
		return l.closer.Close()
	}
	return nil
}
