package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/repo"
)

const DefaultRetentionCheckInterval = 24 * time.Hour

type RetentionOptions struct {
	DB              *gorm.DB
	PartitionMu     *sync.Mutex
	Logs            time.Duration
	Metrics         time.Duration
	Events          time.Duration
	Conflicts       time.Duration
	Interval        time.Duration
	RuntimeSettings bool
}

type RetentionLoop struct {
	opts RetentionOptions
}

func NewRetentionLoop(opts RetentionOptions) *RetentionLoop {
	if opts.Interval <= 0 {
		opts.Interval = DefaultRetentionCheckInterval
	}
	return &RetentionLoop{opts: opts}
}

func (l *RetentionLoop) RunOnce(ctx context.Context) error {
	return l.RunOnceAt(ctx, time.Now())
}

// RunOnceAt 先验证全部保留期，再独立清理每张表。
func (l *RetentionLoop) RunOnceAt(ctx context.Context, now time.Time) error {
	if l.opts.DB == nil {
		return errors.New("retention: database unavailable")
	}
	logs, metrics, events, conflicts := l.opts.Logs, l.opts.Metrics, l.opts.Events, l.opts.Conflicts
	if l.opts.RuntimeSettings {
		settings, err := repo.ReadRuntimeSettings(ctx, l.opts.DB)
		if err != nil {
			return fmt.Errorf("retention: read runtime settings: %w", err)
		}
		logs = time.Duration(settings.LogRetentionSeconds) * time.Second
		metrics = time.Duration(settings.MetricsRetentionSeconds) * time.Second
		events = time.Duration(settings.EventRetentionSeconds) * time.Second
		conflicts = time.Duration(settings.ConflictRetentionSeconds) * time.Second
	}
	for _, period := range []struct {
		name     string
		duration time.Duration
	}{
		{"logs", logs}, {"metrics", metrics},
		{"events", events}, {"conflicts", conflicts},
	} {
		if period.duration <= 0 {
			return fmt.Errorf("retention: %s must be positive", period.name)
		}
	}

	var errs []error
	if l.opts.PartitionMu != nil {
		l.opts.PartitionMu.Lock()
	}
	for _, table := range []struct {
		name      string
		retention time.Duration
	}{
		{repo.TableLogs, logs}, {repo.TablePipelineMetrics, metrics},
	} {
		if err := repo.EnsureFuturePartitions(ctx, l.opts.DB, table.name, now, repo.DefaultPartitionLookahead); err != nil {
			slog.Error("retention partition pre-create failed", slog.String("table", table.name), slog.Any("error", err))
			errs = append(errs, err)
			continue
		}
		if err := repo.DropExpiredPartitions(ctx, l.opts.DB, table.name, now, table.retention); err != nil {
			slog.Error("retention partition drop failed", slog.String("table", table.name), slog.Any("error", err))
			errs = append(errs, err)
		}
	}
	if l.opts.PartitionMu != nil {
		l.opts.PartitionMu.Unlock()
	}
	for _, table := range []struct {
		name      string
		retention time.Duration
	}{
		{"events", events}, {"conflicts", conflicts},
	} {
		count, err := repo.DeleteInBatches(ctx, l.opts.DB, table.name, "ts < ?", []any{now.Add(-table.retention)},
			repo.DefaultBatchDeleteSize, repo.DefaultBatchDeleteInterval)
		if err != nil {
			slog.Error("retention row delete failed", slog.String("table", table.name), slog.Any("error", err))
			errs = append(errs, err)
			continue
		}
		if count > 0 {
			slog.Info("retention rows deleted", slog.String("table", table.name), slog.Int64("count", count))
		}
	}
	if count, err := repo.DeleteExpiredUserSessions(ctx, l.opts.DB, now); err != nil {
		slog.Error("retention session delete failed", slog.Any("error", err))
		errs = append(errs, err)
	} else if count > 0 {
		slog.Info("retention rows deleted", slog.String("table", "user_sessions"), slog.Int64("count", count))
	}
	if count, err := repo.CleanupExpiredLoginLockouts(ctx, l.opts.DB, now, repo.DefaultLoginLockoutRetention); err != nil {
		slog.Error("retention lockout delete failed", slog.Any("error", err))
		errs = append(errs, err)
	} else if count > 0 {
		slog.Info("retention rows deleted", slog.String("table", "login_lockouts"), slog.Int64("count", count))
	}
	return errors.Join(errs...)
}

func (l *RetentionLoop) Run(ctx context.Context) {
	if err := l.RunOnce(ctx); err != nil {
		slog.Error("retention cleanup failed", slog.Any("error", err))
	}
	ticker := time.NewTicker(l.opts.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := l.RunOnce(ctx); err != nil {
				slog.Error("retention cleanup failed", slog.Any("error", err))
			}
		}
	}
}
