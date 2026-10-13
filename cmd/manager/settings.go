package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/repo"
)

func (a *application) initializeSettings(ctx context.Context) error {
	cfg, db := a.config, a.db
	for _, retention := range []time.Duration{cfg.Retention.Logs, cfg.Retention.Metrics, cfg.Retention.Events, cfg.Retention.Conflicts} {
		if retention < time.Second || retention%time.Second != 0 {
			return errors.New("运行设置保留期要求至少1秒且为整秒")
		}
	}
	settingsDefaults := repo.RuntimeSettings{
		LogRetentionSeconds:      int64(cfg.Retention.Logs / time.Second),
		MetricsRetentionSeconds:  int64(cfg.Retention.Metrics / time.Second),
		EventRetentionSeconds:    int64(cfg.Retention.Events / time.Second),
		ConflictRetentionSeconds: int64(cfg.Retention.Conflicts / time.Second),
		PrecheckValiditySeconds:  repo.DefaultPrecheckValiditySeconds,
	}
	settingsCtx, settingsCancel := context.WithTimeout(ctx, 10*time.Second)
	settingsErr := repo.InitializeRuntimeSettings(settingsCtx, db, settingsDefaults, time.Now())
	settingsCancel()
	if settingsErr != nil {
		return fmt.Errorf("初始化运行设置失败: %w", settingsErr)
	}
	return nil
}
