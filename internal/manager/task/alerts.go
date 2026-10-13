package task

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"gorm.io/gorm"
)

type AlertOptions struct {
	DB         *gorm.DB
	Interval   time.Duration
	HTTPClient *http.Client
}

// Run owns the loop. RunOnceAt also takes a database connection lock, so
// different Managers cannot concurrently evaluate or notify the same rules.
type AlertLoop struct {
	db       *gorm.DB
	interval time.Duration
	client   *http.Client
}

func NewAlertLoop(opts AlertOptions) *AlertLoop {
	interval := opts.Interval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	if opts.HTTPClient != nil {
		copy := *opts.HTTPClient
		copy.Timeout = 5 * time.Second
		copy.CheckRedirect = client.CheckRedirect
		client = &copy
	}
	return &AlertLoop{db: opts.DB, interval: interval, client: client}
}

func (l *AlertLoop) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			runCtx, cancel := context.WithTimeout(ctx, time.Minute)
			err := l.RunOnceAt(runCtx, time.Now())
			cancel()
			if err != nil && ctx.Err() == nil {
				slog.Error("alert evaluation failed", slog.Any("error", err))
			}
			timer.Reset(l.interval)
		}
	}
}

func (l *AlertLoop) RunOnceAt(ctx context.Context, now time.Time) error {
	if l.db == nil || now.IsZero() {
		return errors.New("alert evaluation dependencies missing")
	}
	sqlDB, err := l.db.DB()
	if err != nil {
		return err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(conn.Close)
	var schema string
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return err
	}
	key := fmt.Sprintf("xdts.alerts.%x", sha256.Sum256([]byte(schema)))[:63]
	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", key).Scan(&acquired); err != nil {
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return err
	}
	if !acquired.Valid || acquired.Int64 != 1 {
		return nil
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var released sql.NullInt64
		if err := conn.QueryRowContext(releaseCtx, "SELECT RELEASE_LOCK(?)", key).Scan(&released); err != nil || !released.Valid || released.Int64 != 1 {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
	}()
	db := l.db.Session(&gorm.Session{NewDB: true}).WithContext(ctx)
	db.Statement.ConnPool = conn
	var rules []model.AlertRule
	if err := db.Limit(1001).Order("id").Find(&rules).Error; err != nil {
		return err
	}
	if len(rules) > 1000 {
		return errors.New("alert rule evaluation limit exceeded")
	}
	if len(rules) == 0 {
		return nil
	}
	s, err := loadAlertSnapshot(ctx, db, now)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range rules {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := l.applyRule(ctx, db, r, s, now); err != nil {
			errs = append(errs, err)
			continue
		}
		if err := l.notifyRule(ctx, db, r.ID, now); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
