package snapshot

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
)

func (r *Reader) signalRuntimeLocked() {
	close(r.runtimeChanged)
	r.runtimeChanged = make(chan struct{})
}

// A ticket lasts through synchronous publication, so pausing observes both
// source reads and publication completion. No external IO holds runtimeMu.
func (r *Reader) acquireRuntime(ctx context.Context) (ReaderOptions, func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return ReaderOptions{}, nil, err
		}
		r.runtimeMu.Lock()
		if !r.runtimePaused && r.runtimeActive < r.runtime.Concurrency {
			r.runtimeActive++
			opts := r.runtime
			r.runtimeMu.Unlock()
			var once sync.Once
			return opts, func() {
				once.Do(func() { r.runtimeMu.Lock(); r.runtimeActive--; r.signalRuntimeLocked(); r.runtimeMu.Unlock() })
			}, nil
		}
		changed := r.runtimeChanged
		r.runtimeMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ReaderOptions{}, nil, ctx.Err()
		}
	}
}

// PauseRuntime owns the update gate until ResumeRuntime. A failed pause always
// releases its gate; a successful caller must resume even when preparation fails.
func (r *Reader) PauseRuntime(ctx context.Context) error {
	r.runtimeMu.Lock()
	if r.runtimePaused {
		r.runtimeMu.Unlock()
		return errors.New("snapshot: runtime update already pending")
	}
	r.runtimePaused = true
	r.signalRuntimeLocked()
	for r.runtimeActive > 0 {
		changed := r.runtimeChanged
		r.runtimeMu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			r.ResumeRuntime()
			return ctx.Err()
		}
		r.runtimeMu.Lock()
	}
	r.runtimeMu.Unlock()
	if err := ctx.Err(); err != nil {
		r.ResumeRuntime()
		return err
	}
	return nil
}

func (r *Reader) ResumeRuntime() {
	r.runtimeMu.Lock()
	defer r.runtimeMu.Unlock()
	r.runtimePaused = false
	r.signalRuntimeLocked()
}

func ValidateRuntimeConfig(config iproto.SnapshotSourceRuntimeUpdate) error {
	if config.ReadConcurrency == 0 || config.ReadConcurrency > 128 || config.MaxRows == 0 || config.MaxBytes == 0 || config.Update.Config.TableFilter == nil || config.Update.Config.ColumnFilter == nil {
		return errors.New("snapshot: invalid runtime configuration")
	}
	return nil
}

// InstallRuntime is pure computation with all read/publication tickets drained.
func (r *Reader) InstallRuntime(config iproto.SnapshotSourceRuntimeUpdate) error {
	if err := ValidateRuntimeConfig(config); err != nil {
		return err
	}
	r.runtimeMu.Lock()
	defer r.runtimeMu.Unlock()
	if !r.runtimePaused || r.runtimeActive != 0 {
		return errors.New("snapshot: runtime is not drained")
	}
	r.runtime.ColumnFilter, r.runtime.TableFilter = config.Update.Config.ColumnFilter, config.Update.Config.TableFilter
	r.boundaries.SetTableFilter(config.Update.Config.TableFilter)
	r.runtime.RowFilter = config.Update.Config.RowFilter
	r.runtime.Transforms = config.Update.Config.Transforms
	r.runtime.Concurrency, r.runtime.MaxRows, r.runtime.MaxBytes = config.ReadConcurrency, config.MaxRows, config.MaxBytes
	r.runtime.RowsPerSec, r.runtime.BytesPerSec = config.RowsPerSec, config.BytesPerSec
	r.limiter.reconfigure(config.RowsPerSec, config.BytesPerSec)
	return nil
}

func (l *rateLimiter) reconfigure(rows, bytes uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.rowsRate == float64(rows) && l.bytesRate == float64(bytes) {
		return
	}
	now := time.Now()
	elapsed := max(0, now.Sub(l.last).Seconds())
	for _, bucket := range []struct {
		rate   *float64
		tokens *float64
		next   uint64
	}{{&l.rowsRate, &l.rowTokens, rows}, {&l.bytesRate, &l.byteTokens, bytes}} {
		old := *bucket.rate
		*bucket.tokens = min(old, *bucket.tokens+elapsed*old)
		if old == float64(bucket.next) {
			continue
		}
		*bucket.rate = float64(bucket.next)
		if old == 0 || bucket.next == 0 {
			*bucket.tokens = *bucket.rate
		} else {
			*bucket.tokens = min(*bucket.tokens, *bucket.rate)
		}
	}
	l.last = now
}
