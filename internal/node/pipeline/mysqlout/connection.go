package mysqlout

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/go-sql-driver/mysql"
)

var errWriterClosed = fmt.Errorf("mysqlout: writer is closed: %w", sql.ErrConnDone)

// sessionRestoreError marks a session unsafe for another GTID, even when the
// original SQL failure would normally be permanent.
type sessionRestoreError struct{ error }

func (e sessionRestoreError) Unwrap() error { return e.error }

func isConnectionError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, mysql.ErrInvalidConn) ||
		errors.Is(err, sql.ErrConnDone) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if _, ok := errors.AsType[sessionRestoreError](err); ok {
		return true
	}
	_, ok := errors.AsType[net.Error](err)
	return ok
}

func (w *Writer) observeConnectionError(err error) {
	// Cancellation during driver IO can close a connection without returning a
	// network error. Discard it before the next independently bounded attempt.
	if isConnectionError(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		w.broken = true
	}
}

// ensureConnection runs only in the Writer's owning execution context. It
// reconnects once per invocation, without retrying SQL or changing GTID state.
func (w *Writer) ensureConnection(ctx context.Context) error {
	if w.closed || w.db == nil {
		return errWriterClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.conn != nil && !w.broken {
		return nil
	}
	if w.conn != nil {
		// Close alone could return a session with residual GTID_NEXT to the
		// pool. Raw's ErrBadConn forces physical disposal instead.
		_ = w.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = w.conn.Close()
		w.conn = nil
	}
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()
	conn, err := w.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("mysqlout: reconnect target failed: %w", err)
	}
	ready := false
	defer func() {
		if !ready {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			_ = conn.Close()
		}
	}()
	if err := conn.PingContext(ctx); err != nil {
		return fmt.Errorf("mysqlout: reconnected target unreachable: %w", err)
	}
	if err := w.initSession(ctx, conn); err != nil {
		return err
	}
	if w.catchingUp {
		if _, err := conn.ExecContext(ctx, "SET SESSION foreign_key_checks = 0"); err != nil {
			return fmt.Errorf("mysqlout: restore catchup session failed: %w", err)
		}
	}
	w.conn, w.broken = conn, false
	ready = true
	return nil
}
