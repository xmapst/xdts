package mysqlout

import (
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"
)

// BatchTxnError identifies exactly which source transaction failed. Earlier
// transactions may already have committed and must be preserved by callers.
type BatchTxnError struct {
	Index int
	Err   error
}

func (e *BatchTxnError) Error() string {
	if sqlErr, ok := errors.AsType[*mysql.MySQLError](e.Err); ok {
		return fmt.Sprintf("mysqlout: source transaction %d failed (MySQL %d)", e.Index, sqlErr.Number)
	}
	return fmt.Sprintf("mysqlout: source transaction %d failed", e.Index)
}
func (e *BatchTxnError) Unwrap() error { return e.Err }

type rolledBackSQLFailure struct{ error }

func (e rolledBackSQLFailure) Unwrap() error { return e.error }

// CanSkipFailedTransaction requires an actual SQL failure and a confirmed
// rollback/session reset. It never authorizes unknown commit or protocol errors.
func CanSkipFailedTransaction(err error) bool {
	var rolledBack rolledBackSQLFailure
	var sqlErr *mysql.MySQLError
	return errors.As(err, &rolledBack) && errors.As(err, &sqlErr) && !IsRetryable(err) && !errors.Is(err, ErrConflictStopped)
}
