package controller

import (
	"errors"

	"github.com/go-sql-driver/mysql"
)

// commandRetryable classifies known MySQL outcomes. Unknown errors remain
// absent; treating an arbitrary failure as permanent would prevent recovery.
func commandRetryable(err error) *bool {
	var mysqlErr *mysql.MySQLError
	if !errors.As(err, &mysqlErr) {
		return nil
	}
	var retryable bool
	switch mysqlErr.Number {
	case 1205, 1213, 1290:
		retryable = true
	case 1044, 1045, 1049, 1054, 1060, 1062, 1064, 1142, 1143, 1146:
		retryable = false
	default:
		return nil
	}
	return &retryable
}

//go:fix inline
func explicitRetryable(value bool) *bool { return new(value) }

// A permanent side failure ends the whole command. Unknown side failures keep
// their classification absent even when another side is retryable.
func outcomesRetryable(outcomes []cmdSideOutcome) *bool {
	knownRetry := false
	unknown := false
	for _, outcome := range outcomes {
		if outcome.err == nil {
			continue
		}
		decision := commandRetryable(outcome.err)
		if decision == nil {
			unknown = true
			continue
		}
		if !*decision {
			return new(false)
		}
		knownRetry = true
	}
	if unknown || !knownRetry {
		return nil
	}
	return new(true)
}
