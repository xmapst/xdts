package mysqlout

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
)

type replayOutcome struct {
	index, worker int
	applied       bool
	err           error
}

// execute receives exclusive ownership of one writer for a whole transaction.
// beforeCommit is nil unless the caller requests source commit order.
type replayExecute func(context.Context, int, *iproto.Txn, func(context.Context) error) (bool, error)

// scheduleReplay has one owner. Only immutable transactions and independent
// result values cross worker boundaries; no row or positioning-key partitioning
// is permitted. Dependencies refer to received transactions, so filtered clock
// gaps do not cause waits for transactions that will never arrive.
func scheduleReplay(ctx context.Context, txns []*iproto.Txn, degree int, ordered bool, execute replayExecute) (iproto.BatchApplyResult, error) {
	var result iproto.BatchApplyResult
	if degree < 1 || degree > 128 || execute == nil {
		return result, errors.New("mysqlout: invalid replay scheduler")
	}
	degree = min(degree, len(txns))
	results := make(chan replayOutcome, max(1, degree))
	busy := make(map[int]int, degree) // transaction index -> writer index
	cancels := make(map[int]context.CancelFunc, degree)
	free := make([]int, degree)
	for i := range free {
		free[i] = i
	}
	done := make([]bool, len(txns))
	outcomes := make([]replayOutcome, len(txns))
	gates := make([]chan struct{}, len(txns))
	if ordered {
		for i := range gates {
			gates[i] = make(chan struct{})
		}
	}
	nextCommit, failed, next := 0, len(txns), 0
	var lastSequence int64
	var lastSID string
	seenGTIDs := make(map[string]bool, len(txns))
	grant := func() {
		if ordered && nextCommit < len(txns) && nextCommit < failed {
			close(gates[nextCommit])
		}
	}
	grant()
	accept := func(out replayOutcome) {
		delete(busy, out.index)
		if cancel := cancels[out.index]; cancel != nil {
			cancel()
			delete(cancels, out.index)
		}
		free = append(free, out.worker)
		done[out.index], outcomes[out.index] = true, out
		if out.err != nil && out.index < failed {
			failed = out.index
			if ordered {
				// Earlier transactions must finish: error skipping is allowed
				// only when the entire earlier prefix has a known outcome.
				for index, cancel := range cancels {
					if index > failed {
						cancel()
					}
				}
			}
		}
		previous := nextCommit
		for nextCommit < len(txns) && done[nextCommit] && outcomes[nextCommit].err == nil {
			nextCommit++
		}
		if nextCommit != previous {
			grant()
		}
	}
	drain := func() {
		for len(busy) != 0 {
			accept(<-results)
		}
	}
	for next < len(txns) && failed == len(txns) {
		// Observe completions before dispatching another transaction after a
		// failure. A completion arriving concurrently is drained below.
		select {
		case out := <-results:
			accept(out)
			continue
		default:
		}
		if err := ctx.Err(); err != nil {
			failed = next
			outcomes[next] = replayOutcome{index: next, err: err}
			break
		}
		txn := txns[next]
		sid, _, hasSID := strings.Cut(txn.GetGtid(), ":")
		if !hasSID {
			sid = ""
		}
		sequence, dependency := txn.GetSequenceNumber(), txn.GetLastCommitted()
		barrier := txn.GetDdl() != nil || sequence <= 0 || dependency < 0 || dependency >= sequence || sequence <= lastSequence || seenGTIDs[txn.GetGtid()] || (lastSID != "" && sid != lastSID)
		if barrier {
			drain()
			if failed != len(txns) {
				break
			}
			applied, err := execute(ctx, 0, txn, nil)
			// A drained barrier uses the primary writer without spawning an
			// extra owner. No transaction can commit across this boundary.
			busy[next] = 0
			for i, worker := range free {
				if worker == 0 {
					free = append(free[:i], free[i+1:]...)
					break
				}
			}
			accept(replayOutcome{index: next, worker: 0, applied: applied, err: err})
			lastSequence = 0
			lastSID = sid
			seenGTIDs[txn.GetGtid()] = true
			next++
			continue
		}
		blocked := len(free) == 0
		for index := range busy {
			if txns[index].GetSequenceNumber() <= dependency {
				blocked = true
				break
			}
		}
		if blocked {
			accept(<-results)
			continue
		}
		worker := free[0]
		free = free[1:]
		jobCtx, cancel := context.WithCancel(ctx)
		index := next
		busy[index], cancels[index] = worker, cancel
		var beforeCommit func(context.Context) error
		if ordered {
			beforeCommit = func(commitCtx context.Context) error {
				select {
				case <-gates[index]:
					return commitCtx.Err()
				case <-commitCtx.Done():
					return commitCtx.Err()
				}
			}
		}
		go func() {
			out := replayOutcome{index: index, worker: worker}
			defer func() {
				if recover() != nil {
					out.err = errors.New("mysqlout: replay worker panicked")
				}
				results <- out
			}()
			out.applied, out.err = execute(jobCtx, worker, txn, beforeCommit)
		}()
		lastSequence = sequence
		lastSID = sid
		seenGTIDs[txn.GetGtid()] = true
		next++
	}
	drain()
	for index, out := range outcomes {
		if !done[index] || out.err != nil {
			continue
		}
		if out.applied {
			result.AppliedGTIDs = append(result.AppliedGTIDs, txns[index].GetGtid())
		} else {
			result.RejectedGTIDs = append(result.RejectedGTIDs, txns[index].GetGtid())
		}
	}
	if failed != len(txns) {
		return result, &BatchTxnError{Index: failed, Err: outcomes[failed].err}
	}
	if next != len(txns) {
		return result, fmt.Errorf("mysqlout: replay stopped before all transactions completed")
	}
	return result, nil
}
