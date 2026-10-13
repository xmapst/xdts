package binlogin

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"google.golang.org/protobuf/proto"
)

type publication struct {
	id        uint64
	batch     *iproto.DataBatch
	buffer    *resourcequota.Buffer
	bytes     uint64
	cancel    context.CancelFunc
	completed bool
	err       error
}

type publicationOutcome struct {
	id  uint64
	err error
}

// The source session exclusively owns the window and its retirement decisions.
// Publishers receive immutable, independently cloned batches and return values.
// Retained batches survive a connection restart in memory; their original
// epoch/sequence must be resent before reading any new source transactions.
type publicationWindow struct {
	maxBatches int
	maxBytes   uint64
	timeout    time.Duration
	budget     *resourcequota.Budget
	send       func(context.Context, *iproto.DataBatch) error
	confirmed  func(*iproto.DataBatch) error
	pending    []*publication
	results    chan publicationOutcome
	nextID     uint64
	bytes      uint64
	running    int
}

func newPublicationWindow(batches int, bytes uint64, timeout time.Duration, budget *resourcequota.Budget, send func(context.Context, *iproto.DataBatch) error, confirmed func(*iproto.DataBatch) error) (*publicationWindow, error) {
	if batches < 1 || batches > 128 || bytes == 0 || timeout <= 0 || send == nil || confirmed == nil {
		return nil, errors.New("binlogin: invalid publication window")
	}
	return &publicationWindow{maxBatches: batches, maxBytes: bytes, timeout: timeout, budget: budget, send: send, confirmed: confirmed, results: make(chan publicationOutcome, batches)}, nil
}

func (w *publicationWindow) enqueue(ctx context.Context, batch *iproto.DataBatch) error {
	return w.admit(ctx, batch, nil)
}

// Assign a source sequence only after capacity and memory admission succeeds.
// An interrupted capacity wait must not leave a sequence that was never sent.
func (w *publicationWindow) admit(ctx context.Context, batch *iproto.DataBatch, assign func(*iproto.DataBatch) error) error {
	if batch == nil {
		return errors.New("binlogin: nil publication batch")
	}
	cost := uint64(proto.Size(batch))*4 + 256
	if assign != nil {
		// Bound all scalar generation/policy fields assigned after admission.
		cost += 256
	}
	if cost > w.maxBytes {
		return errors.New("binlogin: batch exceeds publication window byte limit")
	}
	if err := w.poll(ctx); err != nil {
		return err
	}
	for len(w.pending) == w.maxBatches || cost > w.maxBytes-w.bytes {
		if err := w.wait(ctx); err != nil {
			return err
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	buffer, err := w.budget.Reserve(cost)
	if err != nil {
		return err
	}
	owned := iproto.CloneDataBatch(batch)
	if assign != nil {
		if err := assign(owned); err != nil {
			buffer.Release()
			return err
		}
	}
	w.nextID++
	entry := &publication{id: w.nextID, batch: owned, buffer: buffer, bytes: cost}
	w.pending = append(w.pending, entry)
	w.bytes += cost
	sendCtx, cancel := context.WithTimeout(ctx, w.timeout)
	entry.cancel = cancel
	w.running++
	// Capture only immutable data. The worker never reads or writes the entry.
	id, ownedBatch, send := entry.id, entry.batch, w.send
	results := w.results
	go func() {
		results <- publicationOutcome{id: id, err: sendPublicationSafely(sendCtx, send, ownedBatch)}
	}()
	return nil
}

func sendPublicationSafely(ctx context.Context, send func(context.Context, *iproto.DataBatch) error, batch *iproto.DataBatch) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.New("binlogin: publication worker panicked")
		}
	}()
	return send(ctx, batch)
}

func (w *publicationWindow) accept(out publicationOutcome) {
	for _, entry := range w.pending {
		if entry.id == out.id && !entry.completed {
			entry.completed, entry.err = true, out.err
			entry.cancel()
			entry.cancel = nil
			w.running--
			return
		}
	}
}

func (w *publicationWindow) retire() error {
	for len(w.pending) != 0 {
		entry := w.pending[0]
		if !entry.completed || entry.err != nil {
			break
		}
		if err := w.confirmed(entry.batch); err != nil {
			return err
		}
		entry.buffer.Release()
		w.bytes -= entry.bytes
		w.pending[0] = nil
		w.pending = w.pending[1:]
	}
	return nil
}

func (w *publicationWindow) failed() bool {
	for _, entry := range w.pending {
		if entry.completed && entry.err != nil {
			return true
		}
	}
	return false
}

func (w *publicationWindow) poll(ctx context.Context) error {
	for w.running != 0 {
		select {
		case out := <-w.results:
			w.accept(out)
		default:
			return w.settle(ctx)
		}
	}
	return w.settle(ctx)
}

func (w *publicationWindow) settle(ctx context.Context) error {
	if err := w.retire(); err != nil {
		return err
	}
	if w.failed() {
		return w.resume(ctx)
	}
	return ctx.Err()
}

func (w *publicationWindow) wait(ctx context.Context) error {
	if w.running == 0 {
		return w.settle(ctx)
	}
	select {
	case out := <-w.results:
		w.accept(out)
		return w.settle(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Pause always joins all workers before returning their batch ownership. Only
// the contiguous successful prefix retires; later confirms stay retained.
func (w *publicationWindow) pause() error {
	for _, entry := range w.pending {
		if entry.cancel != nil {
			entry.cancel()
		}
	}
	for w.running != 0 {
		w.accept(<-w.results)
	}
	return w.retire()
}

// After a failure or connection restart, resend every remaining original batch
// sequentially, including later batches whose earlier attempt was confirmed.
func (w *publicationWindow) resume(ctx context.Context) error {
	if err := w.pause(); err != nil {
		return err
	}
	for len(w.pending) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry := w.pending[0]
		sendCtx, cancel := context.WithTimeout(ctx, w.timeout)
		err := sendPublicationSafely(sendCtx, w.send, entry.batch)
		cancel()
		entry.completed, entry.err = true, err
		if err != nil {
			return err
		}
		// Retire exactly the resent batch: later previous confirms must not
		// bypass the required sequential resend.
		if err := w.confirmed(entry.batch); err != nil {
			return err
		}
		entry.buffer.Release()
		w.bytes -= entry.bytes
		w.pending[0] = nil
		w.pending = w.pending[1:]
	}
	return nil
}

func (w *publicationWindow) drain(ctx context.Context) error {
	for len(w.pending) != 0 {
		if err := w.wait(ctx); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func (w *publicationWindow) close() error {
	err := w.pause()
	for _, entry := range w.pending {
		entry.buffer.Release()
	}
	w.pending, w.bytes = nil, 0
	return err
}
