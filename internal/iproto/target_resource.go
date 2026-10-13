package iproto

import (
	"context"
	"errors"
	"sync"
)

// ITargetRuntimeResource owns prepared or retired target connections. Preparing
// and closing resources happen in the loader's IO worker, outside the actor.
type ITargetRuntimeResource interface {
	Close() error
}

// PreparedTargetResource transfers exclusive ownership at a drained runtime
// boundary. The mutex protects only the ownership decision, not writer state.
// A canceled sender may close an unclaimed candidate or a retired resource;
// it can never close the new resource after the receiver has installed it.
type PreparedTargetResource struct {
	mu        sync.Mutex
	candidate ITargetRuntimeResource
	retired   ITargetRuntimeResource
	claimed   bool
	finished  bool
}

func NewPreparedTargetResource(candidate ITargetRuntimeResource) *PreparedTargetResource {
	return &PreparedTargetResource{candidate: candidate}
}

// Commit calls install exclusively under the receiver's execution ownership.
// install must perform only a small, pure ownership swap, validate before
// mutation, and leave the candidate unchanged on error. It returns the drained
// old resource for the loader to close after the asynchronous response.
func (p *PreparedTargetResource) Commit(ctx context.Context, install func(ITargetRuntimeResource) (ITargetRuntimeResource, error)) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished || p.claimed || p.candidate == nil || ctx == nil || install == nil {
		return errors.New("iproto: target resource transfer is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	retired, err := install(p.candidate)
	if err != nil {
		return err
	}
	p.candidate, p.retired, p.claimed = nil, retired, true
	return nil
}

// Finish is idempotent and also aborts a pending transfer. It takes ownership
// under the mutex and performs potentially blocking close after releasing it.
func (p *PreparedTargetResource) Finish() error {
	p.mu.Lock()
	if p.finished {
		p.mu.Unlock()
		return nil
	}
	p.finished = true
	resource := p.candidate
	if p.claimed {
		resource = p.retired
	}
	p.candidate, p.retired = nil, nil
	p.mu.Unlock()
	if resource != nil {
		return resource.Close()
	}
	return nil
}
