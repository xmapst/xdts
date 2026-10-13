package lifecycle

import (
	"context"
	"errors"
	"slices"
)

// Scope owns startup resources. Use it from one lifecycle goroutine.
type Scope struct {
	cleanup []func() error
}

func (s *Scope) Defer(cleanup func() error) {
	s.cleanup = append(s.cleanup, cleanup)
}

// Go joins the worker before resources registered earlier are released.
func (s *Scope) Go(ctx context.Context, run func(context.Context)) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	s.Defer(func() error {
		cancel()
		<-done
		return nil
	})
	go func() {
		defer close(done)
		run(ctx)
	}()
}

func (s *Scope) Close() error {
	cleanup := s.cleanup
	s.cleanup = nil
	var err error
	for _, c := range slices.Backward(cleanup) {
		err = errors.Join(err, c())
	}
	return err
}
