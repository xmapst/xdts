package schemamigrator

import (
	"context"
	"errors"
	"fmt"

	"github.com/xmapst/xhive"
)

type Module struct {
	*xhive.Skeleton
	runner *Runner
	done   chan error
}

func ModuleName(id string) string { return "schema_migrator." + id }
func New(opts Options) (*Module, error) {
	runner, err := NewRunner(opts)
	if err != nil {
		return nil, err
	}
	return &Module{Skeleton: xhive.NewSkeleton(ModuleName(opts.PipelineID)), runner: runner, done: make(chan error, 1)}, nil
}
func (m *Module) Done() <-chan error { return m.done }
func (m *Module) Priority() uint     { return 60 }
func (m *Module) OnInit() error {
	if m.runner == nil {
		return fmt.Errorf("schemamigrator: runner required")
	}
	return nil
}
func (m *Module) Serve(ctx context.Context) {
	work, cancel := context.WithCancel(ctx)
	defer cancel()
	joined := make(chan struct{})
	go func() { defer close(joined); m.done <- m.runner.Run(work) }()
	m.Skeleton.Serve(ctx)
	cancel()
	<-joined
}
func (m *Module) OnDestroy()   {}
func (m *Module) Close() error { return errors.Join(m.runner.opts.Applier.Close(), m.Skeleton.Close()) }
