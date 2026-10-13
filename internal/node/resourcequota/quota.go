// Package resourcequota owns the Node's shared admission and retained business
// buffer accounting. It does not read or mutate any pipeline module's state.
package resourcequota

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrExceeded        = errors.New("node resource quota exceeded")
	ErrAlreadyAdmitted = errors.New("pipeline role already admitted")
	ErrReleased        = errors.New("resource lease already released")
)

// Limits bounds live pipelines, retained buffers and simultaneous snapshot SQL
// work. Only MaxPipelines accepts zero (unlimited). Buffers are conservatively
// charged by their owners; this accounting is not a process RSS limit.
type Limits struct {
	MaxPipelines        uint64 `mapstructure:"max_pipelines" json:"max_pipelines"`
	MaxBufferBytes      uint64 `mapstructure:"max_buffer_bytes" json:"max_buffer_bytes"`
	PipelineBufferBytes uint64 `mapstructure:"pipeline_buffer_bytes" json:"pipeline_buffer_bytes"`
	SnapshotConcurrency uint64 `mapstructure:"snapshot_concurrency" json:"snapshot_concurrency"`
}

func DefaultLimits() Limits {
	return Limits{MaxPipelines: 100, MaxBufferBytes: 1 << 30, PipelineBufferBytes: 128 << 20, SnapshotConcurrency: 8}
}

func (l Limits) Validate() error {
	var errs []error
	if l.MaxPipelines > 10000 {
		errs = append(errs, errors.New("resources.max_pipelines must be between 0 and 10000"))
	}
	if l.MaxBufferBytes == 0 || l.MaxBufferBytes > 1<<40 {
		errs = append(errs, errors.New("resources.max_buffer_bytes must be between 1 and 1099511627776"))
	}
	if l.PipelineBufferBytes == 0 || l.PipelineBufferBytes > l.MaxBufferBytes {
		errs = append(errs, errors.New("resources.pipeline_buffer_bytes must be positive and at most resources.max_buffer_bytes"))
	}
	if l.SnapshotConcurrency == 0 || l.SnapshotConcurrency > 1024 {
		errs = append(errs, errors.New("resources.snapshot_concurrency must be between 1 and 1024"))
	}
	return errors.Join(errs...)
}

// Exceeded identifies which authoritative limit refused a reservation.
type Exceeded struct {
	PipelineID string
	Resource   string
	Limit      uint64
	Used       uint64
	Requested  uint64
}

func (e *Exceeded) Error() string {
	return fmt.Sprintf("%s: pipeline=%q resource=%s limit=%d used=%d requested=%d", ErrExceeded, e.PipelineID, e.Resource, e.Limit, e.Used, e.Requested)
}

func (e *Exceeded) Unwrap() error { return ErrExceeded }

type pipeline struct {
	roles          map[string]*Admission
	bufferBytes    uint64
	buffers        uint64
	snapshotWorker uint64
}

type Stats struct {
	Pipelines       uint64
	BufferBytes     uint64
	SnapshotWorkers uint64
}

// Coordinator is shared across worker goroutines. Its mutex exclusively owns
// all counters, leases and admission decisions; waits happen outside the mutex.
type Coordinator struct {
	mu        sync.Mutex
	limits    Limits
	pipelines map[string]*pipeline
	stats     Stats
	slots     chan struct{}
}

func New(limits Limits) (*Coordinator, error) {
	if err := limits.Validate(); err != nil {
		return nil, err
	}
	return &Coordinator{limits: limits, pipelines: make(map[string]*pipeline), slots: make(chan struct{}, int(limits.SnapshotConcurrency))}, nil
}

func (c *Coordinator) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

func (c *Coordinator) ensurePipeline(id string) (*pipeline, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New("resource quota: pipeline identity required")
	}
	if p := c.pipelines[id]; p != nil {
		return p, nil
	}
	if c.limits.MaxPipelines > 0 && c.stats.Pipelines >= c.limits.MaxPipelines {
		return nil, &Exceeded{PipelineID: id, Resource: "pipelines", Limit: c.limits.MaxPipelines, Used: c.stats.Pipelines, Requested: 1}
	}
	p := &pipeline{roles: make(map[string]*Admission)}
	c.pipelines[id] = p
	c.stats.Pipelines++
	return p, nil
}

func (c *Coordinator) removeUnused(id string, p *pipeline) {
	if len(p.roles) == 0 && p.buffers == 0 && p.snapshotWorker == 0 {
		delete(c.pipelines, id)
		c.stats.Pipelines--
	}
}

type Admission struct {
	c        *Coordinator
	id, role string
	released bool
}

func (c *Coordinator) Admit(id, role string) (*Admission, error) {
	if role != "source" && role != "target" {
		return nil, errors.New("resource quota: source or target role required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, err := c.ensurePipeline(id)
	if err != nil {
		return nil, err
	}
	if p.roles[role] != nil {
		return nil, fmt.Errorf("%w: pipeline=%q role=%s", ErrAlreadyAdmitted, id, role)
	}
	a := &Admission{c: c, id: id, role: role}
	p.roles[role] = a
	return a, nil
}

func (a *Admission) PipelineID() string { return a.id }

func (a *Admission) Release() {
	if a == nil {
		return
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	if a.released {
		return
	}
	a.released = true
	p := a.c.pipelines[a.id]
	delete(p.roles, a.role)
	a.c.removeUnused(a.id, p)
}

// Budget is an immutable handle for one pipeline. A nil handle is supported for
// independently constructed modules; the production Loader always supplies one.
type Budget struct {
	c  *Coordinator
	id string
}

func (c *Coordinator) Budget(id string) *Budget {
	if c == nil {
		return nil
	}
	return &Budget{c: c, id: id}
}

type Buffer struct {
	budget   *Budget
	bytes    uint64
	released bool
}

func (b *Budget) Reserve(bytes uint64) (*Buffer, error) {
	if b == nil {
		return nil, nil
	}
	b.c.mu.Lock()
	defer b.c.mu.Unlock()
	p, err := b.c.ensurePipeline(b.id)
	if err != nil {
		return nil, err
	}
	if err := b.checkGrowth(p, bytes); err != nil {
		b.c.removeUnused(b.id, p)
		return nil, err
	}
	p.buffers++
	p.bufferBytes += bytes
	b.c.stats.BufferBytes += bytes
	return &Buffer{budget: b, bytes: bytes}, nil
}

func (b *Budget) checkGrowth(p *pipeline, bytes uint64) error {
	if bytes > b.c.limits.PipelineBufferBytes-p.bufferBytes {
		return &Exceeded{PipelineID: b.id, Resource: "pipeline_buffer_bytes", Limit: b.c.limits.PipelineBufferBytes, Used: p.bufferBytes, Requested: bytes}
	}
	if bytes > b.c.limits.MaxBufferBytes-b.c.stats.BufferBytes {
		return &Exceeded{PipelineID: b.id, Resource: "max_buffer_bytes", Limit: b.c.limits.MaxBufferBytes, Used: b.c.stats.BufferBytes, Requested: bytes}
	}
	return nil
}

func (l *Buffer) Grow(bytes uint64) error {
	if l == nil {
		return nil
	}
	b := l.budget
	b.c.mu.Lock()
	defer b.c.mu.Unlock()
	if l.released {
		return ErrReleased
	}
	p := b.c.pipelines[b.id]
	if err := b.checkGrowth(p, bytes); err != nil {
		return err
	}
	l.bytes += bytes
	p.bufferBytes += bytes
	b.c.stats.BufferBytes += bytes
	return nil
}

func (l *Buffer) Release() {
	if l == nil {
		return
	}
	b := l.budget
	b.c.mu.Lock()
	defer b.c.mu.Unlock()
	if l.released {
		return
	}
	l.released = true
	p := b.c.pipelines[b.id]
	p.bufferBytes -= l.bytes
	p.buffers--
	b.c.stats.BufferBytes -= l.bytes
	b.c.removeUnused(b.id, p)
}

// AcquireSnapshot waits before a worker opens a SQL session. Cancellation and
// repeated release cannot leak or overdraw the Node's global worker capacity.
func (b *Budget) AcquireSnapshot(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b == nil {
		return func() {}, nil
	}
	select {
	case b.c.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	b.c.mu.Lock()
	p, err := b.c.ensurePipeline(b.id)
	if err == nil {
		p.snapshotWorker++
		b.c.stats.SnapshotWorkers++
	}
	b.c.mu.Unlock()
	if err != nil {
		<-b.c.slots
		return nil, err
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			b.c.mu.Lock()
			p.snapshotWorker--
			b.c.stats.SnapshotWorkers--
			b.c.removeUnused(b.id, p)
			b.c.mu.Unlock()
			<-b.c.slots
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
