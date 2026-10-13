package snapwriter

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
)

type snapshotWorker struct {
	writer *mysqlout.Writer
	jobs   chan delivery
	busy   bool
}

// Only the coordinator reads or writes the pool. A worker owns its Writer
// between job submission and result receipt; updates require all workers idle.
type workerPool struct {
	workers []snapshotWorker
	limit   int
	results chan result
	done    chan struct{}
}

func (m *Module) newWorkerPool(ctx context.Context) *workerPool {
	p := &workerPool{limit: len(m.opts.Writers), results: make(chan result, 128), done: make(chan struct{}, 128)}
	for _, writer := range m.opts.Writers {
		p.add(ctx, m, writer)
	}
	return p
}

func (p *workerPool) add(ctx context.Context, m *Module, writer *mysqlout.Writer) {
	id := len(p.workers)
	jobs := make(chan delivery, 1)
	p.workers = append(p.workers, snapshotWorker{writer: writer, jobs: jobs})
	results, done := p.results, p.done
	go func() {
		defer func() { done <- struct{}{} }()
		for d := range jobs {
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			committed, err := m.applyChunkPolicy(attempt, writer, d.run, d.policy, d.chunk)
			cancel()
			results <- result{workerID: id, delivery: d, committed: committed, err: err}
		}
	}()
}

func (p *workerPool) idleWorker() int {
	for i := 0; i < p.limit; i++ {
		if !p.workers[i].busy {
			return i
		}
	}
	return -1
}

func (p *workerPool) writers() []*mysqlout.Writer {
	writers := make([]*mysqlout.Writer, len(p.workers))
	for i := range p.workers {
		writers[i] = p.workers[i].writer
	}
	return writers
}

func (p *workerPool) stop() []*mysqlout.Writer {
	for i := range p.workers {
		close(p.workers[i].jobs)
	}
	for range p.workers {
		<-p.done
	}
	return p.writers()
}

// Additional writers have independent ownership until the runtime commit.
// Every failure closes them; the old pool and its limit remain authoritative.
func (m *Module) prepareWorkers(ctx context.Context, p *workerPool, limit int) (writers []*mysqlout.Writer, err error) {
	if limit <= len(p.workers) {
		return nil, nil
	}
	if m.opts.NewWriter == nil {
		return nil, errors.New("snapwriter: growing concurrency requires writer factory")
	}
	defer func() {
		if recover() != nil {
			err = errors.New("snapwriter: writer preparation panicked")
		}
		if err != nil {
			for _, writer := range writers {
				_ = writer.Close()
			}
			writers = nil
		}
	}()
	prepare, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for i := len(p.workers); i < limit; i++ {
		if err = prepare.Err(); err != nil {
			return writers, err
		}
		var writer *mysqlout.Writer
		writer, err = m.opts.NewWriter(prepare)
		if writer != nil {
			writers = append(writers, writer)
		}
		if err != nil {
			return writers, err
		}
		if writer == nil {
			return writers, errors.New("snapwriter: writer factory returned nil")
		}
	}
	return writers, prepare.Err()
}
