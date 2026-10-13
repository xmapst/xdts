package transport

import (
	"context"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"
)

// RecoveryWatcher observes the shared connection through a dedicated channel.
// Its bounded signal merges recoveries until the owner consumes them. Native
// channel recovery and full xamqp channel replacement are both observed.
type RecoveryWatcher struct {
	channel *xamqp.Channel
	events  chan struct{}
	cancel  context.CancelFunc
	done    chan struct{}
	readers sync.WaitGroup
	once    sync.Once
}

func (c *Conn) WatchRecovery() (*RecoveryWatcher, error) {
	if c == nil || c.xc == nil {
		return nil, ErrConnManagerNil
	}
	ch, err := xamqp.NewChannel(c.xc)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := &RecoveryWatcher{channel: ch, events: make(chan struct{}, 1), cancel: cancel, done: make(chan struct{})}
	var generation uint
	w.observeGeneration(false, &generation)
	go w.run(ctx, generation)
	return w, nil
}

func (w *RecoveryWatcher) Events() <-chan struct{} { return w.events }

func (w *RecoveryWatcher) notify() {
	select {
	case w.events <- struct{}{}:
	default:
	}
}

func (w *RecoveryWatcher) observeGeneration(changed bool, generation *uint) {
	w.channel.WithChannelForNewGeneration(func(raw *amqp.Channel, current uint) {
		if changed && current == *generation {
			return
		}
		*generation = current
		states := make(chan *amqp.StateChanged, 8)
		raw.NotifyStateChange(states)
		w.readers.Go(func() {
			// Continue draining through StateClosed, including during shutdown;
			// leaving a registered native listener unread leaks its delivery task.
			for state := range states {
				if state != nil && state.From == amqp.StateReconnecting && state.To == amqp.StateOpen {
					w.notify()
				}
			}
		})
		if changed {
			w.notify()
		}
	})
}

func (w *RecoveryWatcher) run(ctx context.Context, generation uint) {
	defer close(w.done)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.observeGeneration(true, &generation)
		}
	}
}

// Close stops registration before closing the owned channel and joining all
// listener readers. The shared Conn remains owned by its caller.
func (w *RecoveryWatcher) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		w.cancel()
		<-w.done
		_ = w.channel.Close()
		w.readers.Wait()
		close(w.events)
	})
}
