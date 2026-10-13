package transport

import (
	"context"
	"errors"
	"strings"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"
)

var ErrQueueInspectBusy = errors.New("transport: queue inspection already in flight")

// QueueStats is the passive-declare result. Messages counts ready deliveries;
// AMQP does not expose unacknowledged deliveries in this response.
type QueueStats struct {
	Messages  uint64
	Consumers uint32
}

// InspectQueue uses a disposable channel so a missing queue cannot close a
// publisher or consumer channel. The caller's context bounds its wait. AMQP's
// declare RPC has no cancellation API: after a timeout, the connection keeps
// its single inspection slot until the RPC and channel cleanup have finished.
func (c *Conn) InspectQueue(ctx context.Context, name string) (QueueStats, error) {
	if err := ctx.Err(); err != nil {
		return QueueStats{}, err
	}
	if c == nil || c.xc == nil {
		return QueueStats{}, ErrConnManagerNil
	}
	if strings.TrimSpace(name) == "" {
		return QueueStats{}, errors.New("transport: queue name is empty")
	}
	if !c.queueInspectBusy.CompareAndSwap(false, true) {
		return QueueStats{}, ErrQueueInspectBusy
	}
	type result struct {
		stats QueueStats
		err   error
	}
	done := make(chan result, 1)
	go func() {
		r := func() result {
			ch, err := xamqp.NewChannel(c.xc)
			if err != nil {
				return result{err: err}
			}
			defer func(cleanup func() error) { _ = cleanup() }(ch.Close)
			var observed result
			// A missing queue is an expected observation. Do not replay its
			// passive declaration during automatic channel recovery.
			ch.WithChannelForNewGeneration(func(raw *amqp.Channel, _ uint) {
				q, err := raw.QueueDeclarePassive(name, false, false, false, false, nil)
				observed = result{stats: QueueStats{Messages: uint64(q.Messages), Consumers: uint32(q.Consumers)}, err: err}
			})
			return observed
		}()
		c.queueInspectBusy.Store(false)
		done <- r
	}()
	select {
	case r := <-done:
		return r.stats, r.err
	case <-ctx.Done():
		return QueueStats{}, ctx.Err()
	}
}
