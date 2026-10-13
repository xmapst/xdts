package transport

import (
	"context"
	"errors"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var ErrPipelineProbeBusy = errors.New("transport: pipeline probe already in flight")
var ErrPipelineProbeOccupied = errors.New("transport: pipeline queue has deliveries or consumers")

// pipelineProbeIO keeps its connection slot until actual IO and channel cleanup
// finish. A timed out AMQP RPC cannot spawn an unbounded series of replacements.
func (c *Conn) pipelineProbeIO(ctx context.Context, work func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if c == nil || c.xc == nil {
		return ErrConnManagerNil
	}
	if !c.queueProbeBusy.CompareAndSwap(false, true) {
		return ErrPipelineProbeBusy
	}
	done := make(chan error, 1)
	go func() {
		err := work()
		c.queueProbeBusy.Store(false)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// PreparePipelineProbe declares the same queues and bindings as the production
// loader. Existing queues are never deleted or purged. deliveryLimit is the
// current production projection's zero value, not a caller-supplied override.
func (c *Conn) PreparePipelineProbe(ctx context.Context, pipelineID string, snapshot bool) error {
	return c.preparePipelineProbe(ctx, pipelineID, snapshot, false)
}

func (c *Conn) preparePipelineProbe(ctx context.Context, pipelineID string, snapshot, allowBacklog bool) error {
	id, err := uuidx.Parse(pipelineID)
	if err != nil || id.IsZero() {
		return errors.New("transport: invalid pipeline identity")
	}
	return c.pipelineProbeIO(ctx, func() error {
		dc, err := NewDeclarator(c)
		if err != nil {
			return err
		}
		defer dc.Close()
		if err := dc.DeclareDeadLetterQueue(); err != nil {
			return err
		}
		if err := dc.DeclareExchange(DataExchangeOptions()); err != nil {
			return err
		}
		queues := []struct {
			opts  xamqp.QueueOptions
			route string
		}{{PipelineDataQueueOptions(pipelineID, 0), RoutingKeyPipelineData(pipelineID)}}
		if snapshot {
			queues = append(queues, struct {
				opts  xamqp.QueueOptions
				route string
			}{PipelineSnapshotQueueOptions(pipelineID), RoutingKeyPipelineSnapshot(pipelineID)})
		}
		for _, q := range queues {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := dc.DeclareQueue(q.opts); err != nil {
				return err
			}
			if err := dc.BindQueue(ExchangeData, q.opts.Name, q.route); err != nil {
				return err
			}
			stats, err := c.InspectQueue(ctx, q.opts.Name)
			if err != nil {
				return err
			}
			if !allowBacklog && stats.Messages != 0 || stats.Consumers != 0 {
				return ErrPipelineProbeOccupied
			}
		}
		return nil
	})
}

// ReceivePipelineProbe uses a disposable channel, manual acknowledgements and
// prefetch one. Only the validated message is acknowledged. Any other delivery
// is requeued and ends the check. It does not run an SQL writer or alter state.
func (c *Conn) ReceivePipelineProbe(ctx context.Context, pipelineID string, snapshot bool, validate func(*iproto.Envelope) error) error {
	id, err := uuidx.Parse(pipelineID)
	if err != nil || id.IsZero() || validate == nil {
		return errors.New("transport: invalid pipeline probe")
	}
	queue := PipelineDataQueue(pipelineID)
	if snapshot {
		queue = PipelineSnapshotQueue(pipelineID)
	}
	return c.receivePipelineProbe(ctx, queue, false, validate)
}

func (c *Conn) receivePipelineProbe(ctx context.Context, queue string, deleteQueue bool, validate func(*iproto.Envelope) error) error {
	return c.pipelineProbeIO(ctx, func() error {
		ch, err := xamqp.NewChannel(c.xc)
		if err != nil {
			return err
		}
		defer func(cleanup func() error) { _ = cleanup() }(ch.Close)
		ch.WithChannelForNewGeneration(func(raw *amqp.Channel, _ uint) {
			if raw == nil {
				err = ErrConnManagerNil
				return
			}
			if err = ctx.Err(); err != nil {
				return
			}
			if deleteQueue {
				defer func(cleanup func(string, bool, bool, bool) (int, error), arg0 string, arg1 bool, arg2 bool, arg3 bool) {
					_, _ = cleanup(arg0, arg1, arg2, arg3)
				}(raw.QueueDelete, queue, false, false, false)
			}
			var stats amqp.Queue
			stats, err = raw.QueueDeclarePassive(queue, false, false, false, false, nil)
			if err != nil {
				return
			}
			if stats.Consumers != 0 {
				err = ErrPipelineProbeOccupied
				return
			}
			if err = raw.Qos(1, 0, false); err != nil {
				return
			}
			var deliveries <-chan amqp.Delivery
			deliveries, err = raw.Consume(queue, "xdts.probe."+uuidx.New().String(), false, false, false, false, nil)
			if err != nil {
				return
			}
			// Detect a concurrent consumer instead of assuming we own its queue.
			stats, err = raw.QueueDeclarePassive(queue, false, false, false, false, nil)
			if err != nil {
				return
			}
			if stats.Consumers != 1 {
				err = ErrPipelineProbeOccupied
				return
			}
			select {
			case <-ctx.Done():
				err = ctx.Err()
			case delivery, ok := <-deliveries:
				if !ok {
					err = errors.New("transport: pipeline probe channel closed")
					return
				}
				msg := newMessage(xamqp.Delivery{Delivery: delivery})
				env, parseErr := msg.Envelope()
				if ctx.Err() != nil {
					parseErr = ctx.Err()
				}
				if parseErr == nil {
					parseErr = validate(env)
				}
				if parseErr != nil {
					nackErr := msg.Ack.NackRequeue()
					err = errors.Join(parseErr, nackErr)
					return
				}
				err = msg.Ack.Ack()
			}
		})
		return err
	})
}

// Keep the probe's external lifetime bounded even when a peer is unavailable.
const PipelineProbeTimeout = 30 * time.Second

// The auxiliary queue shares the exact business routing key on ExchangeData.
// Its small expiring copy never consumes or acknowledges business deliveries.
// A valid one-time probe is also appended to the untouched production queue;
// its existing data-plane probe handler discards it without a SQL transaction.
func resumeProbeQueue(probeID string) (string, error) {
	id, err := uuidx.Parse(probeID)
	if err != nil || id.IsZero() {
		return "", errors.New("transport: invalid resume probe identity")
	}
	return "xdts.resume-probe." + probeID, nil
}

func (c *Conn) PrepareResumingPipelineProbe(ctx context.Context, pipelineID string, snapshot bool, probeID string) error {
	queue, err := resumeProbeQueue(probeID)
	if err != nil {
		return err
	}
	if err := c.preparePipelineProbe(ctx, pipelineID, snapshot, true); err != nil {
		return err
	}
	return c.pipelineProbeIO(ctx, func() error {
		dc, err := NewDeclarator(c)
		if err != nil {
			return err
		}
		defer dc.Close()
		if err := ctx.Err(); err != nil {
			return err
		}
		opts := durableQueueOptions(queue, amqp.Table{"x-queue-type": "classic", "x-expires": int32(45000), "x-message-ttl": int32(40000), "x-max-length": int32(1), "x-max-length-bytes": int32(65536), "x-overflow": "drop-head"})
		if err := dc.DeclareQueue(opts); err != nil {
			return err
		}
		route := RoutingKeyPipelineData(pipelineID)
		if snapshot {
			route = RoutingKeyPipelineSnapshot(pipelineID)
		}
		return dc.BindQueue(ExchangeData, queue, route)
	})
}

func (c *Conn) ReceiveResumingPipelineProbe(ctx context.Context, probeID string, validate func(*iproto.Envelope) error) error {
	queue, err := resumeProbeQueue(probeID)
	if err != nil || validate == nil {
		return errors.New("transport: invalid resume route probe")
	}
	return c.receivePipelineProbe(ctx, queue, true, validate)
}
