package transport

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xmapst/xamqp"

	"github.com/xmapst/xdts/internal/iproto"
)

const maxPublisherInflight = 128

// Publisher owns a confirm channel and correlates returns before resolving each
// publication. The return buffer has one slot for every possible in-flight
// publication. RabbitMQ sends return before ack; amqp091-go dispatch enqueues
// that return synchronously before completing the deferred confirmation.
// Callers retain responsibility for serializing transaction fragments.
type Publisher struct {
	channel *xamqp.Channel
	// The public xamqp zero-routing-key publish performs its connection-level
	// blocked check without sending a frame. Keep that shared-connection check.
	flowGuard *xamqp.Publisher

	mu          sync.Mutex
	byMessageID map[string]*pendingPublish
	closed      chan struct{}
	slots       chan struct{}
	workers     sync.WaitGroup
	readers     sync.WaitGroup
	once        sync.Once
	ctx         context.Context
	cancel      context.CancelFunc

	// Serializes listener installation and frame submission, not confirm waits.
	publishMu  sync.Mutex
	generation *publisherGeneration
}

type publisherGeneration struct {
	number  uint
	returns <-chan amqp.Return
	paused  bool // guarded by Publisher.mu
	ready   bool // native recovery is complete; guarded by Publisher.mu
}

func NewPublisher(conn *Conn) (*Publisher, error) {
	if conn == nil || conn.xc == nil {
		return nil, ErrConnManagerNil
	}
	guard, err := xamqp.NewPublisher(conn.xc)
	if err != nil {
		return nil, fmt.Errorf("transport: 创建发布者失败: %w", err)
	}
	ch, err := xamqp.NewChannel(conn.xc, xamqp.WithChannelOptionsConfirm)
	if err != nil {
		guard.Close()
		return nil, fmt.Errorf("transport: 创建确认通道失败: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Publisher{channel: ch, flowGuard: guard, byMessageID: make(map[string]*pendingPublish), closed: make(chan struct{}), slots: make(chan struct{}, maxPublisherInflight), ctx: ctx, cancel: cancel}, nil
}

// PublishEnvelope always sends mandatory, persistent messages. Cancellation
// bounds the caller's wait, including when native socket writes do not honor
// context. An abandoned publication retains its slot and message_id until its
// confirmation or channel closure: releasing it sooner could overflow the
// return listener and misclassify a dropped notification as success.
func (pub *Publisher) PublishEnvelope(ctx context.Context, exchange, routingKey string, env *iproto.Envelope) (Outcome, error) {
	if env == nil || env.Properties.MessageID == "" {
		return OutcomeUnknown, ErrMissingMessageID
	}
	if err := iproto.ValidateHeaders(env.Headers); err != nil {
		return OutcomeUnknown, fmt.Errorf("transport: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return OutcomeUnknown, err
	}
	select {
	case pub.slots <- struct{}{}:
	case <-ctx.Done():
		return OutcomeUnknown, ctx.Err()
	case <-pub.closed:
		return OutcomeUnknown, ErrPublisherClosed
	}
	id := env.Properties.MessageID
	pub.mu.Lock()
	select {
	case <-pub.closed:
		pub.mu.Unlock()
		<-pub.slots
		return OutcomeUnknown, ErrPublisherClosed
	default:
	}
	if _, exists := pub.byMessageID[id]; exists {
		pub.mu.Unlock()
		<-pub.slots
		return OutcomeUnknown, ErrMessageIDInFlight
	}
	p := &pendingPublish{}
	pub.byMessageID[id] = p
	pub.workers.Add(1)
	pub.mu.Unlock()
	// Headers are restricted to scalars and []byte by ValidateHeaders.
	message := envelopePublishing(env)
	done := make(chan publishResult, 1)
	go pub.publish(ctx, exchange, routingKey, message, p, done)
	select {
	case result := <-done:
		return result.outcome, result.err
	case <-ctx.Done():
		return OutcomeUnknown, ctx.Err()
	case <-pub.closed:
		return OutcomeUnknown, ErrPublisherClosed
	}
}

type publishResult struct {
	outcome Outcome
	err     error
}

func (pub *Publisher) publish(ctx context.Context, exchange, routingKey string, message amqp.Publishing, p *pendingPublish, done chan<- publishResult) {
	result := publishResult{outcome: OutcomeUnknown}
	var generation *publisherGeneration
	defer pub.workers.Done()
	defer func() {
		pub.mu.Lock()
		// A failed send or native reset can leave already received returns.
		// Drain them before releasing admission or allowing identity reuse.
		if generation != nil {
			pub.drainReturns(generation)
		}
		delete(pub.byMessageID, message.MessageId)
		pub.mu.Unlock()
		<-pub.slots
		done <- result
	}()
	if err := pub.flowGuard.PublishWithContext(ctx, nil, nil); err != nil {
		result.err = err
		return
	}
	pub.publishMu.Lock()
	var confirmation *amqp.DeferredConfirmation
	pub.channel.WithChannelForNewGeneration(func(raw *amqp.Channel, number uint) {
		if pub.generation == nil || pub.generation.number != number {
			generation = &publisherGeneration{number: number, returns: raw.NotifyReturn(make(chan amqp.Return, maxPublisherInflight)), ready: !raw.IsClosed()}
			pub.generation = generation
			flow := raw.NotifyFlow(make(chan bool, 1))
			states := make(chan *amqp.StateChanged, 8)
			raw.NotifyStateChange(states)
			pub.readers.Add(2)
			go pub.readFlow(generation, flow)
			go pub.readStates(generation, states)
		}
		generation = pub.generation
		pub.mu.Lock()
		paused := generation.paused
		if raw.IsClosed() {
			generation.ready = false
		}
		ready := generation.ready
		pub.mu.Unlock()
		if !ready {
			result.err = amqp.ErrClosed
			return
		}
		if paused {
			result.err = xamqp.ErrPublishFlowPaused
			return
		}
		select {
		case <-pub.closed:
			result.err = ErrPublisherClosed
			return
		default:
		}
		confirmation, result.err = raw.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, message)
	})
	pub.publishMu.Unlock()
	if result.err != nil {
		return
	}
	if confirmation == nil {
		result.err = ErrUnexpectedConfirmationCount
		return
	}
	// Caller timeout must not release the return buffer reservation. Only
	// confirmation completion or publisher shutdown releases this worker.
	ack, err := confirmation.WaitContext(pub.ctx)
	if err != nil {
		result.err = ErrPublisherClosed
		return
	}
	pub.mu.Lock()
	pub.drainReturns(generation)
	result.outcome = p.outcome(ack)
	pub.mu.Unlock()
}

// drainReturns is called with mu held, after the deferred confirmation. No
// asynchronous callback sits between the raw return queue and settlement.
func (pub *Publisher) drainReturns(generation *publisherGeneration) {
	for {
		select {
		case r, ok := <-generation.returns:
			if !ok {
				return
			}
			if p := pub.byMessageID[r.MessageId]; p != nil {
				p.returned = true
			}
			slog.Warn("message returned by broker", slog.String("message_id", r.MessageId), slog.String("exchange", r.Exchange), slog.String("routing_key", r.RoutingKey), slog.Uint64("reply_code", uint64(r.ReplyCode)), slog.String("reply_text", r.ReplyText))
		default:
			return
		}
	}
}

func (pub *Publisher) readFlow(generation *publisherGeneration, flow <-chan bool) {
	defer pub.readers.Done()
	for active := range flow {
		pub.mu.Lock()
		generation.paused = !active
		pub.mu.Unlock()
	}
}

// IsClosed becomes false before the native channel.open/confirm handshakes
// finish. Keep publications stopped until this channel reports StateOpen;
// another channel's recovery event cannot authorize its publication session.
func (pub *Publisher) readStates(generation *publisherGeneration, states <-chan *amqp.StateChanged) {
	defer pub.readers.Done()
	for state := range states {
		if state != nil {
			pub.mu.Lock()
			generation.ready = state.To == amqp.StateOpen
			pub.mu.Unlock()
		}
	}
}

func (pub *Publisher) Close() {
	pub.once.Do(func() {
		pub.mu.Lock()
		close(pub.closed)
		pub.cancel()
		pub.mu.Unlock()
		_ = pub.channel.Close()
		pub.workers.Wait()
		pub.readers.Wait()
		pub.flowGuard.Close()
	})
}

// envelopePublishing copies the complete wire payload before crossing into a
// worker. It does not re-encode the envelope or alter its signed properties.
func envelopePublishing(env *iproto.Envelope) amqp.Publishing {
	headers := HeadersFromMap(env.Headers)
	for key, value := range headers {
		if b, ok := value.([]byte); ok {
			headers[key] = bytes.Clone(b)
		}
	}
	p := env.Properties
	message := amqp.Publishing{Headers: headers, Body: bytes.Clone(env.Body), ContentType: p.ContentType, ContentEncoding: p.ContentEncoding, DeliveryMode: amqp.Persistent, CorrelationId: p.CorrelationID, MessageId: p.MessageID, Type: p.Type}
	if p.TimestampMS > 0 {
		message.Timestamp = time.UnixMilli(p.TimestampMS)
	}
	if p.ExpirationMS > 0 {
		message.Expiration = strconv.FormatInt(p.ExpirationMS, 10)
	}
	return message
}
