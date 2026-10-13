package transport

// Outcome is the final result of a single publication. Numeric values are part
// of the existing transport contract and retain their original meanings.
type Outcome int

const (
	OutcomeUnknown Outcome = iota
	// OutcomeConfirmed means ack=true and no matching mandatory return. The
	// raw return queue is drained before this result is computed.
	OutcomeConfirmed
	// OutcomeNacked includes broker nack and an unresolved deferred confirmation
	// closed during native recovery. Both require retry; a bare bool cannot
	// reliably distinguish their causes.
	OutcomeNacked
	OutcomeReturned
)

func (o Outcome) String() string {
	switch o {
	case OutcomeConfirmed:
		return "confirmed"
	case OutcomeNacked:
		return "nacked"
	case OutcomeReturned:
		return "returned"
	default:
		return "unknown"
	}
}

// pendingPublish belongs to Publisher.mu, including returns drained by another
// concurrently completing publication. It survives caller cancellation until
// the actual deferred confirmation has completed.
type pendingPublish struct{ returned bool }

func (p *pendingPublish) outcome(ack bool) Outcome {
	if p.returned {
		return OutcomeReturned
	}
	if !ack {
		return OutcomeNacked
	}
	return OutcomeConfirmed
}
