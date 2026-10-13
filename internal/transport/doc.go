// Package transport adapts protocol envelopes to AMQP.
// It declares topology, publishes with confirmation and mandatory return tracking,
// and consumes with explicit acknowledgement. UInt64 headers use decimal strings.
//
// Business handlers own verification, panic reporting and ACK policy. Data-plane
// failures retain unconfirmed messages; control-plane consumers may acknowledge
// invalid messages after reporting errors. RPC replies use per-instance queues and
// correlation IDs. Callers supply operation deadlines and own shared connections.
package transport
