// Package amqpout publishes incremental data, watermarks and snapshot messages.
// Publication requires broker confirmation and mandatory routing checks. Retry
// budgets are supplied by the caller through a context deadline.
//
// Complete source transactions keep their GTIDs; large transactions are fragmented
// in order. Concurrent snapshot publication uses independently owned messages and
// buffers. Connection and publisher handles are shared process dependencies.
package amqpout
