package resourcequota

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

// Decode charges the received wire bytes before decoding and conservatively
// reserves decoded objects plus filtering/encoding copies before retention.
// The returned buffer transfers with the message until it is applied or freed.
func (b *Budget) Decode(body []byte, encoding string, msg proto.Message) (*Buffer, error) {
	if b == nil {
		return nil, iproto.Decode(body, encoding, msg)
	}
	buffer, err := b.Reserve(uint64(len(body)))
	if err != nil {
		return nil, err
	}
	transferred := false
	defer func() {
		if !transferred {
			buffer.Release()
		}
	}()
	// Snapshot other reservations under the authoritative quota lock. A
	// concurrent reservation may reduce the available amount; Grow checks
	// again atomically before the decoded message can be retained.
	b.c.mu.Lock()
	p := b.c.pipelines[b.id]
	available := min(b.c.limits.PipelineBufferBytes-p.bufferBytes, b.c.limits.MaxBufferBytes-b.c.stats.BufferBytes)
	b.c.mu.Unlock()
	if err := iproto.DecodeLimited(body, encoding, msg, available/4); err != nil {
		if errors.Is(err, iproto.ErrBodyTooLarge) {
			return nil, &Exceeded{PipelineID: b.id, Resource: "decoded_buffer_bytes", Limit: available / 4, Requested: available/4 + 1}
		}
		return nil, err
	}
	if err := buffer.Grow(retainedMessageBytes(msg)); err != nil {
		return nil, err
	}
	transferred = true
	return buffer, nil
}

func retainedMessageBytes(msg proto.Message) uint64 {
	cost := uint64(proto.Size(msg))*4 + 256
	rows := func(row *iproto.Row) uint64 { return 128 + uint64(len(row.GetValues()))*192 }
	switch m := msg.(type) {
	case *iproto.DataBatch:
		for _, txn := range m.GetTxns() {
			cost += 256
			for _, change := range txn.GetRows() {
				cost += 128
				if change.Before != nil {
					cost += rows(change.Before)
				}
				if change.After != nil {
					cost += rows(change.After)
				}
			}
		}
	case *iproto.SnapshotChunk:
		for _, row := range m.GetRows() {
			cost += rows(row)
		}
	}
	return cost
}
