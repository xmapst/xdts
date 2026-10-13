package amqpin

import (
	"context"
	"errors"
	"fmt"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

// The input coordinator owns generations, buffers and durable cursors. Actor
// handlers only install configuration while execution ownership is released.
type publicationKey struct{ epoch, first uint64 }
type publicationCursor struct {
	PipelineID      string `bson:"pipeline_id"`
	RunID           uint64 `bson:"run_id"`
	Epoch           uint64 `bson:"epoch"`
	First           uint64 `bson:"first_seq"`
	Last            uint64 `bson:"last_seq"`
	GTIDs           string `bson:"gtids"`
	WindowBatches   uint32 `bson:"window_batches"`
	WindowBytes     uint64 `bson:"window_bytes"`
	WindowTimeoutMS int64  `bson:"window_timeout_ms"`
}
type publicationGeneration struct {
	key      publicationKey
	limits   iproto.PublicationWindowLimits
	cursor   publicationCursor
	proof    gmysql.GTIDSet
	next     uint64
	future   map[uint64]*publicationEntry
	bytes    uint64
	deadline time.Time
}
type publicationEntry struct {
	batch   *iproto.DataBatch
	handles []iAckHandle
	buffer  *resourcequota.Buffer
	bytes   uint64
}
type publicationAck struct {
	generation *publicationGeneration
	sequence   uint64
	gtids      []string
	handles    []iAckHandle
}

func (h *publicationAck) MessageID() string { return h.handles[0].MessageID() }
func (h *publicationAck) Ack() error {
	var errs []error
	for _, handle := range h.handles {
		if err := handle.Ack(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (m *Module) publicationRecordID(key publicationKey) string {
	return fmt.Sprintf("%d:%s:%d:%d:%d", len(m.opts.PipelineID), m.opts.PipelineID, m.opts.RunID, key.epoch, key.first)
}
func (m *Module) publicationContext() (context.Context, context.CancelFunc) {
	base := m.shutdownCtx
	if base == nil {
		base = context.Background()
	}
	return context.WithTimeout(base, m.opts.ApplyAttemptTimeout)
}
func (m *Module) publicationGeneration(batch *iproto.DataBatch) (*publicationGeneration, error) {
	key := publicationKey{batch.GetEpoch(), batch.GetPublicationWindowFirstSeq()}
	limits, err := iproto.NormalizePublicationWindow(batch.GetPublicationWindowPolicy())
	if err != nil {
		return nil, err
	}
	if _, ok := m.opts.Store.(iCriticalAsyncStore); !ok || limits.Batches > m.opts.MaxReassemblyChunks {
		return nil, errors.New("amqpin: publication generation requires durable store and sufficient prefetch")
	}
	if generation := m.publicationWindows[key]; generation != nil {
		if generation.limits != limits {
			return nil, errors.New("amqpin: publication generation policy changed")
		}
		return generation, nil
	}
	if m.publicationWindows == nil {
		m.publicationWindows = make(map[publicationKey]*publicationGeneration)
	}
	if len(m.publicationWindows) >= 128 {
		for oldKey, old := range m.publicationWindows {
			if len(old.future) == 0 && old.cursor.Last == old.next-1 {
				delete(m.publicationWindows, oldKey)
				break
			}
		}
		if len(m.publicationWindows) >= 128 {
			return nil, errors.New("amqpin: too many unfinished publication generations")
		}
	}
	cursor := publicationCursor{PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, Epoch: key.epoch, First: key.first, Last: key.first - 1, WindowBatches: uint32(limits.Batches), WindowBytes: limits.Bytes, WindowTimeoutMS: limits.Timeout.Milliseconds()}
	ctx, cancel := m.publicationContext()
	defer cancel()
	err = m.opts.Store.Get(ctx, "publication_window_cursors", m.publicationRecordID(key), &cursor)
	if err != nil && !errors.Is(err, xdoc.ErrNotFound) {
		return nil, errors.New("amqpin: publication cursor unavailable")
	}
	if cursor.PipelineID != m.opts.PipelineID || cursor.RunID != m.opts.RunID || cursor.Epoch != key.epoch || cursor.First != key.first || cursor.Last < key.first-1 || cursor.Last >= 1<<63 || cursor.WindowBatches != uint32(limits.Batches) || cursor.WindowBytes != limits.Bytes || cursor.WindowTimeoutMS != limits.Timeout.Milliseconds() {
		return nil, errors.New("amqpin: invalid publication cursor identity")
	}
	proof, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, cursor.GTIDs)
	if err != nil || (cursor.Last >= key.first && cursor.GTIDs == "") {
		return nil, errors.New("amqpin: invalid publication cursor proof")
	}
	generation := &publicationGeneration{key: key, limits: limits, cursor: cursor, proof: proof, next: cursor.Last + 1, future: make(map[uint64]*publicationEntry)}
	m.publicationWindows[key] = generation
	return generation, nil
}
func batchGTIDs(batch *iproto.DataBatch) []string {
	gtids := make([]string, 0, len(batch.GetTxns()))
	for _, txn := range batch.GetTxns() {
		gtids = append(gtids, txn.GetGtid())
	}
	return gtids
}
func publicationGTIDSet(gtids []string) (gmysql.GTIDSet, error) {
	set := emptyGTIDSet()
	for _, gtid := range gtids {
		if err := set.Update(gtid); err != nil {
			return nil, err
		}
	}
	return set, nil
}
func (m *Module) acceptPublicationBatch(batch *iproto.DataBatch, handle iAckHandle, buffer *resourcequota.Buffer) bool {
	if m.reassembly != nil {
		m.freeze("发布窗口消息到达时事务分片仍未完整")
		return false
	}
	generation, err := m.publicationGeneration(batch)
	if err != nil {
		m.freeze(err.Error())
		return false
	}
	sequence := batch.GetBatchSeq()
	if sequence < generation.next {
		m.flushReplay()
		if m.isFrozen() {
			return false
		}
		set, err := publicationGTIDSet(batchGTIDs(batch))
		if err != nil || sequence > generation.cursor.Last || !generation.proof.Contain(set) {
			m.freeze("发布窗口重复序号缺少已处理事务证明")
			return false
		}
		m.ackDuplicate(handle)
		return false
	}
	if old := generation.future[sequence]; old != nil {
		if !proto.Equal(old.batch, batch) || len(old.handles) >= m.opts.MaxReassemblyChunks {
			m.freeze("发布窗口重复序号内容不同或超过消息上限")
			return false
		}
		old.handles = append(old.handles, handle)
		return false
	}
	size := uint64(proto.Size(batch))*4 + 256
	if sequence-generation.next >= uint64(generation.limits.Batches) || len(generation.future) >= generation.limits.Batches || size > generation.limits.Bytes || generation.bytes > generation.limits.Bytes-size {
		m.freeze("发布窗口乱序缓冲超过批数或字节上限")
		return false
	}
	entry := &publicationEntry{batch: batch, handles: []iAckHandle{handle}, buffer: buffer, bytes: size}
	generation.future[sequence] = entry
	generation.bytes += size
	for !m.isFrozen() {
		next := generation.future[generation.next]
		if next == nil {
			break
		}
		delete(generation.future, generation.next)
		generation.bytes -= next.bytes
		ack := &publicationAck{generation: generation, sequence: generation.next, gtids: batchGTIDs(next.batch), handles: next.handles}
		generation.next++
		if next.batch.GetEpoch() > m.lastEpoch || (next.batch.GetEpoch() == m.lastEpoch && next.batch.GetBatchSeq() > m.lastBatchSeq) {
			m.recordSeen(next.batch.GetEpoch(), next.batch.GetBatchSeq())
			m.continuityBaselineSet = true
		}
		if m.coalescing {
			if !m.appendReplay(next.batch, ack, next.buffer) {
				next.buffer.Release()
			}
		} else {
			m.applyAndFinish(next.batch, []iAckHandle{ack})
			next.buffer.Release()
		}
		// A different missing sequence starts a new bounded wait, whereas further
		// arrivals above the same gap never extend its deadline.
		generation.deadline = time.Time{}
	}
	if len(generation.future) != 0 && generation.deadline.IsZero() {
		generation.deadline = m.now().Add(generation.limits.Timeout)
	}
	return true
}

// One critical write covers each coalesced continuous prefix, before any broker
// acknowledgment. Failure leaves actual MySQL commits eligible for safe replay.
func (m *Module) persistPublicationAcknowledgements(handles []iAckHandle) error {
	type prefix struct {
		generation *publicationGeneration
		last       uint64
		proof      gmysql.GTIDSet
	}
	var prefixes []*prefix
	for _, handle := range handles {
		ack, ok := handle.(*publicationAck)
		if !ok {
			continue
		}
		var current *prefix
		for _, candidate := range prefixes {
			if candidate.generation == ack.generation {
				current = candidate
				break
			}
		}
		if current == nil {
			current = &prefix{generation: ack.generation, last: ack.generation.cursor.Last, proof: ack.generation.proof.Clone()}
			prefixes = append(prefixes, current)
		}
		if ack.sequence != current.last+1 {
			return errors.New("amqpin: noncontiguous publication acknowledgment")
		}
		for _, gtid := range ack.gtids {
			if err := current.proof.Update(gtid); err != nil {
				return err
			}
		}
		current.last = ack.sequence
	}
	for _, prefix := range prefixes {
		writer, ok := m.opts.Store.(iCriticalAsyncStore)
		if !ok {
			return errors.New("amqpin: publication cursor requires durable store")
		}
		cursor := prefix.generation.cursor
		cursor.Last, cursor.GTIDs = prefix.last, prefix.proof.String()
		ctx, cancel := m.publicationContext()
		err := m.persistSkipOp(ctx, writer, store.Op{Collection: "publication_window_cursors", ID: m.publicationRecordID(prefix.generation.key), Doc: cursor, Critical: true})
		cancel()
		if err != nil {
			return err
		}
		prefix.generation.cursor, prefix.generation.proof = cursor, prefix.proof
	}
	return nil
}
func (m *Module) publicationDeadline() time.Time {
	var earliest time.Time
	for _, generation := range m.publicationWindows {
		if !generation.deadline.IsZero() && (earliest.IsZero() || generation.deadline.Before(earliest)) {
			earliest = generation.deadline
		}
	}
	return earliest
}
func (m *Module) expirePublications() {
	deadline := m.publicationDeadline()
	if !deadline.IsZero() && !m.now().Before(deadline) {
		m.freeze("发布窗口等待缺失批次超时，保留未确认消息")
		m.discardPublicationBuffers()
	}
}
func (m *Module) discardPublicationBuffers() {
	for _, generation := range m.publicationWindows {
		for _, entry := range generation.future {
			entry.buffer.Release()
		}
		clear(generation.future)
		generation.bytes = 0
		generation.deadline = time.Time{}
	}
}

type iAppliedGTIDProof interface {
	HasAppliedGTIDs(context.Context, []string) (bool, error)
}

// A new epoch can replay transactions whose old generation still has a gap.
// Only exact completed facts or the native target GTID set can release those
// old deliveries; watermarks and received sequence numbers are not evidence.
func (m *Module) retireSupersededPublications() {
	for _, generation := range m.publicationWindows {
		if generation.key.epoch >= m.lastEpoch {
			continue
		}
		for sequence, entry := range generation.future {
			set, err := publicationGTIDSet(batchGTIDs(entry.batch))
			if err != nil {
				m.freeze("旧发布窗口事务证明无效")
				return
			}
			proven := m.appliedSet != nil && m.appliedSet.Contain(set)
			for _, current := range m.publicationWindows {
				proven = proven || current.proof.Contain(set)
			}
			if !proven {
				if writer, ok := m.opts.Applier.(iAppliedGTIDProof); ok {
					ctx, cancel := m.publicationContext()
					proven, err = writer.HasAppliedGTIDs(ctx, batchGTIDs(entry.batch))
					cancel()
					if err != nil {
						m.freeze("读取旧发布窗口原事务确认状态失败")
						return
					}
				}
			}
			if !proven {
				continue
			}
			for _, handle := range entry.handles {
				m.ackDuplicate(handle)
			}
			entry.buffer.Release()
			delete(generation.future, sequence)
			generation.bytes -= entry.bytes
		}
		if len(generation.future) == 0 {
			generation.deadline = time.Time{}
		}
	}
}
