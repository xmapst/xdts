package amqpin

import (
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/transport"
)

type ownedInput struct {
	message *transport.Message
	buffer  *resourcequota.Buffer
	err     error
	decoded *preparedBatch
}

type pendingReplay struct {
	batch   *iproto.DataBatch
	handles []iAckHandle
	buffers []*resourcequota.Buffer
	batches int
}

// The serial callback owns only its message and a bounded decode ticket. All
// continuity, policy, SQL and acknowledgement remain on the input coordinator.
func (m *Module) enqueueInput(message *transport.Message) {
	entry := m.prepareInput(message)
	select {
	case m.input <- entry:
	case <-m.shutdownCtx.Done():
		entry.release()
	}
}

type preparedBatch struct {
	batch  *iproto.DataBatch
	buffer *resourcequota.Buffer
	err    error
	slots  chan struct{}
}

func (entry *ownedInput) release() {
	entry.buffer.Release()
	if prepared := entry.decoded; prepared != nil {
		prepared.buffer.Release()
		if prepared.slots != nil {
			<-prepared.slots
			prepared.slots = nil
		}
	}
}

func (m *Module) prepareInput(message *transport.Message) (entry ownedInput) {
	buffer, err := m.opts.Budget.Reserve(uint64(len(message.Body)) + 256)
	entry = ownedInput{message: message, buffer: buffer, err: err}
	if err != nil {
		return entry
	}
	env, err := message.Envelope()
	if err != nil || env.Properties.Type != iproto.MessageTypeDataBatch {
		return entry
	}
	select {
	case m.predecodeSlots <- struct{}{}:
	default:
		return entry
	}
	prepared := &preparedBatch{batch: new(iproto.DataBatch), slots: m.predecodeSlots}
	entry.decoded = prepared
	defer func() {
		if recover() != nil {
			prepared.buffer.Release()
			prepared.buffer = nil
			prepared.batch = nil
			prepared.err = errors.New("amqpin: input decode failed")
		}
	}()
	prepared.buffer, prepared.err = m.opts.Budget.Decode(env.Body, env.Properties.ContentEncoding, prepared.batch)
	return entry
}

func (m *Module) runInput() {
	defer close(m.inputDone)
	defer m.releaseQueuedInput()
	defer m.discardPublicationBuffers()
	for {
		if m.recoveryWaiting {
			select {
			case <-m.shutdownCtx.Done():
				return
			case completion := <-m.opts.RecoveryCompletions:
				if !m.beginRuntimeWork() {
					return
				}
				err := m.finishRecovery(completion)
				if err != nil {
					m.freeze("单表恢复完成屏障失败")
				}
				completion.Result <- err
				m.finishInputGroup()
				continue
			}
		}
		var entry ownedInput
		var timer *time.Timer
		var timeout <-chan time.Time
		if deadline := m.publicationDeadline(); !deadline.IsZero() {
			timer = time.NewTimer(max(0, time.Until(deadline)))
			timeout = timer.C
		}
		select {
		case request := <-m.opts.RecoveryRequests:
			if timer != nil {
				timer.Stop()
			}
			if !m.beginRuntimeWork() {
				return
			}
			err := m.acceptRecovery(request)
			if err != nil {
				m.freeze("单表恢复起点屏障失败")
				request.Permit <- err
			}
			m.finishInputGroup()
			continue
		case <-m.shutdownCtx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case entry = <-m.input:
			if timer != nil {
				timer.Stop()
			}
		case <-timeout:
			if !m.beginRuntimeWork() {
				return
			}
			m.expirePublications()
			m.finishInputGroup()
			continue
		}
		if !m.beginRuntimeWork() {
			entry.release()
			return
		}
		m.consumeInputGroup(entry)
	}
}

func (m *Module) consumeInputGroup(entry ownedInput) {
	defer func() {
		if recover() != nil {
			m.freeze("增量输入协调器异常，保留未确认消息")
		}
		entry.release()
		m.finishInputGroup()
	}()
	m.coalescing = iproto.ReplayDegree(m.runtimeWritePolicy) > 1 && m.catchupHigh == nil
	limit := 1
	if m.coalescing {
		limit = min(iproto.WriteBatchSize(m.runtimeWritePolicy), m.opts.MaxReassemblyChunks)
	}
	for i := 0; i < limit; i++ {
		if entry.err != nil {
			m.freeze(entry.err.Error())
		}
		m.handlePreparedMessage(entry.message, entry.decoded)
		entry.release()
		if i+1 == limit || m.isFrozen() {
			break
		}
		select {
		case entry = <-m.input:
		default:
			m.flushReplay()
			return
		}
	}
	m.flushReplay()
}

func (m *Module) finishInputGroup() {
	if !m.isFrozen() {
		if err := m.maybePermitRecovery(); err != nil {
			m.freeze("单表恢复事务提交屏障失败")
		}
	}
	if !m.isFrozen() {
		m.retireSupersededPublications()
		m.expirePublications()
	}
	if m.isFrozen() {
		m.discardPublicationBuffers()
	}
	m.discardPendingReplay()
	m.coalescing = false
	kept := m.heldHandles[:0]
	for _, handle := range m.heldHandles {
		if tracked, ok := handle.(*trackedAck); !ok || !tracked.done {
			kept = append(kept, handle)
		}
	}
	clear(m.heldHandles[len(kept):])
	m.heldHandles = kept
	m.handlerMu.Lock()
	defer m.handlerMu.Unlock()
	m.handlerActive = false
	if m.handlerClosing {
		m.discardReassembly()
		m.heldHandles = nil
	}
	m.finishRuntimeUpdateLocked()
}

func (m *Module) releaseQueuedInput() {
	for {
		select {
		case entry := <-m.input:
			entry.release()
		default:
			return
		}
	}
}

func batchHasDDL(batch *iproto.DataBatch) bool {
	for _, txn := range batch.GetTxns() {
		if txn.GetDdl() != nil {
			return true
		}
	}
	return false
}

func (m *Module) appendReplay(batch *iproto.DataBatch, handle iAckHandle, buffer *resourcequota.Buffer) bool {
	limit := iproto.WriteBatchSize(m.runtimeWritePolicy)
	if m.pendingReplay != nil && len(m.pendingReplay.batch.Txns)+len(batch.Txns) > limit {
		m.flushReplay()
		if m.isFrozen() {
			return false
		}
	}
	if len(batch.Txns) > limit {
		m.applyAndFinish(batch, []iAckHandle{handle})
		return false
	}
	if m.pendingReplay == nil {
		m.pendingReplay = &pendingReplay{batch: &iproto.DataBatch{PipelineId: batch.PipelineId, RunId: batch.RunId, Epoch: batch.Epoch, BatchSeq: batch.BatchSeq}}
	}
	pending := m.pendingReplay
	pending.batch.Txns = append(pending.batch.Txns, batch.Txns...)
	pending.handles = append(pending.handles, handle)
	pending.buffers = append(pending.buffers, buffer)
	pending.batches++
	return true
}

func (m *Module) flushReplay() {
	pending := m.pendingReplay
	if pending == nil {
		return
	}
	m.pendingReplay = nil
	defer func() {
		for _, buffer := range pending.buffers {
			buffer.Release()
		}
	}()
	if m.isFrozen() {
		return
	}
	previous := m.stats.batchesApplied.Load()
	m.applyAndFinish(pending.batch, pending.handles)
	if m.stats.batchesApplied.Load() != previous {
		m.stats.batchesApplied.Add(int64(pending.batches - 1))
	}
}

func (m *Module) discardPendingReplay() {
	if m.pendingReplay != nil {
		for _, buffer := range m.pendingReplay.buffers {
			buffer.Release()
		}
		m.pendingReplay = nil
	}
}
