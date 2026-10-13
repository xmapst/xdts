package binlogin

import (
	"context"
	"errors"
	"fmt"
	"math"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
)

func (m *Module) publishTransaction(ctx context.Context, read, published gmysql.GTIDSet, txn *iproto.Txn) (bool, error) {
	limits, err := iproto.NormalizePublicationWindow(m.sourceRuntime.PublicationWindow)
	if err != nil {
		return false, err
	}
	// DDL and whole fragmented transactions are serial publication barriers.
	// The next parallel segment gets a new first sequence after these messages.
	if limits.Batches <= 1 || txn.GetDdl() != nil || len(txn.GetRows()) > m.opts.MaxRowsPerChunk {
		if err := m.drainPublicationWindow(ctx); err != nil {
			return false, err
		}
		return false, m.emitTxn(ctx, txn)
	}
	m.publishRead, m.publishConfirmed = read, published
	if m.publishWindow == nil {
		if m.batchSeq.Load() == math.MaxUint64 {
			return false, errors.New("binlogin: batch sequence exhausted")
		}
		m.publishFirstSeq = m.batchSeq.Load() + 1
		window, err := newPublicationWindow(limits.Batches, limits.Bytes, m.opts.SendTimeout, m.opts.Budget, m.publishBatch, m.confirmPublication)
		if err != nil {
			return false, err
		}
		m.publishWindow = window
	}
	batch := singleTxnBatch(m.opts.PipelineID, m.opts.RunID, m.epoch, 0, txn)
	if err := m.limiter.waitForBatch(ctx, batch); err != nil {
		return false, fmt.Errorf("binlogin: incremental rate wait interrupted: %w", err)
	}
	return true, m.publishWindow.admit(ctx, batch, func(owned *iproto.DataBatch) error {
		if m.batchSeq.Load() == math.MaxUint64 {
			return errors.New("binlogin: batch sequence exhausted")
		}
		owned.BatchSeq = m.batchSeq.Add(1)
		owned.PublicationWindowFirstSeq = m.publishFirstSeq
		owned.PublicationWindowPolicy = iproto.ClonePublicationWindow(m.sourceRuntime.PublicationWindow)
		return nil
	})
}

func (m *Module) confirmPublication(batch *iproto.DataBatch) error {
	for _, txn := range batch.GetTxns() {
		if err := m.publishConfirmed.Update(txn.GetGtid()); err != nil {
			return err
		}
		if m.opts.SnapshotDDLGuard != nil {
			m.opts.SnapshotDDLGuard.Observe(m.publishConfirmed.String())
		}
		m.stats.txnsEmitted.Add(1)
		m.stats.rowsEmitted.Add(int64(len(txn.GetRows())))
		m.emitProgress(m.publishRead.String(), m.publishConfirmed.String(), txn.GetCommitTsMs(), len(txn.GetRows()))
	}
	return nil
}

func (m *Module) pollPublications(ctx context.Context) error {
	if m.publishWindow == nil {
		return ctx.Err()
	}
	return m.publishWindow.poll(ctx)
}

func (m *Module) drainPublicationWindow(ctx context.Context) error {
	if m.publishWindow == nil {
		return ctx.Err()
	}
	if err := m.publishWindow.drain(ctx); err != nil {
		return err
	}
	m.closePublicationWindow()
	return nil
}

func (m *Module) closePublicationWindow() {
	if m.publishWindow != nil {
		_ = m.publishWindow.close()
		m.publishWindow = nil
		m.publishFirstSeq = 0
	}
}

func (m *Module) resumePublications(ctx context.Context, fallback string) (string, error) {
	if m.publishWindow == nil {
		return fallback, nil
	}
	if err := m.publishWindow.resume(ctx); err != nil {
		return "", err
	}
	return m.publishConfirmed.String(), nil
}
