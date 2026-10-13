package amqpin

import (
	"context"
	"fmt"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type iTableRecoveryApplier interface {
	SetRecoveryTables(context.Context, []*iproto.SnapshotRecoveryTable) error
	FlushRecoveryTransactions(context.Context, string) (iproto.BatchApplyResult, error)
}

func (m *Module) acceptRecovery(request iproto.SnapshotRecoveryRequest) error {
	if err := iproto.ValidateSnapshotRecoveryBegin(request.Begin); err != nil {
		return err
	}
	b := request.Begin
	if b.PipelineId != m.opts.PipelineID || b.RunId != m.opts.RunID || b.Recovery.Sequence != m.recoverySequence+1 || m.recoveryRequest != nil || len(b.Tables) != len(m.recoveryTables) {
		return fmt.Errorf("amqpin: recovery request identity/order mismatch")
	}
	for i, table := range b.Tables {
		if !proto.Equal(table, m.recoveryTables[i]) {
			return fmt.Errorf("amqpin: recovery request scope differs from isolated tables")
		}
	}
	m.recoveryRequest = &request
	m.recoveryPending = true
	return m.maybePermitRecovery()
}

func (m *Module) maybePermitRecovery() error {
	request := m.recoveryRequest
	if request == nil || m.recoveryWaiting {
		return nil
	}
	low, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, request.Begin.LowGtidSet)
	if err != nil {
		return err
	}
	if m.catchupHigh != nil && !low.Contain(m.catchupHigh) {
		return fmt.Errorf("amqpin: recovery low precedes snapshot high")
	}
	if !m.consumedSet.Contain(low) {
		return nil
	}
	applier, ok := m.opts.Applier.(iTableRecoveryApplier)
	if !ok {
		return fmt.Errorf("amqpin: recovery applier missing")
	}
	// The returned result records native commits before any permit is sent.
	ctx, cancel := context.WithTimeout(m.shutdownCtx, 10*m.opts.ApplyAttemptTimeout)
	result, err := applier.FlushRecoveryTransactions(ctx, request.Begin.LowGtidSet)
	cancel()
	m.recordApplyResult(result)
	if err != nil {
		return err
	}
	m.recoveryWaiting = true
	select {
	case request.Permit <- nil:
	case <-m.shutdownCtx.Done():
		return m.shutdownCtx.Err()
	}
	return nil
}

func (m *Module) finishRecovery(completion iproto.SnapshotRecoveryCompletion) error {
	end := completion.End
	if m.recoveryRequest == nil || !m.recoveryWaiting || !proto.Equal(end.GetRecovery(), m.recoveryRequest.Begin.GetRecovery()) || end.GetLowGtidSet() != m.recoveryRequest.Begin.LowGtidSet {
		return fmt.Errorf("amqpin: unexpected recovery completion")
	}
	applier := m.opts.Applier.(iTableRecoveryApplier)
	// Transactions acknowledged after L remain in the durable log. Replay
	// them after replacement so native GTIDs cannot hide overwritten rows.
	ctx, cancel := context.WithTimeout(m.shutdownCtx, 10*m.opts.ApplyAttemptTimeout)
	result, err := applier.FlushRecoveryTransactions(ctx, m.consumedSet.String())
	cancel()
	m.recordApplyResult(result)
	if err != nil {
		return err
	}
	if err := applier.SetRecoveryTables(m.shutdownCtx, end.RecoveryTables); err != nil {
		return err
	}
	high, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.HighGtidSet)
	if err != nil {
		return err
	}
	if !high.Contain(m.consumedSet) {
		// A source watermark can contain independent commits beyond this H;
		// H itself must at least include the immutable recovery low.
		low, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, end.LowGtidSet)
		if !high.Contain(low) {
			return fmt.Errorf("amqpin: invalid recovery high")
		}
	}
	m.catchupHigh = high
	m.recoverySequence = end.Recovery.Sequence
	m.recoveryWaiting = false
	m.recoveryRequest = nil
	m.recoveryPending = len(end.RecoveryTables) > 0
	m.recoveryTables = make([]*iproto.SnapshotRecoveryTable, 0, len(end.RecoveryTables))
	for _, table := range end.RecoveryTables {
		m.recoveryTables = append(m.recoveryTables, proto.Clone(table).(*iproto.SnapshotRecoveryTable))
	}
	if m.opts.FullOnly {
		m.fullOnlyHigh = high.Clone()
	}
	m.maybeFinishCatchup()
	return nil
}
