package dispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// dispatchRPC only calls this after verifying the envelope and source identity.
func (d *Dispatcher) onSourceRecoveryReq(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var req iproto.SourceRecoveryRequest
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &req); err != nil {
		return fmt.Errorf("dispatch: invalid source recovery request: %w", err)
	}
	reply, err := repo.ReserveSourceEpochs(ctx, d.db, nodeID, &req, d.now())
	if err != nil {
		code := "SOURCE_RECOVERY_FAILED"
		if errors.Is(err, repo.ErrSourceRecoveryConflict) {
			code = "SOURCE_RECOVERY_CHANGED"
		} else if errors.Is(err, repo.ErrSourceRecoveryUnavailable) {
			code = "SOURCE_RECOVERY_UNAVAILABLE"
		}
		reply = &iproto.SourceRecoveryReply{Error: &iproto.Error{Code: code, Message: "source recovery cannot establish a current published checkpoint"}, RequestId: req.RequestId}
	}
	return d.publishReply(ctx, env, nodeID, iproto.MessageTypeSourceRecoveryReply, reply)
}
