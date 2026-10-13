package mysqlout

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
)

// InstallRuntimeResource is pure and called only after all batch workers drain.
// The stable root handle belongs to the consumer. Retired connections transfer
// to the loader, which closes them outside the actor and ownership mutex.
func (w *Writer) InstallRuntimeResource(resource iproto.ITargetRuntimeResource, config iproto.TargetRuntimeConfig) (iproto.ITargetRuntimeResource, error) {
	fresh, ok := resource.(*Writer)
	if !ok || fresh == nil || fresh == w || w.closed || fresh.closed ||
		w.opts.PipelineID != fresh.opts.PipelineID || w.opts.Host != fresh.opts.Host || w.opts.Port != fresh.opts.Port ||
		w.opts.User != fresh.opts.User || w.opts.Password != fresh.opts.Password || w.opts.ExpectedServerUUID != fresh.opts.ExpectedServerUUID {
		return nil, errors.New("mysqlout: invalid prepared target writer")
	}
	if err := fresh.UpdateRuntimeMapping(config); err != nil {
		return nil, err
	}
	if len(w.workers) == len(fresh.workers) {
		if err := w.UpdateRuntimeMapping(config); err != nil {
			return nil, err
		}
		return fresh, nil
	}
	retired := new(Writer)
	*retired = *w
	// The candidate has already completed all IO and validation. These value
	// swaps cannot fail and transfer each pool to a single new owner.
	*w = *fresh
	w.conflictSink = retired.conflictSink
	w.catchingUp, w.restoreForeignKeys = retired.catchingUp, retired.restoreForeignKeys
	w.recovery = retired.recovery
	retired.recovery = nil
	*fresh = Writer{closed: true}
	return retired, nil
}
