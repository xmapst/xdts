package controller

import (
	"context"
	"errors"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/node/store"
)

// The pipeline's durable Manager ceiling continues fencing all old reservations.
// Relinquishing local ownership retires the remainder of this node's range so
// migrating back with its old disk must acquire a fresh range from Manager.
func releaseLocalSourceEpochs(ctx context.Context, st iStoreAccess, id string) error {
	if st == nil {
		return errors.New("controller: source migration requires persistent store")
	}
	var doc map[string]any
	if err := st.Get(ctx, "checkpoints", id+".source", &doc); err != nil {
		if errors.Is(err, xdoc.ErrNotFound) {
			return nil
		}
		return err
	}
	epoch, ok := store.CheckpointEpoch(doc)
	if !ok {
		return errors.New("controller: source migration has invalid local epoch")
	}
	doc["epoch_ceiling"] = int64(epoch)
	return st.PutCritical(ctx, store.Op{Collection: "checkpoints", ID: id + ".source", Doc: doc, Critical: true})
}
