package bootstrap

import (
	"context"
	"errors"
	"time"

	"github.com/xmapst/xdoc"

	"github.com/xmapst/xdts/internal/node/store"
)

const RetirementRecordID = "retirement"

var ErrIdentityRetired = errors.New("bootstrap: node identity retired; wait for a new register_code and data_dir")

// Retirement is a separate immutable identity document; existing identity keys
// and the store password remain intact for recovery and resource reclamation.
type Retirement struct {
	NodeID    string    `bson:"node_id"`
	Reason    string    `bson:"reason"`
	RetiredAt time.Time `bson:"retired_at"`
}

type IRetirementReader interface {
	Get(context.Context, string, string, any) error
}

type IRetirementStore interface {
	IRetirementReader
	PutCritical(context.Context, store.Op) error
}

func CheckRetirement(ctx context.Context, storage IRetirementReader, nodeID string) error {
	var record Retirement
	if err := storage.Get(ctx, "identity", RetirementRecordID, &record); err != nil {
		if errors.Is(err, xdoc.ErrNotFound) {
			return nil
		}
		return err
	}
	if record.NodeID != nodeID || !canonicalUUID(record.NodeID) || record.Reason != "NODE_IDENTITY_RETIRED" || record.RetiredAt.IsZero() {
		return errors.New("bootstrap: invalid retirement record; restore the original identity")
	}
	return ErrIdentityRetired
}

func PersistRetirement(ctx context.Context, storage IRetirementStore, nodeID string) error {
	if !canonicalUUID(nodeID) {
		return errors.New("bootstrap: invalid retired node ID")
	}
	if err := CheckRetirement(ctx, storage, nodeID); errors.Is(err, ErrIdentityRetired) {
		return nil
	} else if err != nil {
		return err
	}
	err := storage.PutCritical(ctx, store.Op{Collection: "identity", ID: RetirementRecordID, InsertOnly: true,
		Doc: Retirement{NodeID: nodeID, Reason: "NODE_IDENTITY_RETIRED", RetiredAt: time.Now().UTC()}})
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return err
	}
	if err := CheckRetirement(ctx, storage, nodeID); !errors.Is(err, ErrIdentityRetired) {
		if err != nil {
			return err
		}
		return errors.New("bootstrap: retirement was not committed")
	}
	return nil
}
