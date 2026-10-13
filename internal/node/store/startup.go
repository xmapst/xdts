package store

import (
	"context"
	"errors"

	"github.com/xmapst/xdoc"
)

// PutStartupCritical is confined to the startup owner before OnInit/Serve.
// All later writes use the existing actor batching and critical acknowledgements.
func (m *Module) PutStartupCritical(ctx context.Context, op Op) error {
	if m.initialized || m.closed || m.identityDB == nil {
		return errors.New("store: startup identity write outside prepared startup phase")
	}
	op.Critical = true
	if err := op.validate(); err != nil {
		return err
	}
	if op.Collection != "identity" {
		return errors.New("store: startup writes are limited to identity")
	}
	doc, err := buildDoc(op)
	if err != nil {
		return err
	}
	return m.identityDB.Transaction(ctx, func(tx *xdoc.Tx) error {
		collection := tx.Collection(op.Collection)
		if op.InsertOnly {
			_, err := collection.FindByID(ctx, xdoc.String(op.ID))
			if err == nil {
				return ErrAlreadyExists
			}
			if !errors.Is(err, xdoc.ErrNotFound) {
				return err
			}
			_, err = collection.InsertOne(ctx, doc)
			return err
		}
		_, err := collection.Upsert(ctx, doc)
		return err
	})
}
