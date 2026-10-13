package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/xmapst/xdts/internal/manager/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RecordConflictEvent persists both query surfaces under the producer's event
// UUID. The pipeline lock serializes retries and node reassignment together.
func RecordConflictEvent(ctx context.Context, db *gorm.DB, event model.Event, conflict model.Conflict) error {
	if event.ID.IsZero() || event.ID != conflict.ID || event.PipelineID.IsZero() || event.NodeID.IsZero() || event.PipelineID != conflict.PipelineID || event.Type != model.EventTypeConflict {
		return fmt.Errorf("repo: invalid conflict event identity")
	}
	if err := event.Validate(); err != nil {
		return err
	}
	if err := conflict.Validate(); err != nil {
		return err
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var pipeline model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&pipeline, "id = ?", event.PipelineID).Error; err != nil {
			return err
		}
		if pipeline.TargetNodeID != event.NodeID {
			return fmt.Errorf("repo: conflict reporter is not the assigned target")
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&event).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&conflict).Error; err != nil {
			return err
		}
		var storedEvent model.Event
		if err := tx.First(&storedEvent, "id = ?", event.ID).Error; err != nil {
			return err
		}
		var storedConflict model.Conflict
		if err := tx.First(&storedConflict, "id = ?", event.ID).Error; err != nil {
			return err
		}
		if storedEvent.PipelineID != event.PipelineID || storedEvent.NodeID != event.NodeID || storedEvent.Type != event.Type || storedEvent.Level != event.Level || storedEvent.Ts.UnixMilli() != event.Ts.UnixMilli() || !sameConflictJSON(storedEvent.Payload, event.Payload) || storedConflict.PipelineID != conflict.PipelineID || storedConflict.SchemaName != conflict.SchemaName || storedConflict.Table != conflict.Table || storedConflict.ConflictType != conflict.ConflictType || storedConflict.Resolution != conflict.Resolution || storedConflict.Ts.UnixMilli() != conflict.Ts.UnixMilli() || !sameConflictJSON(storedConflict.PK, conflict.PK) || !sameConflictJSON(storedConflict.SourceRow, conflict.SourceRow) || !sameConflictJSON(storedConflict.TargetRow, conflict.TargetRow) {
			return fmt.Errorf("repo: conflict event UUID was reused with different contents")
		}
		return nil
	})
}

func sameConflictJSON(a, b model.JSON) bool {
	decode := func(raw model.JSON) (any, error) {
		if len(raw) == 0 {
			return nil, nil
		}
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.UseNumber()
		var v any
		err := d.Decode(&v)
		return v, err
	}
	av, ae := decode(a)
	bv, be := decode(b)
	return ae == nil && be == nil && reflect.DeepEqual(av, bv)
}
