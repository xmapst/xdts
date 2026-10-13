package repo

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// RecordNonRetryableCommand atomically stops only the current command intent.
// Late results and duplicate failures cannot overwrite newer recovery state.
func RecordNonRetryableCommand(ctx context.Context, db *gorm.DB, id, nodeID uuidx.UUID, cmdSeq, configVersion uint64, event model.Event, now time.Time) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&p, "id=?", id).Error; err != nil {
			return err
		}
		if p.SourceNodeID != nodeID && p.TargetNodeID != nodeID {
			return fmt.Errorf("command result ownership mismatch")
		}
		if cmdSeq == 0 || p.LastCmdSeq != cmdSeq || p.ConfigVersion != configVersion {
			return nil
		}
		if p.RecoveryExhaustedCmdSeq != nil && *p.RecoveryExhaustedCmdSeq == cmdSeq {
			return nil
		}
		if err := tx.Model(&p).Update("recovery_exhausted_cmd_seq", cmdSeq).Error; err != nil {
			return err
		}
		event.Ts = now
		return InsertEvents(ctx, tx, []model.Event{event})
	}, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
}
