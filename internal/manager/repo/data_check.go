package repo

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/datacheck"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var ErrDataCheckBusy = errors.New("data check already pending")
var ErrDataCheckNotFound = errors.New("data check not found")

func CreateDataCheck(ctx context.Context, db *gorm.DB, pipeline, user uuidx.UUID, config datacheck.Config, sample uint64) (*model.DataCheckRun, error) {
	if config.Validate() != nil || len(config.Tables) > 20 || sample == 0 || sample > 1000000 {
		return nil, datacheck.ErrConfig
	}
	for _, t := range config.Tables {
		left, right := []string{}, []string{}
		for _, c := range t.Columns {
			left = append(left, c.Source)
			right = append(right, c.Target)
		}
		if (datacheck.LocalTable{Schema: t.SourceSchema, Table: t.SourceTable, Columns: left}).Validate() != nil || (datacheck.LocalTable{Schema: t.TargetSchema, Table: t.TargetTable, Columns: right}).Validate() != nil {
			return nil, datacheck.ErrConfig
		}
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	var run *model.DataCheckRun
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var p model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", pipeline).First(&p).Error; err != nil {
			return err
		}
		if p.DeletedAt.Valid {
			return ErrPipelineNotFound
		}
		var count int64
		if err := tx.Model(&model.DataCheckRun{}).Where("pipeline_id=? AND status IN ?", pipeline, []string{"queued", "running"}).Count(&count).Error; err != nil {
			return err
		}
		if count > 0 {
			return ErrDataCheckBusy
		}
		run = &model.DataCheckRun{PipelineID: pipeline, ConfigVersion: p.ConfigVersion, CreatedBy: user, Status: "queued", Config: raw, SampleModulo: sample, Results: model.JSON("[]"), ExpiresAt: time.Now().UTC().Add(10 * time.Minute)}
		return tx.Create(run).Error
	})
	return run, err
}

func ListDataChecks(ctx context.Context, db *gorm.DB, pipeline uuidx.UUID) ([]model.DataCheckRun, error) {
	out := []model.DataCheckRun{}
	err := db.WithContext(ctx).Where("pipeline_id=?", pipeline).Order("created_at DESC,id DESC").Limit(100).Find(&out).Error
	return out, err
}

func GetDataCheck(ctx context.Context, db *gorm.DB, pipeline, id uuidx.UUID) (*model.DataCheckRun, error) {
	var out model.DataCheckRun
	err := db.WithContext(ctx).Where("pipeline_id=? AND id=?", pipeline, id).First(&out).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, ErrDataCheckNotFound
	}
	return &out, err
}

func CancelDataCheck(ctx context.Context, db *gorm.DB, pipeline, id uuidx.UUID) error {
	if _, err := GetDataCheck(ctx, db, pipeline, id); err != nil {
		return err
	}
	return db.WithContext(ctx).Model(&model.DataCheckRun{}).Where("pipeline_id=? AND id=? AND status IN ?", pipeline, id, []string{"queued", "running"}).Updates(map[string]any{"status": "cancelled", "finished_at": time.Now().UTC()}).Error
}

func ClaimDataCheck(ctx context.Context, db *gorm.DB) (*model.DataCheckRun, error) {
	now := time.Now().UTC()
	if err := db.WithContext(ctx).Model(&model.DataCheckRun{}).Where("status IN ? AND expires_at<?", []string{"queued", "running"}, now).Updates(map[string]any{"status": "interrupted", "error": "校验任务已超时或执行进程中断，请重新发起", "finished_at": now}).Error; err != nil {
		return nil, err
	}
	var result *model.DataCheckRun
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var run model.DataCheckRun
		err := tx.Clauses(clause.Locking{Strength: "UPDATE", Options: "SKIP LOCKED"}).Where("status=? AND expires_at>?", "queued", now).Order("created_at,id").First(&run).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := tx.Model(&model.DataCheckRun{}).Where("id=? AND status=?", run.ID, "queued").Updates(map[string]any{"status": "running", "started_at": now}).Error; err != nil {
			return err
		}
		run.Status = "running"
		run.StartedAt = &now
		result = &run
		return nil
	})
	return result, err
}

func UpdateDataCheck(ctx context.Context, db *gorm.DB, id uuidx.UUID, results []datacheck.DigestComparison, status, message string) error {
	raw, err := json.Marshal(results)
	if err != nil {
		return err
	}
	update := map[string]any{"results": model.JSON(raw), "completed_tables": len(results), "status": status, "error": message}
	if status != "running" {
		update["finished_at"] = time.Now().UTC()
	}
	return db.WithContext(ctx).Model(&model.DataCheckRun{}).Where("id=? AND status=?", id, "running").Updates(update).Error
}
