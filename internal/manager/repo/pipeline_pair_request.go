package repo

import (
	"context"
	"errors"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
)

var ErrPipelinePairRequestConflict = errors.New("bidirectional request identity reused")
var ErrPipelinePairResultUnavailable = errors.New("bidirectional result no longer available")

func GetPipelinePairRequest(ctx context.Context, db *gorm.DB, id uuidx.UUID) (*model.PipelinePairRequest, error) {
	var request model.PipelinePairRequest
	err := db.WithContext(ctx).First(&request, "id=?", id).Error
	return &request, err
}

func CreatePipelinePairRequest(ctx context.Context, db *gorm.DB, request *model.PipelinePairRequest) error {
	if err := db.WithContext(ctx).Create(request).Error; err != nil {
		if isDuplicateKeyError(err) {
			return ErrPipelinePairRequestConflict
		}
		return err
	}
	return nil
}
