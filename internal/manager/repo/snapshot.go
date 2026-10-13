// 全量运行仓储方法。
package repo

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var ErrSnapshotNotFound = errors.New("repo: 全量运行记录不存在")

// GetPipelineSnapshot 取某 pipeline 最近一次全量运行（run_id 最大的一条）。
//
// 一条 pipeline 可以有多次全量运行（唯一键是 (pipeline_id, run_id)），所以
// "取该 pipeline 的 snapshot"必须先定义取哪一次。这里取 run_id 最大者，
// 因为 run_id 由服务端单调递增分配，而 created_at 在同毫秒重跑时无法排序。
func GetPipelineSnapshot(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID) (*model.PipelineSnapshot, error) {
	var s model.PipelineSnapshot
	err := db.WithContext(ctx).Where("pipeline_id = ?", pipelineID).
		Order("run_id DESC").First(&s).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("repo: 查询全量运行失败: %w", err)
	}
	return &s, nil
}

// GetPipelineSnapshotRun 取某 pipeline 指定 run 的全量运行记录。
func GetPipelineSnapshotRun(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID uint64) (*model.PipelineSnapshot, error) {
	var s model.PipelineSnapshot
	err := db.WithContext(ctx).Where("pipeline_id = ? AND run_id = ?", pipelineID, runID).First(&s).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrSnapshotNotFound
		}
		return nil, fmt.Errorf("repo: 查询全量运行失败: %w", err)
	}
	return &s, nil
}

// ListSnapshotTables 分页列出某次全量运行的按表进度（按 schema、table 升序）。
//
// # 为什么形参不是 snapshotID
//
// pipeline_snapshot_tables 没有指向 pipeline_snapshots 主键的列：它的归属键
// 是 (pipeline_id, run_id, schema_name, table_name)，与 snapshots 的唯一键
// (pipeline_id, run_id) 同源。用 snapshot 的 id 或 snapshot_uuid 在这张表里
// 定位不到任何行。
//
// 之所以不补一个 snapshot_id 列来迁就调用形状：那样同一条归属关系会同时由
// (pipeline_id, run_id) 和 snapshot_id 两处表达，两处不一致时无法判定哪边
// 是权威——这正是红线「状态归属唯一」要防的形状。调用方从
// GetPipelineSnapshot 拿到的记录里已经有这两个值。
func ListSnapshotTables(ctx context.Context, db *gorm.DB, pipelineID uuidx.UUID, runID uint64, page, pageSize int) ([]model.PipelineSnapshotTable, int64, error) {
	limit, offset := normalizePage(page, pageSize)

	base := db.WithContext(ctx).Model(&model.PipelineSnapshotTable{}).
		Where("pipeline_id = ? AND run_id = ?", pipelineID, runID)

	var total int64
	if err := base.Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("repo: 统计全量按表进度总数失败: %w", err)
	}

	var rows []model.PipelineSnapshotTable
	err := base.Order("schema_name ASC, table_name ASC").Limit(limit).Offset(offset).Find(&rows).Error
	if err != nil {
		return nil, 0, fmt.Errorf("repo: 查询全量按表进度失败: %w", err)
	}
	return rows, total, nil
}

// CreateSnapshot 写入一次全量运行记录。
func CreateSnapshot(ctx context.Context, db *gorm.DB, s *model.PipelineSnapshot) error {
	if err := s.Validate(); err != nil {
		return err
	}
	if s.PipelineID.IsZero() {
		return fmt.Errorf("repo: 全量运行记录缺少 pipeline_id")
	}
	if err := db.WithContext(ctx).Create(s).Error; err != nil {
		if isDuplicateKeyError(err) {
			return fmt.Errorf("repo: 全量运行 (pipeline_id, run_id) 已存在: %w", err)
		}
		return fmt.Errorf("repo: 写入全量运行记录失败: %w", err)
	}
	return nil
}

// UpsertSnapshotTable 按 (pipeline_id, run_id, schema_name, table_name) 幂等
// 写入按表进度。
//
// 全量分块进度由节点反复上报，同一张表会被更新很多次；这里用"先查后写"而
// 不是 ON DUPLICATE KEY UPDATE，是为了让调用方能拿到 created 标志区分首次
// 登记与进度推进。并发首插撞唯一键时返回可重试错误而不是静默丢弃。
//
// 上面那句"两个并发调用带同一个键时后者会阻塞"只在 RR 下成立：
//
// REPEATABLE READ（MySQL 默认）二级索引等值查询无匹配行时 InnoDB 加间隙锁
//
//	                            ⇒ 第二个事务被挡住 ⇒ 互斥成立
//	READ COMMITTED              间隙锁被禁用 ⇒ 两个事务都查不到 ⇒ 都插入 ⇒ 两行
//
// 全仓当前没有任何地方设置或断言 transaction_isolation ⇒ 走 MySQL 默认 RR ⇒
// 今天是安全的。但这使正确性成为一个没人声明的部署配置依赖：运维把实例调成
// RC（生产常见调优）之后不会有任何报错、不会有测试失败，表现是高并发时出现同键
// 重复行——而高并发恰好是本函数唯一需要生效的场景。
func UpsertSnapshotTable(ctx context.Context, db *gorm.DB, in model.PipelineSnapshotTable) (created bool, err error) {
	if err := in.Validate(); err != nil {
		return false, err
	}
	if in.PipelineID.IsZero() {
		return false, fmt.Errorf("repo: 全量按表进度缺少 pipeline_id")
	}
	if in.SchemaName == "" || in.Table == "" {
		return false, fmt.Errorf("repo: 全量按表进度缺少库名或表名")
	}

	err = db.Transaction(func(tx *gorm.DB) error {
		var cur model.PipelineSnapshotTable
		qerr := tx.WithContext(ctx).
			Where("pipeline_id = ? AND run_id = ? AND schema_name = ? AND table_name = ?",
				in.PipelineID, in.RunID, in.SchemaName, in.Table).First(&cur).Error
		switch {
		case errors.Is(qerr, gorm.ErrRecordNotFound):
			row := in
			row.ID = uuidx.Nil
			if cerr := tx.WithContext(ctx).Create(&row).Error; cerr != nil {
				if isDuplicateKeyError(cerr) {
					return fmt.Errorf("repo: 全量按表进度并发首插冲突，请重试: %w", cerr)
				}
				return fmt.Errorf("repo: 写入全量按表进度失败: %w", cerr)
			}
			created = true
			return nil
		case qerr != nil:
			return fmt.Errorf("repo: 查询全量按表进度失败: %w", qerr)
		}
		if in.DoneRows < cur.DoneRows || in.DoneChunks != nil && cur.DoneChunks != nil && *in.DoneChunks < *cur.DoneChunks || cur.Status == model.SnapshotTableStatusDone && in.Status != model.SnapshotTableStatusDone ||
			cur.ObservedAtMS != 0 && in.ObservedAtMS < cur.ObservedAtMS {
			return nil
		}
		if cur.TotalChunks != nil && in.TotalChunks != nil && *cur.TotalChunks != *in.TotalChunks {
			return fmt.Errorf("repo: snapshot table total changed")
		}
		// Reporter can merge an unchanged snapshot into a fresh binlog report.
		// Its outer timestamp must not refresh a stale per-table read rate.
		if cur.EstRows == in.EstRows && cur.DoneRows == in.DoneRows && cur.Status == in.Status &&
			equalSnapshotMetric(cur.TotalChunks, in.TotalChunks) && equalSnapshotMetric(cur.DoneChunks, in.DoneChunks) && equalSnapshotMetric(cur.RowsPerSec, in.RowsPerSec) {
			return nil
		}

		changes := map[string]any{
			"est_rows":       in.EstRows,
			"done_rows":      in.DoneRows,
			"status":         in.Status,
			"observed_at_ms": in.ObservedAtMS,
		}
		if in.TotalChunks != nil {
			changes["total_chunks"], changes["done_chunks"] = *in.TotalChunks, *in.DoneChunks
		}
		// A restarted source has no measured rate until it reads a new chunk.
		changes["rows_per_sec"] = in.RowsPerSec
		uerr := tx.WithContext(ctx).Model(&model.PipelineSnapshotTable{}).Where("id = ?", cur.ID).Updates(changes).Error
		if uerr != nil {
			return fmt.Errorf("repo: 更新全量按表进度失败: %w", uerr)
		}
		created = false
		return nil
	})
	if err != nil {
		return false, err
	}
	return created, nil
}

func equalSnapshotMetric[T uint64 | float64](a, b *T) bool {
	return a == nil && b == nil || a != nil && b != nil && *a == *b
}
