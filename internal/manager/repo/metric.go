// pipeline_metrics 保留分块追加契约；实时进度由 internal/manager/repo/progress.go 在事务内
// 维护完整最新状态和降采样游标，再调用本文件的追加入口。
package repo

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
)

// insertPipelineMetricsChunkSize 是 InsertPipelineMetrics 单块插入的行数
// 上限，取值与 insertLogsChunkSize/insertEventsChunkSize（log.go/
// internal/manager/repo/event.go）一致，保持三条批量入库路径同一个可调参数、同一个心智模型——
// 虽然 pipeline_metrics 每行只有几个定长数值列，没有 TEXT/JSON，单块字节
// 数远小于 logs/events，理论上可以取更大的块，但对齐既有值更利于三条路径
// 统一调优，需要时再单独放大。
const insertPipelineMetricsChunkSize = 500

// InsertPipelineMetrics commits bounded chunks with nonzero business timestamps.
// A later failure leaves earlier chunks committed; callers must not assume rollback
// or deduplication when deciding whether to retry.
func InsertPipelineMetrics(ctx context.Context, db *gorm.DB, rows []model.PipelineMetric) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += insertPipelineMetricsChunkSize {
		end := min(start+insertPipelineMetricsChunkSize, len(rows))
		chunk := rows[start:end]
		for i := range chunk {
			if chunk[i].Ts.IsZero() {
				return fmt.Errorf("repo: 批量写入 pipeline 指标失败：第 %d 行 ts 为空（第 %d 行之前的块已提交，不回滚）", start+i, start)
			}
			if chunk[i].PipelineID.IsZero() {
				return fmt.Errorf("repo: 批量写入 pipeline 指标失败：第 %d 行 pipeline_id 为空（第 %d 行之前的块已提交，不回滚）", start+i, start)
			}
			if chunk[i].DelayMs < 0 || chunk[i].RPS < 0 || chunk[i].BPS < 0 || math.IsNaN(chunk[i].RPS) || math.IsNaN(chunk[i].BPS) || math.IsInf(chunk[i].RPS, 0) || math.IsInf(chunk[i].BPS, 0) || chunk[i].QueueDepth < 0 || chunk[i].Errors < 0 || (chunk[i].TPS != nil && (math.IsNaN(*chunk[i].TPS) || math.IsInf(*chunk[i].TPS, 0) || *chunk[i].TPS < 0)) || (chunk[i].Conflicts != nil && *chunk[i].Conflicts < 0) || (len(chunk[i].Values) != 0 && !json.Valid(chunk[i].Values)) {
				return fmt.Errorf("repo: 批量写入 pipeline 指标失败：第 %d 行存在非法数值或 JSON（第 %d 行之前的块已提交，不回滚）", start+i, start)
			}
		}
		if err := db.WithContext(ctx).Create(&chunk).Error; err != nil {
			return fmt.Errorf("repo: 批量写入 pipeline 指标失败：第 %d~%d 行所在块写入出错（更早的块已提交，不回滚）: %w", start, end-1, err)
		}
	}
	return nil
}
