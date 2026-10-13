// 日志检索与批量写入仓储方法。
package repo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	defaultLogQueryLimit = 50
	maxLogQueryLimit     = 500
)

// LogQuery 是日志检索的过滤条件与分页参数（需求 MGR-MON-02）。
//
// Since/Until 是必填的时间范围：一是日志检索本来就该圈定时间窗，避免
// 误加载全量；二是 logs 表按天 RANGE COLUMNS(ts) 分区（见
// internal/manager/repo/ddl_log.go），ts 出现在 WHERE 条件里优化器才能做分区裁剪、跳过窗口外
// 的分区，否则会退化成扫描全部分区。
type LogQuery struct {
	Since, Until time.Time

	// NodeID/PipelineID 为 uuidx.Nil 表示不按该维度过滤。
	NodeID     uuidx.UUID
	PipelineID uuidx.UUID
	// Level 为空字符串表示不按级别过滤。
	Level model.LogLevel
	// Module 为空字符串表示不按模块过滤。
	Module string
	// Keyword 对 message 做 LIKE 模糊匹配（自动转义字面量里的
	// %/_/\，按字面匹配，不被解释成通配符），空字符串表示不过滤。
	Keyword string

	// Limit 是单页条数，<=0 时取 defaultLogQueryLimit，超过
	// maxLogQueryLimit 时截断为 maxLogQueryLimit。
	Limit int

	// BeforeTs/BeforeID 是游标分页的续页起点：传入上一页最后一条记录的
	// (ts, id)，本次只返回比它更早的记录（结果按 ts DESC, id DESC 排序，
	// 即翻页方向与排序方向一致）。BeforeTs 为零值表示从最新的记录开始
	// （第一页）。翻页时必须同时传 BeforeTs 与 BeforeID——只给 BeforeTs
	// 会漏掉同一毫秒内排在游标之后、但 id 更大的记录。
	BeforeTs time.Time
	BeforeID uuidx.UUID
}

// applyLogFilters 把 LogQuery 里"这条记录是否满足过滤条件"相关的部分
// （必填时间范围 + 节点/pipeline/级别/模块/关键字）应用到 tx 上，并做
// 相应的参数校验。ListLogs 与 CountLogs 共用这一份逻辑：如果列表查询和
// 总数统计各自维护一套过滤条件拼接代码，两套逻辑迟早会在某次改动中漂移
// ——到那时会出现"列表显示 20 条、总数却说有 35 条"这种从现象上很难联想
// 到"两处 WHERE 子句不一致"的 bug，共用同一份代码从根上消除这种漂移的
// 可能性。
func applyLogFilters(tx *gorm.DB, q LogQuery) (*gorm.DB, error) {
	tx = ReadResourceScope(tx.Statement.Context, tx, ReadLog)
	if q.Since.IsZero() || q.Until.IsZero() {
		return nil, fmt.Errorf("repo: 日志检索必须指定 Since/Until 时间范围，避免扫描全部分区")
	}
	if q.Until.Before(q.Since) {
		return nil, fmt.Errorf("repo: 日志检索的 Until 不能早于 Since")
	}
	if q.Level != "" && !q.Level.Valid() {
		return nil, fmt.Errorf("model: 未知的日志级别 %q", q.Level)
	}

	tx = tx.Where("ts >= ? AND ts <= ?", q.Since, q.Until)
	if !q.NodeID.IsZero() {
		tx = tx.Where("node_id = ?", q.NodeID)
	}
	if !q.PipelineID.IsZero() {
		tx = tx.Where("pipeline_id = ?", q.PipelineID)
	}
	if q.Level != "" {
		tx = tx.Where("level = ?", q.Level)
	}
	if q.Module != "" {
		tx = tx.Where("module = ?", q.Module)
	}
	if q.Keyword != "" {
		tx = tx.Where("message LIKE ? ESCAPE '\\\\'", "%"+escapeLikeKeyword(q.Keyword)+"%")
	}
	return tx, nil
}

func ListLogs(ctx context.Context, db *gorm.DB, q LogQuery) ([]model.Log, error) {
	tx, err := applyLogFilters(db.WithContext(ctx).Model(&model.Log{}), q)
	if err != nil {
		return nil, err
	}

	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultLogQueryLimit
	case limit > maxLogQueryLimit:
		limit = maxLogQueryLimit
	}

	if !q.BeforeTs.IsZero() {
		tx = tx.Where("(ts < ?) OR (ts = ? AND id < ?)", q.BeforeTs, q.BeforeTs, q.BeforeID)
	}

	var logs []model.Log
	if err := tx.Order("ts DESC, id DESC").Limit(limit).Find(&logs).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询日志失败: %w", err)
	}
	return logs, nil
}

// 过滤条件与 ListLogs 完全共用 LogQuery 结构与 applyLogFilters 逻辑
// （时间范围必填、节点/pipeline/级别/模块/关键字过滤），因此两者对同一份
// 数据永远给出一致的口径。忽略 q.Limit/BeforeTs/BeforeID：这三个字段是
// ListLogs 分页专用的，与"总共有多少条"无关，调用方不需要（也不应该）为
// 了拿总数而给它们赋值——即使赋了值也不会影响 CountLogs 的结果。
func CountLogs(ctx context.Context, db *gorm.DB, q LogQuery) (int64, error) {
	tx, err := applyLogFilters(db.WithContext(ctx).Model(&model.Log{}), q)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.Count(&count).Error; err != nil {
		return 0, fmt.Errorf("repo: 统计日志总数失败: %w", err)
	}
	return count, nil
}

// escapeLikeKeyword 转义 LIKE 模式里的字面量通配符（%、_）与转义符本身
// （\），让调用方传入的关键字按字面匹配，不被解释成 SQL 通配符。配合
// ListLogs 里显式的 `ESCAPE '\\'` 子句使用。
func escapeLikeKeyword(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return r.Replace(s)
}

// insertLogsChunkSize 是 InsertLogs 单块插入的行数上限。取 500 而不是
// DeleteInBatches 用的 5000：那是纯 DELETE，这里每行还带 message（TEXT）
// 与 attrs（JSON），单块字节数更容易顶到 max_allowed_packet，取一个更
// 保守的值。
const insertLogsChunkSize = 500

// InsertLogs 批量写入日志。
//
// 不做去重：重复投递（网络重试、outbox 补发）由调用方与更上游的确认机制
// 处理，本函数只负责如实写入调用方给定的行——调用方若希望重放幂等，需要
// 自己在写入前生成确定性的行 ID（Base/TimePKBase 的 BeforeCreate 钩子只
// 在 ID 为零值时才会分配新的 UUIDv7）。
//
// 校验 ts 非零：logs 按天分区、主键是 (id, ts)，零值 ts（0001-01-01）既
// 超出 MySQL DATETIME 的取值范围，也落不进任何已创建的分区，会导致该行
// 所在块整体写入失败——这里提前逐行校验，让错误信息直接定位到具体是第几
// 行漏设了 ts，不用等数据库报错后再去反查。
func InsertLogs(ctx context.Context, db *gorm.DB, rows []model.Log) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += insertLogsChunkSize {
		end := min(start+insertLogsChunkSize, len(rows))
		chunk := rows[start:end]
		for i := range chunk {
			if chunk[i].Ts.IsZero() {
				return fmt.Errorf("repo: 批量写入日志失败：第 %d 行 ts 为空（第 %d 行之前的块已提交，不回滚）", start+i, start)
			}
			if err := chunk[i].Validate(); err != nil {
				return fmt.Errorf("repo: 批量写入日志失败：第 %d 行校验未通过（第 %d 行之前的块已提交，不回滚）: %w", start+i, start, err)
			}
		}
		if err := db.WithContext(ctx).Create(&chunk).Error; err != nil {
			return fmt.Errorf("repo: 批量写入日志失败：第 %d~%d 行所在块写入出错（更早的块已提交，不回滚）: %w", start, end-1, err)
		}
	}
	return nil
}
