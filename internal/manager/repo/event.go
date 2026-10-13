// 事件检索、确认与批量写入仓储方法（需求 MGR-MON-03/09：错误事件、DDL
// 记录、冲突记录、全量进度入库与查看）。
//
// 检索 + acked_by 确认操作 + InsertEvents 批量写入（调用方见 InsertEvents
// doc，那里列了 ingest 与 dispatch 的全部六处）。
package repo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

const (
	defaultEventQueryLimit = 50
	maxEventQueryLimit     = 500
)

// ErrEventNotFound 表示事件不存在。
var ErrEventNotFound = errors.New("repo: 事件不存在")

// ErrEventAlreadyAcked 表示该事件已被另一个用户确认过（方案 B：只允许首次
// 确认）。同一个用户重复确认不返回本错误，返回 nil —— 见 AckEvent 的文档。
//
// 而且在实际落地的方案（端点与本函数同批）下，它落地即有消费方
// （`internal/manager/http` 的 ack 端点映射 409），那个顾虑的前提本就不成立。
// ⇒ 一般化：哨兵的可见性跟着端点走，不跟着语义走。
var ErrEventAlreadyAcked = errors.New("repo: 事件已被他人确认")

// EventQuery 是事件检索的过滤条件与分页参数。
type EventQuery struct {
	Since, Until time.Time

	// NodeID/PipelineID 为 uuidx.Nil 表示不按该维度过滤。
	NodeID     uuidx.UUID
	PipelineID uuidx.UUID
	// Type/Level 为空字符串表示不按该维度过滤。
	Type  model.EventType
	Level model.EventLevel
	// 附一条查法上的坑：按中文措辞「待处理事件」搜 `web/` 命中 0（同目录
	// 搜「事件」命中 15 个文件，搜索路径是通的）。光凭这个 0 会得出「这个队列
	// 从未被建造」——而那个结论对 `internal/manager/model/node.go` 的「待清理」列表是真的，
	// 对这里是假的。前端用英文标识符，穿透它的是后端参数名（onlyUnacked），
	// 不是中文措辞。
	OnlyUnacked bool

	// Limit 是单页条数，<=0 时取 defaultEventQueryLimit，超过
	// maxEventQueryLimit 时截断为 maxEventQueryLimit。
	Limit int

	// BeforeTs/BeforeID 是游标分页的续页起点，语义与 LogQuery 同名字段
	// 一致：传入上一页最后一条记录的 (ts, id)，本次只返回比它更早的记录
	// （结果按 ts DESC, id DESC 排序）。翻页时必须同时传两者。
	BeforeTs time.Time
	BeforeID uuidx.UUID
}

// applyEventFilters 把 EventQuery 里"这条记录是否满足过滤条件"相关的
// 部分（可选时间范围 + 节点/pipeline/类型/级别/是否未确认）应用到 tx 上，
// 并做相应的参数校验。ListEvents 与 CountEvents 共用这一份逻辑，理由与
// log.go 的 applyLogFilters 相同：两套过滤条件拼接代码各自维护迟早会
// 漂移，届时会出现"列表 20 条、总数 35 条"这类从现象上很难联想到"两处
// WHERE 子句不一致"的 bug。
//
// 不包含 BeforeTs/BeforeID 游标条件，也不设置 Order/Limit：这些是
// ListEvents 分页专用的参数，与"总共有多少条满足过滤条件"无关。
func applyEventFilters(tx *gorm.DB, q EventQuery) (*gorm.DB, error) {
	tx = ReadResourceScope(tx.Statement.Context, tx, ReadEvent)
	if !q.Until.IsZero() && q.Until.Before(q.Since) {
		return nil, fmt.Errorf("repo: 事件检索的 Until 不能早于 Since")
	}
	if q.Type != "" && !q.Type.Valid() {
		return nil, fmt.Errorf("model: 未知的事件类型 %q", q.Type)
	}
	if q.Level != "" && !q.Level.Valid() {
		return nil, fmt.Errorf("model: 未知的事件级别 %q", q.Level)
	}

	if !q.Since.IsZero() {
		tx = tx.Where("ts >= ?", q.Since)
	}
	if !q.Until.IsZero() {
		tx = tx.Where("ts <= ?", q.Until)
	}
	if !q.NodeID.IsZero() {
		tx = tx.Where("node_id = ?", q.NodeID)
	}
	if !q.PipelineID.IsZero() {
		tx = tx.Where("pipeline_id = ?", q.PipelineID)
	}
	if q.Type != "" {
		tx = tx.Where("type = ?", q.Type)
	}
	if q.Level != "" {
		tx = tx.Where("level = ?", q.Level)
	}
	if q.OnlyUnacked {
		// 不能写成 `acked_by = ?` 绑定 uuidx.Nil：uuidx.UUID.Value() 对
		// 零值返回 (nil, nil)，也就是 SQL NULL，而 `column = NULL` 在 SQL
		// 里恒为 NULL（既不是 true 也不是 false），永远不会匹配任何行，
		// 包括 acked_by 本身就是 NULL 的行——必须显式用 IS NULL。
		tx = tx.Where("acked_by IS NULL")
	}
	return tx, nil
}

// ListEvents 按条件检索事件，按 ts 倒序（最新的在前）分页返回（需求
// MGR-MON-03）。
func ListEvents(ctx context.Context, db *gorm.DB, q EventQuery) ([]model.Event, error) {
	tx, err := applyEventFilters(db.WithContext(ctx).Model(&model.Event{}), q)
	if err != nil {
		return nil, err
	}

	limit := q.Limit
	switch {
	case limit <= 0:
		limit = defaultEventQueryLimit
	case limit > maxEventQueryLimit:
		limit = maxEventQueryLimit
	}

	if !q.BeforeTs.IsZero() {
		tx = tx.Where("(ts < ?) OR (ts = ? AND id < ?)", q.BeforeTs, q.BeforeTs, q.BeforeID)
	}

	var events []model.Event
	if err := tx.Order("ts DESC, id DESC").Limit(limit).Find(&events).Error; err != nil {
		return nil, fmt.Errorf("repo: 查询事件失败: %w", err)
	}
	return events, nil
}

func CountEvents(ctx context.Context, db *gorm.DB, q EventQuery) (int64, error) {
	tx, err := applyEventFilters(db.WithContext(ctx).Model(&model.Event{}), q)
	if err != nil {
		return 0, err
	}
	var count int64
	if err := tx.Count(&count).Error; err != nil {
		return 0, fmt.Errorf("repo: 统计事件总数失败: %w", err)
	}
	return count, nil
}

// AckEvent records the first acknowledgement with an atomic conditional update.
// Repeated acknowledgements preserve the original user and timestamp.
func AckEvent(ctx context.Context, db *gorm.DB, id uuidx.UUID, userID uuidx.UUID, now time.Time) error {
	if userID.IsZero() {
		return fmt.Errorf("repo: 确认事件的 userID 不能为空（零值 userID 会让 acked_by 停留在 NULL 而本函数却返回成功）")
	}
	if now.IsZero() {
		return fmt.Errorf("repo: 确认事件的 now 不能为零值（Go 零值时间超出 MySQL DATETIME 取值范围）")
	}

	res := db.WithContext(ctx).Model(&model.Event{}).
		Where("id = ? AND acked_by IS NULL", id).
		Updates(map[string]any{"acked_by": userID, "acked_at": now})
	if res.Error != nil {
		return fmt.Errorf("repo: 确认事件失败: %w", res.Error)
	}
	if res.RowsAffected == 1 {
		return nil
	}

	// CAS 未命中，三种来源，读一次 acked_by 消歧。
	var cur model.Event
	if err := db.WithContext(ctx).Select("id", "acked_by").Where("id = ?", id).Take(&cur).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrEventNotFound
		}
		return fmt.Errorf("repo: 校验事件确认状态失败: %w", err)
	}
	if cur.AckedBy == userID {
		// 同一个用户重复确认：按幂等处理，不报错，也不覆盖已有的 acked_at。
		return nil
	}
	return ErrEventAlreadyAcked
}

// insertEventsChunkSize 是 InsertEvents 单块插入的行数上限，取值与
// insertLogsChunkSize（internal/manager/repo/log.go）一致：每行带 payload（JSON），单块字节数
// 同样要避免顶到 max_allowed_packet。
const insertEventsChunkSize = 500

// InsertEvents 批量写入事件。
//
//   - `ingest`：`internal/manager/ingest/ctl_event.go`（单条 AMQP 事件）、`internal/manager/ingest/ctl_offline.go`（离线批量）、
//     `internal/manager/ingest/ctl_stats.go`（统计摘要），经 `internal/manager/ingest/ingest.go` 注入 `repo.InsertEvents`。
//   - `dispatch`：`internal/manager/dispatch/ctl_cmd_result.go`、`internal/manager/dispatch/ctl_config_apply_result.go`、
//     `internal/manager/dispatch/reconcile.go` 三处直接调用。
func InsertEvents(ctx context.Context, db *gorm.DB, rows []model.Event) error {
	if len(rows) == 0 {
		return nil
	}
	for start := 0; start < len(rows); start += insertEventsChunkSize {
		end := min(start+insertEventsChunkSize, len(rows))
		chunk := rows[start:end]
		for i := range chunk {
			if chunk[i].Ts.IsZero() {
				return fmt.Errorf("repo: 批量写入事件失败：第 %d 行 ts 为空（第 %d 行之前的块已提交，不回滚）", start+i, start)
			}
			if err := chunk[i].Validate(); err != nil {
				return fmt.Errorf("repo: 批量写入事件失败：第 %d 行校验未通过（第 %d 行之前的块已提交，不回滚）: %w", start+i, start, err)
			}
		}
		if err := db.WithContext(ctx).Create(&chunk).Error; err != nil {
			return fmt.Errorf("repo: 批量写入事件失败：第 %d~%d 行所在块写入出错（更早的块已提交，不回滚）: %w", start, end-1, err)
		}
	}
	return nil
}
