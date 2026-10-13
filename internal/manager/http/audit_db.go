package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// DBAuditRecorder 将关键操作写入 audit_logs，提供独立于日志轮转的审计记录。
// 管理库连接成功后构造；实现统一的 IAuditRecorder 接口。
type DBAuditRecorder struct {
	db *gorm.DB
}

// NewDBAuditRecorder 构造落库版审计记录器。
func NewDBAuditRecorder(db *gorm.DB) *DBAuditRecorder {
	return &DBAuditRecorder{db: db}
}

var _ IAuditRecorder = (*DBAuditRecorder)(nil)

// auditDetail 是落进 audit_logs.detail 的补充信息：AuditEvent 里除了
// Action/TargetType/TargetID/UserID 之外、没有专门列去承载但审计排查时
// 有用的字段（谁登录的、成功与否、失败原因、来源 IP/UA）。这些字段本身
// 就不含密码/令牌等敏感数据（AuditEvent 的文档注释已经约束调用方不得把
// 这类内容塞进 Reason），落库不违反「敏感数据不得写日志」的约束。
//
// Username 的存在本身也是这次修复依赖的一个既有事实：审计记录「谁做的」
// 这个人类可读信息从一开始就不指望靠 join user_id 拿到——所以下面
// auditUnknownUserID 这个占位符不会让任何现有/规划中的展示逻辑失真
// （web/src/api/audit.ts 的 AuditLogEntry.actor 同样是独立字符串字段，
// 不是 user_id）。
type auditDetail struct {
	Username  string          `json:"username"`
	Success   bool            `json:"success"`
	Reason    string          `json:"reason,omitempty"`
	ClientIP  string          `json:"client_ip,omitempty"`
	UserAgent string          `json:"user_agent,omitempty"`
	Changes   json.RawMessage `json:"changes,omitempty"`
}

// auditUnknownUserID 是「审计事件没有可归属的已登录用户」时落库用的占位
// UserID。
//
// 取一个非零、任何真实用户 UUID 几乎不可能撞上的固定占位值——真实用户 ID
// 来自 uuidx.New()（UUIDv7，含时间戳与随机比特），不会产出这种全字段规整
// 的字面量。
var auditUnknownUserID = uuidx.MustParse("00000000-0000-0000-0000-000000000001")

// Record 实现 IAuditRecorder。
//
// 非空但解析失败（不是合法 UUID）单独处理，同样落 auditUnknownUserID、但
// 额外记一条 Error 日志：这种取值不该出现——当前所有生产调用点要么用
// UserIDFromContext(c) 或已登录凭据的 UUID（合法值，系统不变量要求
// SessionAuth 校验过的用户 ID 都是 auth.BootstrapAdmin/注册流程产出的合法
// UUID），要么（仅登录失败一处）完全不设 UserID（空字符串）；「非空但解析
// 失败」不属于这两种已知形态中的任何一种，值得被日志监控捕捉、提示调用方
// 出了新的编程错误，但审计记录本身仍然保留——不能因为「值得警惕」就换成
// 「丢弃」，那正是这次要修的问题。
//
// TargetID 解析失败时退化为 uuidx.Nil（不是 auditUnknownUserID）：该列
// 可为 NULL（不是所有审计动作都有明确的单一目标），Value() 把零值转成
// SQL NULL 在这里正是我们想要的行为，不是陷阱——「记录下动作发生过、但
// 目标身份解析失败」比放弃整条记录更有审计价值。
func (r *DBAuditRecorder) Record(ctx context.Context, event AuditEvent) {
	userID := auditUnknownUserID
	switch event.UserID {
	case "":
		// 按设计没有已知操作人，不是错误，不额外记日志——见上面文档注释。
	default:
		parsed, err := uuidx.Parse(event.UserID)
		switch {
		case err != nil:
			slog.LogAttrs(ctx, slog.LevelError, "audit user id invalid, recorded as unknown",
				slog.String("action", string(event.Action)),
				slog.String("user_id", event.UserID),
			)
		case parsed.IsZero():
			// 显式传了字面量零值 UUID：解析本身没有失败，但落库同样会撞
			// auditUnknownUserID 文档注释里说的 Value() 陷阱，且这不是
			// 「调用方传了垃圾」的信号（零值 UUID 是合法 UUID），不额外
			// 记错误日志，按「没有已知操作人」处理即可。
		default:
			userID = parsed
		}
	}
	targetID, err := uuidx.Parse(event.TargetID)
	if err != nil {
		targetID = uuidx.Nil
	}

	detail, err := json.Marshal(auditDetail{
		Username:  event.Username,
		Success:   event.Success,
		Reason:    event.Reason,
		ClientIP:  event.ClientIP,
		UserAgent: event.UserAgent,
		Changes:   event.Changes,
	})
	if err != nil {
		// 结构体全是字符串/布尔字段，编码失败极不可能发生；即便发生也
		// 退化为不带 detail 落库，而不是放弃整条审计记录。
		slog.LogAttrs(ctx, slog.LevelError, "audit detail encode failed",
			slog.String("action", string(event.Action)), slog.Any("error", err))
		detail = nil
	}

	log := &model.AuditLog{
		UserID:     userID,
		Action:     event.Action,
		TargetType: event.TargetType,
		TargetID:   targetID,
		Detail:     model.JSON(detail),
	}
	if err := repo.CreateAuditLog(ctx, r.db, log, time.Now()); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "audit record write failed",
			slog.String("action", string(event.Action)),
			slog.String("target_type", string(event.TargetType)),
			slog.String("target_id", event.TargetID),
			slog.Any("error", err),
		)
	}
}
