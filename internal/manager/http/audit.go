package http

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/xmapst/xdts/internal/manager/model"
)

// 绝不能出现密码、密码哈希、会话令牌；调用方也不能把它们塞进
// Reason 或 Changes。
type AuditEvent struct {
	UserID     string
	Username   string
	Action     model.AuditAction
	TargetType model.AuditTargetType
	TargetID   string
	Success    bool
	// Reason 是失败原因（如「用户不存在」「密码错误」「账号已被禁用」），
	// 只进日志/审计存储，绝不能通过 HTTP 响应回显给登录请求方——那样就等于
	// 用另一个渠道泄露了「账号是否存在」的旁路信息，和登录接口本身「不区分
	// 用户不存在与密码错误」的要求矛盾。
	Reason    string
	Changes   json.RawMessage
	ClientIP  string
	UserAgent string
}

// baseAuditEvent 组装一条成功审计事件的公共字段（当前操作人身份、来源
// IP/UA），调用方只需要补 Action/TargetType/TargetID 再传给
// h.audit.Record。取「Success: true」为默认值：所有调用点都是业务操作
// 已经成功执行之后才记审计，失败路径在到达这里之前已经用 Fail/
// FailInternal 提前返回、不会走到审计这一步（登录失败是例外，
// AuditActionLoginFailure 有自己的专门构造逻辑，不复用这个 helper）。
//
// 抽出来是因为 internal/manager/http/centers.go、internal/manager/http/nodes.go 与 internal/manager/http/register_codes.go 里同样的六行样板
// （UserID/Username/ClientIP/UserAgent 四个字段）会在这一批新增审计调用
// 点里重复十次以上；internal/manager/http/register_codes.go 里已有的两处保留原样不动，不做
// 无关重构。
func baseAuditEvent(c *gin.Context) AuditEvent {
	return AuditEvent{
		UserID:    UserIDFromContext(c),
		Username:  UsernameFromContext(c),
		Success:   true,
		ClientIP:  c.ClientIP(),
		UserAgent: c.Request.UserAgent(),
	}
}

// 验证落库版审计记录器是否已在生产装配点接线；不验证每个写 handler 是否
// 真的调用到它，也不验证落库是否成功。
type IAuditRecorder interface {
	Record(ctx context.Context, event AuditEvent)
}

type SlogAuditRecorder struct{}

var _ IAuditRecorder = SlogAuditRecorder{}

func (SlogAuditRecorder) Record(ctx context.Context, event AuditEvent) {
	slog.LogAttrs(ctx, slog.LevelInfo, "audit event",
		slog.String("user_id", event.UserID),
		slog.String("username", event.Username),
		slog.String("action", string(event.Action)),
		slog.String("target_type", string(event.TargetType)),
		slog.String("target_id", event.TargetID),
		slog.Bool("success", event.Success),
		slog.String("reason", event.Reason),
		slog.String("client_ip", event.ClientIP),
	)
}

// MemoryAuditRecorder 把审计事件保存在内存里，供测试断言「确实写了审计」
// 使用；不用于生产（生产默认使用数据库审计记录器）。
type MemoryAuditRecorder struct {
	mu     sync.Mutex
	events []AuditEvent
}

// NewMemoryAuditRecorder 构造一个内存审计记录器。
func NewMemoryAuditRecorder() *MemoryAuditRecorder {
	return &MemoryAuditRecorder{}
}

var _ IAuditRecorder = (*MemoryAuditRecorder)(nil)

func (r *MemoryAuditRecorder) Record(_ context.Context, event AuditEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

// Events 返回目前记录到的全部审计事件的副本，供测试断言。
func (r *MemoryAuditRecorder) Events() []AuditEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]AuditEvent, len(r.events))
	copy(out, r.events)
	return out
}
