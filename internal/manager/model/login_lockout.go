package model

import (
	"fmt"
	"time"
)

// LoginLockoutScope 是登录失败计数的维度（需求 MGR-USR-01a）。
//
// 必须按「用户名」与「客户端 IP」两个独立维度分别计数：只按用户名计数
// 挡不住同一 IP 对大量账号各试少数几次的密码喷洒，只按 IP 计数则挡不住
// 从多个 IP 集中攻击单一账号。任一维度超阈值都要拒绝登录。
type LoginLockoutScope string

const (
	LoginLockoutScopeUsername LoginLockoutScope = "username"
	LoginLockoutScopeClientIP LoginLockoutScope = "client_ip"
)

// Valid 校验维度取值。
func (s LoginLockoutScope) Valid() bool {
	switch s {
	case LoginLockoutScopeUsername, LoginLockoutScopeClientIP:
		return true
	default:
		return false
	}
}

// LoginLockout 是登录失败计数与锁定记录（需求
// MGR-USR-01a）。(scope, subject) 唯一：subject 在 scope=username 时是
// 用户名，scope=client_ip 时是客户端 IP。
//
// 判定流程：登录请求到达 → 查 (username, 该用户名) 与
// (client_ip, 该 IP) 两条记录 → 任一条 locked_until > now 即拒绝 →
// 否则校验口令 → 成功清零两条记录，失败两条各自 +1、超阈值置
// locked_until。
//
// 安全约束（需求 MGR-USR-01b）：账号被锁定时必须返回与其它失败情形完全
// 相同的通用错误，不得告知调用方「账号已被锁定」——这是调用方（HTTP
// 登录 handler）的职责，本层只负责存取这些字段，不做任何错误信息拼接。
type LoginLockout struct {
	Base

	Scope   LoginLockoutScope `gorm:"column:scope;type:varchar(16);not null;uniqueIndex:uk_login_lockouts_scope_subject,priority:1" json:"scope"`
	Subject string            `gorm:"column:subject;type:varchar(255);not null;uniqueIndex:uk_login_lockouts_scope_subject,priority:2" json:"subject"`

	FailureCount int `gorm:"column:failure_count;not null;default:0" json:"failure_count"`
	// WindowStartedAt 是当前计数窗口的起点，超出窗口长度即视为过期，下一
	// 次失败会重新开始计数（不是在旧计数上继续累加）。
	WindowStartedAt time.Time `gorm:"column:window_started_at;not null" json:"window_started_at"`
	// LockedUntil 为 nil 表示未锁定；非 nil 且晚于当前时间表示仍处于锁定
	// 期。用指针而非零值 time.Time，避免 Go 零值（0001-01-01）落库时超出
	// MySQL DATETIME 取值范围报错。
	LockedUntil *time.Time `gorm:"column:locked_until;index:idx_login_lockouts_locked_until" json:"locked_until"`
}

// TableName 显式指定表名。
func (LoginLockout) TableName() string { return "login_lockouts" }

// Validate 校验枚举字段。
func (l *LoginLockout) Validate() error {
	if !l.Scope.Valid() {
		return fmt.Errorf("model: 未知的登录锁定维度 %q", l.Scope)
	}
	return nil
}

// Locked 判定该记录在 now 时刻是否处于锁定状态。
func (l LoginLockout) Locked(now time.Time) bool {
	return l.LockedUntil != nil && l.LockedUntil.After(now)
}
