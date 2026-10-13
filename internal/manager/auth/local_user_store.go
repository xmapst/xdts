package auth

import (
	"context"
	"errors"
	"sync"
)

// ErrLocalUserNotFound 表示按用户名找不到本地账号。
//
// 本地登录 handler 必须把这个错误和「密码错误」一视同仁地处理成同一个对外
// 错误（不区分「用户不存在」与「密码错误」，避免给攻击者提供账号是否存在
// 的旁路信息）——这条规则由调用方（http 层的登录 handler）负责落实，本
// 文件只负责查询本身。
var ErrLocalUserNotFound = errors.New("auth: 本地账号不存在")

// LocalCredential 是本地账号登录校验所需的最小视图。
//
// 不复用 internal/manager/auth/user_store.go 里鉴权中间件用的通用 User 结构体：PasswordHash 是
// 本地登录特有的字段，混进 SessionAuth/RequireRole 依赖的通用 User 里会把
// 具体登录方式的实现细节泄漏到会话/角色中间件层（User 类型定义处的注释
// 已经讲过这条设计原则），LocalCredential 只给本地登录 handler 用。
type LocalCredential struct {
	UserID             string
	Username           string
	PasswordHash       string // HashPassword 产生的编码字符串，不是明文
	Role               Role
	Disabled           bool
	MustChangePassword bool
}

// 初始管理员的创建（首次启动按配置创建，需求 MGR-USR-01）与账号管理相关的
// HTTP 端点（增删改本地账号）已由 internal/manager/http/users.go 实现；本接口只覆盖
// 登录时需要的只读查询。
type ILocalUserStore interface {
	// GetByUsername 按用户名查本地账号凭据；不存在时返回
	// ErrLocalUserNotFound。
	GetByUsername(ctx context.Context, username string) (LocalCredential, error)
	// Any 报告是否存在至少一条本地账号凭据，供 BootstrapAdmin（internal/manager/auth/bootstrap.go）
	// 判断「本地账号表是否为空」——只有为空时才创建初始管理员（需求
	// MGR-USR-01c），避免每次启动都重复创建、或覆盖已经存在的账号（不管
	// 这些账号是当初 bootstrap 创建的，还是以后有了账号管理端点之后建的）。
	Any(ctx context.Context) (bool, error)
	// Create 写入一条新的本地账号凭据（目前只有 BootstrapAdmin 会调用
	// 它）。调用方保证 Username 唯一；具体的重复处理方式由实现决定——
	// 内存实现直接覆盖，未来落库实现应该在唯一索引冲突时返回错误。
	Create(ctx context.Context, cred LocalCredential) error
}

// MemoryLocalUserStore 是 ILocalUserStore 的内存实现，供独立测试
// 使用。真实的本地账号数据来自初始管理员创建，以及以后可能新增的账号
// 管理端点，骨架阶段这里不会预置任何生产数据。
type MemoryLocalUserStore struct {
	mu     sync.RWMutex
	byUser map[string]LocalCredential
}

// NewMemoryLocalUserStore 构造一个内存本地账号凭据存储。
func NewMemoryLocalUserStore() *MemoryLocalUserStore {
	return &MemoryLocalUserStore{byUser: make(map[string]LocalCredential)}
}

var _ ILocalUserStore = (*MemoryLocalUserStore)(nil)

// Put 写入或替换一条本地账号凭据，供测试与临时联调使用。
func (s *MemoryLocalUserStore) Put(cred LocalCredential) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byUser[cred.Username] = cred
}

// Delete 删除一条本地账号凭据，供测试使用。
func (s *MemoryLocalUserStore) Delete(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byUser, username)
}

func (s *MemoryLocalUserStore) GetByUsername(_ context.Context, username string) (LocalCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cred, ok := s.byUser[username]
	if !ok {
		return LocalCredential{}, ErrLocalUserNotFound
	}
	return cred, nil
}

func (s *MemoryLocalUserStore) Any(_ context.Context) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.byUser) > 0, nil
}

func (s *MemoryLocalUserStore) Create(_ context.Context, cred LocalCredential) error {
	s.Put(cred)
	return nil
}
