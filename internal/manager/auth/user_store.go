package auth

import (
	"context"
	"errors"
	"sync"
)

// ErrUserNotFound 表示按 ID 找不到用户。
var ErrUserNotFound = errors.New("auth: 用户不存在")

// User 是鉴权中间件需要的用户最小视图。密码哈希、LDAP/OIDC 身份映射等属于
// 本地/外部登录（MGR-USR-01/02/03，另外的任务）的实现细节，不放在这个
// 通用结构体里，避免把具体登录方式的细节泄漏到会话/角色中间件层。
type User struct {
	ID                 string
	Username           string
	Role               Role
	Disabled           bool // 账号被禁用后，即使持有未过期的会话令牌也应当被拒绝
	MustChangePassword bool
}

// SessionAuth 中间件用它在每次请求时取「当前」角色与启用状态，而不是只信
// 任签发会话时缓存在 Session.Role 里的快照：这样管理员改角色或禁用账号能
// 立刻生效，不用等旧会话自然过期。
type IUserStore interface {
	GetByID(ctx context.Context, id string) (User, error)
	// Create 写入一条新用户记录。调用方保证 ID 唯一；具体的重复处理方式
	// 由实现决定——内存实现直接覆盖，落库实现应该在主键/唯一索引冲突时
	// 返回错误，而不是静默覆盖已有账号。
	//
	// 落库实现 `http.dbUserStore.Create` 今天无条件返回
	// ErrUserStoreCreateNotImplemented；为什么这样做、以及真正实现时要先
	// 解决什么契约缺口，只记在那一处，本处不复述（避免同一结论四处写全、
	// 改动时三处变成复述）。
	Create(ctx context.Context, user User) error
}

// MemoryUserStore 是 IUserStore 的内存实现，供独立测试使用（真实的
// 本地/LDAP/OIDC 登录落地前，这里不会有生产数据）。
type MemoryUserStore struct {
	mu    sync.RWMutex
	users map[string]User
}

// NewMemoryUserStore 构造一个内存用户存储。
func NewMemoryUserStore() *MemoryUserStore {
	return &MemoryUserStore{users: make(map[string]User)}
}

var _ IUserStore = (*MemoryUserStore)(nil)

// Put 写入或替换一个用户记录，供测试与临时联调使用。
func (s *MemoryUserStore) Put(u User) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[u.ID] = u
}

// Delete 删除一个用户记录，供测试使用。
func (s *MemoryUserStore) Delete(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.users, id)
}

func (s *MemoryUserStore) GetByID(_ context.Context, id string) (User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return User{}, ErrUserNotFound
	}
	return u, nil
}

func (s *MemoryUserStore) Create(_ context.Context, user User) error {
	s.Put(user)
	return nil
}
