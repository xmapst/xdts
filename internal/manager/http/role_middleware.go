package http

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"

	"github.com/xmapst/xdts/internal/manager/auth"
)

// RequireRole 要求当前用户的角色在允许集合里，否则 403。
//
// 用于比「非只读」更具体的限制——例如 MGR-USR-08 提到的「只读角色不得访问
// 任何返回凭据、注册码明文的接口」：这类接口即使是 GET（不会被
// DenyReadOnlyWrites 拦截），也要单独挂 RequireRole(auth.RoleAdmin)。
// internal/manager/http/register_codes.go 的 POST /register-codes（本文件里唯一会返回明文注册码
// 的接口）已经这样挂了；后续如果出现其他同类接口，同样按需挂上即可，不需要
// 改这个文件。
//
// 必须排在 SessionAuth 之后（依赖 ctxKeyRole 已经写入 context）。
func RequireRole(roles ...auth.Role) gin.HandlerFunc {
	allowed := make(map[auth.Role]bool, len(roles))
	for _, r := range roles {
		allowed[r] = true
	}
	return func(c *gin.Context) {
		role, ok := RoleFromContext(c)
		if !ok || !allowed[role] {
			respondError(c, http.StatusForbidden, "forbidden", "没有权限执行此操作", nil)
			c.Abort()
			return
		}
		c.Next()
	}
}

// readOnlySafeRoutes explicitly allows write-method routes for read-only roles.
// Unregistered routes are denied.
var readOnlySafeRoutes = struct {
	mu sync.RWMutex
	m  map[string]bool
}{m: make(map[string]bool)}

func readOnlySafeKey(method, path string) string { return method + " " + path }

// MarkReadOnlySafe 显式登记一条「写方法但对只读角色安全」的路由。
func MarkReadOnlySafe(method, path string) {
	readOnlySafeRoutes.mu.Lock()
	readOnlySafeRoutes.m[readOnlySafeKey(method, path)] = true
	readOnlySafeRoutes.mu.Unlock()
}

func isReadOnlySafe(method, path string) bool {
	readOnlySafeRoutes.mu.RLock()
	defer readOnlySafeRoutes.mu.RUnlock()
	return readOnlySafeRoutes.m[readOnlySafeKey(method, path)]
}

// DenyReadOnlyWrites 是「默认拒绝、显式放行」的全局闸门（需求 MGR-USR-08：
// 「所有写接口对只读角色返回 403」）：只读角色对任何非幂等方法
// （POST/PUT/PATCH/DELETE）一律 403，除非该路由被 MarkReadOnlySafe 显式
// 登记过。
//
// 挂在 /api/v1 分组级中间件上，而不是让每个 handler 自己判断角色：新增的
// 业务路由天然被覆盖，不存在「某个 handler 忘了加只读校验」这种漏洞——这
// 正是团队要求的「默认拒绝、显式放行」而非「默认放行、逐个拦截」。
//
// 必须排在 SessionAuth 之后（依赖 ctxKeyRole 已经写入 context）。
func DenyReadOnlyWrites() gin.HandlerFunc {
	return func(c *gin.Context) {
		if idempotentMethods[c.Request.Method] {
			c.Next()
			return
		}
		role, ok := RoleFromContext(c)
		if !ok {
			// 未认证请求走到这里有两种情况：① SessionAuth 本该拦截但配置
			// 有误（防御性放行，交由后续中间件/handler 兜底）；② 请求命中
			// 了 SessionAuth 的豁免路径（登录/OIDC 起始与回调），这些端点
			// 本来就没有角色概念，不归只读闸门管。
			c.Next()
			return
		}
		if role != auth.RoleReadOnly {
			c.Next()
			return
		}
		if isReadOnlySafe(c.Request.Method, c.FullPath()) {
			c.Next()
			return
		}
		respondError(c, http.StatusForbidden, "readonly_forbidden", "只读角色不允许执行写操作", nil)
		c.Abort()
	}
}
