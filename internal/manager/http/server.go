package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/auth"
	"github.com/xmapst/xdts/internal/manager/dispatch"
)

// Options 是构造 Server 所需的最小依赖。
type Options struct {
	// Listen 是 HTTP 监听地址，如 ":8080"。
	Listen string
	// ShutdownTimeout 是 Shutdown 等待在途请求排空的最长时间。
	ShutdownTimeout time.Duration
	// Sessions 是会话存储；为 nil 时使用 auth.NewMemorySessionStore()。
	Sessions auth.ISessionStore
	// Users 是用户存储；为 nil 时使用 auth.NewMemoryUserStore()（此时不会
	// 预置任何用户，本地/LDAP/OIDC 登录 handler 就绪前没有人能登录成功，
	// 这是预期行为，不是 bug）。
	Users auth.IUserStore

	LocalUsers auth.ILocalUserStore
	// Argon2Params 是本地账号密码哈希/校验使用的 argon2id 参数，来自
	// cfg.Auth.Local.Argon2。为零值（Argon2Params{}）时使用
	// auth.DefaultArgon2Params()。
	Argon2Params auth.Argon2Params
	// SessionTTL 是登录成功后签发的会话 / CSRF Cookie 有效期，来自
	// cfg.Auth.SessionTTL。<= 0 时使用一个兜底默认值（见 internal/manager/http/auth_local.go 的
	// defaultSessionTTL），避免遗漏配置时签出 Max-Age=0 的 Cookie。
	SessionTTL time.Duration
	// AuthMethods 描述当前启用的登录方式，来自
	// cfg.Auth.{Local,LDAP,OIDC}.Enabled，供 GET /auth/me 告知前端登录页
	// 展示哪些入口。
	AuthMethods LoginMethods
	// LDAP 配置独立传入，不让 HTTP 层依赖进程完整配置。
	LDAPConfig auth.LDAPConfig
	OIDCConfig auth.OIDCConfig

	Audit IAuditRecorder

	Lockouts auth.ILockoutStore
	// LockoutPolicy 是连续失败锁定的阈值/窗口/锁定时长，来自
	// cfg.Auth.Lockout。为零值（LockoutPolicy{}）时使用
	// auth.DefaultLockoutPolicy()。
	LockoutPolicy auth.LockoutPolicy
	// CookieSecure 控制会话 / CSRF Cookie 的 Secure 属性，来自
	// cfg.HTTP.CookieSecure。
	CookieSecure bool
	// DB 是管理库连接。cmd/manager 在构造 HTTP Server 前完成连接和迁移。
	// 独立鉴权测试可不提供 DB；业务资源请求通过 requireDB 返回 503。
	DB *gorm.DB
	// LogRetention 是日志保留期，来自 cfg.Retention.Logs
	// （config.RetentionConfig.Logs）。GET /logs 的时间范围上限取它
	// 与内部查询成本硬上限（internal/manager/http/logs.go 的 maxLogWindowCostCap）两者的较小值
	// ——不是只看这一个字段，见 internal/manager/http/logs.go 顶部注释里"两种不同约束"的说明。
	// 这里只传一个 time.Duration，不传整个 config.Config/RetentionConfig：
	// internal/manager/http 目前不依赖 internal/manager/config 包，没有
	// 理由为了这一个字段引入这层依赖。
	//
	// <= 0 视为未配置，退回只用内部硬上限（config.Load 已经校验
	// retention.logs 必须为正，正常路径不会出现，这里只是防御性兜底，不是
	// 期望的运行路径）。
	LogRetention    time.Duration
	RuntimeSettings bool
	// Dispatcher synchronizes persisted changes and publishes configuration notifications.
	Dispatcher dispatch.IPipelineDispatcher
	// CleanupWake schedules a serialized resource scan after deletion or retry.
	CleanupWake func()
	// EndpointProber 接收手动端点探测任务；AMQP 未接通时返回服务不可用。
	EndpointProber IEndpointProber
	// BinlogServerIDRangeMin/Max 是 create pipeline 时分配 dump 用
	// server_id（MGR-PL-06）的号段，来自 cfg.Pipeline.
	// BinlogServerIDRangeMin/Max。与 LogRetention 同样的理由：这里只传两个
	// uint32 原始值，不传 config.PipelineConfig 整个结构体——
	// internal/manager/http 不依赖 internal/manager/config 包。
	BinlogServerIDRangeMin uint32
	BinlogServerIDRangeMax uint32
}

// Server 包装 gin.Engine 与 http.Server，提供启动与优雅关闭。
type Server struct {
	engine          *gin.Engine
	api             *gin.RouterGroup
	httpServer      *http.Server
	shutdownTimeout time.Duration
	sessions        auth.ISessionStore
	users           auth.IUserStore
	localUsers      auth.ILocalUserStore
	audit           IAuditRecorder
	lockouts        auth.ILockoutStore
	cookieSecure    bool
	db              *gorm.DB
	dispatcher      dispatch.IPipelineDispatcher
	oidc            *auth.OIDCAuthenticator
}

// New 按 release 模式装配 gin 引擎：请求 ID → 访问日志 → Recovery 三层通用
// 中间件，健康检查，/api/v1 路由组（CSRF → 会话鉴权 → 首次改密闸门 → 只读角色闸门），以及
// 内嵌前端资源的 SPA 回退（需求 C-20、MGR-WEB-04、MGR-USR-04/05/08）。
func New(opts Options) (*Server, error) {
	// 号段是必填契约，不属于下面那一族"可选依赖缺省时给个内存实现"的零值
	// 兜底：那些兜底换来的是一个语义完整的默认对象，而号段一旦回落就变成
	// "分配出去的 server_id 来自哪份副本说不清"。见 Options 的字段文档。
	if opts.BinlogServerIDRangeMin == 0 || opts.BinlogServerIDRangeMax == 0 {
		return nil, fmt.Errorf(
			"http: Options.BinlogServerIDRangeMin/Max 必须非零（当前 min=%d max=%d）；生产路径由 config.PipelineConfig 提供，测试用 withTestBinlogServerIDRange 补齐",
			opts.BinlogServerIDRangeMin, opts.BinlogServerIDRangeMax)
	}

	sessions := opts.Sessions
	if sessions == nil {
		sessions = auth.NewMemorySessionStore()
	}
	users := opts.Users
	if users == nil {
		users = auth.NewMemoryUserStore()
	}
	localUsers := opts.LocalUsers
	if localUsers == nil {
		localUsers = auth.NewMemoryLocalUserStore()
	}
	audit := opts.Audit
	if audit == nil {
		audit = SlogAuditRecorder{}
	}
	lockouts := opts.Lockouts
	if lockouts == nil {
		lockouts = auth.NewMemoryLockoutStore()
	}
	dispatcher := opts.Dispatcher
	if dispatcher == nil {
		dispatcher = noopPipelineDispatcher{}
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(RequestID(), AccessLog(), Recovery())

	engine.GET("/healthz", healthHandler)

	api := engine.Group("/api/v1")
	// 中间件顺序固定：CSRF（登录接口豁免）→ 会话鉴权（登录/OIDC 起始与回调、
	// /auth/me 豁免，写入 ctxKeyRole）→ 首次改密闸门 → 只读角色闸门（依赖 ctxKeyRole，默认
	// 拒绝写方法，显式放行走 MarkReadOnlySafe）。调换顺序会让后两层拿不到
	// 需要的前置状态；豁免路径的取舍见 internal/manager/http/csrf.go 里 LoginPath 等常量、
	// internal/manager/http/auth_local.go 里 MeAuthPath 的注释。
	api.Use(
		CSRF(LoginPath),
		SessionAuth(sessions, users, sessionAuthExemptPaths()...),
		RequirePasswordChange(),
		DenyReadOnlyWrites(),
		ResourceScopeGate(opts.DB),
	)
	// 各业务模块（端点、策略、pipeline、中心、节点、日志/事件/冲突等观测
	// 端点）的子路由依次通过 Server.API() 挂到这个 group 上（
	// HTTP API 概览，见下方 register*Routes 调用），均自动获得上面三层
	// 保护。/api/v1/* 未匹配的请求会落到下面 registerStatic 注册的
	// NoRoute，按 JSON 404 处理。
	localAuth, err := newLocalAuthHandler(sessions, users, localUsers, opts, audit, lockouts)
	if err != nil {
		return nil, err
	}

	// 下面每个登录方式的挂载分支都以 implementedLoginMethods 为条件——
	// 它是"本二进制能做什么"的**唯一**来源，cmd/manager 的启动检查读的是
	// 同一个它（见 internal/manager/http/auth_local.go 上那段"为什么必须只有这一份"）。
	//
	// 注意 implementedLoginMethods 与 opts.AuthMethods 是两件事：这里判的是
	// **实现**（路由存不存在），handleLogin 里判的是 **配置**（运维启没启用）。
	// 两层都要，缺任一层都会出现"看得见入口但用不了"或"配了却没入口"。
	if implementedLoginMethods.Local || implementedLoginMethods.LDAP {
		api.POST("/auth/login", localAuth.handleLogin)
	}

	var oidcProvider *auth.OIDCAuthenticator
	if implementedLoginMethods.OIDC {
		oidcAuth := &oidcAuthHandler{login: localAuth, configured: opts.AuthMethods.OIDC}
		if opts.AuthMethods.OIDC {
			if opts.DB == nil {
				return nil, errors.New("http: OIDC 登录需要管理库")
			}
			oidcAuth.provider, err = auth.NewOIDCAuthenticator(context.Background(), opts.OIDCConfig)
			if err != nil {
				return nil, err
			}
			oidcProvider = oidcAuth.provider
		}
		api.GET("/auth/oidc/start", oidcAuth.start)
		api.GET("/auth/oidc/callback", oidcAuth.callback)
	}

	// /auth/logout 与 /auth/me 是**会话**端点，不属于任何单一登录方式：
	// 无论会话是本地、LDAP 还是 OIDC 签发的，登出与"我是谁"都要能用。
	// 因此它们不挂在上面的分支里，保持无条件注册。
	api.POST("/auth/logout", localAuth.handleLogout)
	MarkReadOnlySafe(http.MethodPost, "/api/v1/auth/logout")
	api.GET("/auth/me", localAuth.handleMe)
	MarkReadOnlySafe(http.MethodPut, "/api/v1/auth/password")
	api.PUT("/auth/password", localAuth.handleChangePassword)

	registerNodeRoutes(api, opts.DB, audit)
	registerUserRoutes(api, opts.DB, audit, opts.Argon2Params, opts.LockoutPolicy)
	registerCenterRoutes(api, opts.DB, audit, dispatcher)
	registerRegisterCodeRoutes(api, opts.DB, audit)
	registerLogRoutes(api, opts.DB, opts.LogRetention, opts.RuntimeSettings)
	registerEventRoutes(api, opts.DB, audit)
	registerConflictRoutes(api, opts.DB)
	registerStrategyRoutes(api, opts.DB, audit)
	registerEndpointRoutes(api, opts.DB, audit, opts.EndpointProber)
	registerPipelineRoutes(api, opts.DB, audit, dispatcher, opts.BinlogServerIDRangeMin, opts.BinlogServerIDRangeMax)
	registerPipelineCleanupRoutes(api, opts.DB, audit, opts.CleanupWake)
	registerPrecheckReadRoutes(api, opts.DB, opts.EndpointProber)
	registerDataCheckRoutes(api, opts.DB, opts.EndpointProber, audit)
	registerSchemaPlanRoutes(api, opts.DB, opts.EndpointProber, audit, dispatcher)
	registerSnapshotRoutes(api, opts.DB)
	registerMetricsRoutes(api, opts.DB)
	registerAlertRoutes(api, opts.DB, audit)
	registerAuditQueryRoutes(api, opts.DB)
	registerSettingsRoutes(api, opts.DB)
	registerDashboardRoutes(api, opts.DB)
	registerReinitRoutes(api, opts.DB, opts.EndpointProber)
	registerPositionRoutes(api, opts.DB, opts.EndpointProber, audit, dispatcher)

	if err := registerStatic(engine); err != nil {
		if oidcProvider != nil {
			oidcProvider.Close()
		}
		return nil, err
	}

	return &Server{
		oidc:   oidcProvider,
		engine: engine,
		api:    api,
		httpServer: &http.Server{
			Addr:              opts.Listen,
			Handler:           engine,
			ReadHeaderTimeout: 10 * time.Second,
		},
		shutdownTimeout: opts.ShutdownTimeout,
		sessions:        sessions,
		users:           users,
		localUsers:      localUsers,
		audit:           audit,
		lockouts:        lockouts,
		cookieSecure:    opts.CookieSecure,
		db:              opts.DB,
		dispatcher:      dispatcher,
	}, nil
}

// Engine 暴露底层 gin.Engine，供测试用 httptest 直接发请求。
func (s *Server) Engine() *gin.Engine { return s.engine }

// API 暴露 /api/v1 路由组（已经挂好 CSRF/会话鉴权/只读角色闸门），供业务
// 模块注册子路由、供测试注册路由做端到端验证。不要绕过它直接在 Engine()
// 上注册 /api/v1/* 路由，否则会跳过这三层保护。
func (s *Server) API() *gin.RouterGroup { return s.api }

// Sessions 暴露会话存储，供登录/登出 handler 签发或撤销会话。
func (s *Server) Sessions() auth.ISessionStore { return s.sessions }

// Users 暴露用户存储，供登录 handler 校验账号、供用户管理接口读写。
func (s *Server) Users() auth.IUserStore { return s.users }

// LocalUsers 暴露本地账号凭据存储，供测试使用；后续若新增账号管理端点，
// 也会用到它。
func (s *Server) LocalUsers() auth.ILocalUserStore { return s.localUsers }

// Audit 暴露审计记录器，供测试断言审计事件确实被记录。
func (s *Server) Audit() IAuditRecorder { return s.audit }

// Lockouts 暴露登录失败锁定状态存储，供测试与运维排障读写。
func (s *Server) Lockouts() auth.ILockoutStore { return s.lockouts }

// DB 暴露管理库连接，供后台任务（回收循环等）与测试直接操作。可能为 nil
// （Options.DB 未设置时），调用方需自行判断。
func (s *Server) DB() *gorm.DB { return s.db }

func (s *Server) Dispatcher() dispatch.IPipelineDispatcher { return s.dispatcher }

// CookieSecure 报告会话 / CSRF Cookie 是否应该带 Secure 属性。
func (s *Server) CookieSecure() bool { return s.cookieSecure }

// Start 以非阻塞方式启动 HTTP 监听。监听失败（如端口被占用）会通过返回的
// channel 上报一次；调用 Shutdown 触发的正常退出不会往这个 channel 发送
// 任何东西。
func (s *Server) Start() <-chan error {
	errCh := make(chan error, 1)
	go func() {
		slog.Info("manager http server started", slog.String("listen", s.httpServer.Addr))
		if err := s.httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http: 监听失败: %w", err)
		}
	}()
	return errCh
}

// Shutdown 优雅关闭 HTTP 服务，等待在途请求排空，超时由 Options.ShutdownTimeout
// 控制（需求 MGR-WEB-04：支持优雅关闭）。
func (s *Server) Shutdown(ctx context.Context) error {
	if s.oidc != nil {
		defer s.oidc.Close()
	}
	shutdownCtx, cancel := context.WithTimeout(ctx, s.shutdownTimeout)
	defer cancel()
	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("http: 优雅关闭失败: %w", err)
	}
	return nil
}
