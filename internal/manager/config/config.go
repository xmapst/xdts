// 配置字段由 YAML、环境变量和命令行合并，在启动前统一校验。
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/manager/auth"
)

// Config 是 Manager 进程的完整配置视图。
type Config struct {
	HTTP            HTTPConfig            `mapstructure:"http"`
	Database        DatabaseConfig        `mapstructure:"database"`
	AMQP            AMQPConfig            `mapstructure:"amqp"`
	Auth            AuthConfig            `mapstructure:"auth"`
	Dispatch        DispatchConfig        `mapstructure:"dispatch"`
	BackgroundTasks BackgroundTasksConfig `mapstructure:"background_tasks"`
	Pipeline        PipelineConfig        `mapstructure:"pipeline"`
	Log             LogConfig             `mapstructure:"log"`
	Retention       RetentionConfig       `mapstructure:"retention"`
}

// HTTPConfig 对应 manager.yaml 的 http 段。
type HTTPConfig struct {
	Listen          string        `mapstructure:"listen"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
	CookieSecure    bool          `mapstructure:"cookie_secure"`
}

// DatabaseConfig 对应 manager.yaml 的 database 段：管理库连接，必须是独立
// MySQL 实例，不得复用业务库（部署拓扑）。
type DatabaseConfig struct {
	DSN string `mapstructure:"dsn"`
}

// AMQPConfig 对应 manager.yaml 的 amqp 段：管理中心 Broker 地址。
type AMQPConfig struct {
	URL string `mapstructure:"url"`

	LogQueueMaxLength int `mapstructure:"log_queue_max_length"`
}

type AuthConfig struct {
	SessionTTL          time.Duration   `mapstructure:"session_ttl"`
	Local               LocalAuthConfig `mapstructure:"local"`
	LDAP                LDAPAuthConfig  `mapstructure:"ldap"`
	OIDC                OIDCAuthConfig  `mapstructure:"oidc"`
	DefaultExternalRole string          `mapstructure:"default_external_role"`
	Lockout             LockoutConfig   `mapstructure:"lockout"`
}

// LockoutConfig 对应 auth.lockout：登录失败连续锁定策略（需求
// MGR-USR-01a）。按用户名与客户端 IP 两个独立维度计数（见
// internal/manager/auth.LockoutScope 的注释：只按单一维度计数防不住密码
// 喷洒），两个维度共用同一套阈值/窗口/锁定时长——这不是本地账号专属配置，
// 挂在 auth 段顶层而不是 auth.local 下：设计上本地与 LDAP 登录共用同一个
// 端点（internal/manager/http/csrf.go 里 LoginPath 的注释），锁定策略也应该对两者都生效。
//
// 默认值依据 OWASP Authentication Cheat Sheet 对账号锁定的建议：连续失败
// 5 次后锁定 15 分钟，具体理由见 internal/manager/auth.DefaultLockoutPolicy
// 的注释。
type LockoutConfig struct {
	Threshold    int           `mapstructure:"threshold"`
	Window       time.Duration `mapstructure:"window"`
	LockDuration time.Duration `mapstructure:"lock_duration"`
}

// LocalAuthConfig 对应 auth.local：本地账号登录。
type LocalAuthConfig struct {
	Enabled           bool         `mapstructure:"enabled"`
	BootstrapUser     string       `mapstructure:"bootstrap_user"`
	BootstrapPassword string       `mapstructure:"bootstrap_password"`
	Argon2            Argon2Config `mapstructure:"argon2"`
}

// Argon2Config 对应 auth.local.argon2：本地账号密码哈希（argon2id，需求
// MGR-USR-01）的成本参数，具体哈希/校验逻辑在 internal/manager/auth
// （auth.Argon2Params / auth.HashPassword）。
//
// 默认值（见 internal/manager/config/load.go 的 applyDefaults）依据 OWASP Password Storage Cheat
// Sheet（2023 修订版）针对 argon2id 给出的推荐配置第二档：内存 19 MiB
// （19456 KiB）、时间成本 2 次迭代、并行度 1；第一档（46 MiB、1 次迭代）
// 内存占用更高，不适合与其它服务共用主机部署的场景，两档抗暴力破解强度
// 相当。盐 16 字节、输出 32 字节是同一份文档给出的通用建议。
//
// 做成可配置项而不是写死在代码里：不同部署环境的 CPU/内存资源、期望的
// 登录延迟不同，运维应该能按需要调优，而不必改代码重新编译。
type Argon2Config struct {
	MemoryKiB   uint32 `mapstructure:"memory_kib"`
	Iterations  uint32 `mapstructure:"iterations"`
	Parallelism uint8  `mapstructure:"parallelism"`
	SaltLength  uint32 `mapstructure:"salt_length"`
	KeyLength   uint32 `mapstructure:"key_length"`
}

// LDAPAuthConfig 对应 auth.ldap。BindPassword 是服务账号密码本身；理由与
// LocalAuthConfig.BootstrapPassword 相同（三层合并已经支持环境变量覆盖，
// 不需要再多一层间接引用）。序列化/日志输出前必须走 Config.redacted()。
type LDAPAuthConfig struct {
	CAFile           string            `mapstructure:"ca_file"`
	Timeout          time.Duration     `mapstructure:"timeout"`
	Enabled          bool              `mapstructure:"enabled"`
	URL              string            `mapstructure:"url"`
	BindDN           string            `mapstructure:"bind_dn"`
	BindPassword     string            `mapstructure:"bind_password"`
	UserBaseDN       string            `mapstructure:"user_base_dn"`
	UserFilter       string            `mapstructure:"user_filter"`
	GroupRoleMapping map[string]string `mapstructure:"group_role_mapping"`
}

// OIDCAuthConfig 对应 auth.oidc。ClientSecret 是 OIDC client secret 本身，
// 理由同上。序列化/日志输出前必须走 Config.redacted()。
type OIDCAuthConfig struct {
	CAFile           string            `mapstructure:"ca_file"`
	Timeout          time.Duration     `mapstructure:"timeout"`
	StateTTL         time.Duration     `mapstructure:"state_ttl"`
	Enabled          bool              `mapstructure:"enabled"`
	Issuer           string            `mapstructure:"issuer"`
	ClientID         string            `mapstructure:"client_id"`
	ClientSecret     string            `mapstructure:"client_secret"`
	RedirectURL      string            `mapstructure:"redirect_url"`
	Scopes           []string          `mapstructure:"scopes"`
	RoleClaim        string            `mapstructure:"role_claim"`
	ClaimRoleMapping map[string]string `mapstructure:"claim_role_mapping"`
}

type DispatchConfig struct {
	// ReconcileInterval 是对账循环的轮询周期。默认 15s。
	ReconcileInterval time.Duration `mapstructure:"reconcile_interval"`
}

// BackgroundTasksConfig 对应 manager.yaml 的 background_tasks 段：三个与
// DispatchConfig.ReconcileInterval 同类但归属不同模块的周期性后台维护
// 循环各自的轮询周期——reclaim.Loop（两步删除资源回收）、
// task.InspectionLoop（巡检节点自动选择+派发+结果落库）、
// task.PartitionLoop（logs/pipeline_metrics 分区预创建）。
type BackgroundTasksConfig struct {
	ReclaimInterval        time.Duration `mapstructure:"reclaim_interval"`
	InspectionInterval     time.Duration `mapstructure:"inspection_interval"`
	AlertInterval          time.Duration `mapstructure:"alert_interval"`
	PartitionCheckInterval time.Duration `mapstructure:"partition_check_interval"`

	// EndpointProbeEnabled enables periodic endpoint probes; the default is false.
	// Manual probes are independent. Successful probes update persisted endpoint metadata.
	EndpointProbeEnabled bool `mapstructure:"endpoint_probe_enabled"`
}

// PipelineConfig controls the globally unique binlog replication server ID range.
type PipelineConfig struct {
	// BinlogServerIDRangeMin/Max 是分配 server_id 时的候选区间（闭区间，
	// 含两端）。Min 必须为正——0 是 MySQL 复制协议保留值，代表"未注册的
	// 匿名客户端"，不能分配给真实 dump 连接；Max 必须严格大于 Min，
	// 否则区间只有单个值，起不到冲突重试应有的"换一个候选值"的效果。
	BinlogServerIDRangeMin uint32 `mapstructure:"binlog_server_id_range_min"`
	BinlogServerIDRangeMax uint32 `mapstructure:"binlog_server_id_range_max"`
}

// LogConfig 对应 manager.yaml 的 log 段。Format 取 xslog 的两种格式
// （console/json，见 github.com/xmapst/xslog），满足需求 C-06（Manager 与
// Node 统一使用 log/slog + xslog）。
//
// MaxSizeMB/MaxBackups/MaxAgeDays 是文件日志的轮转参数（对应
// xslog.WithMaxSizeMB/WithMaxBackups/WithMaxAgeDays），字段名和 tag 与
// internal/node/config.LogConfig 逐字一致——C-06 要求的是统一，运维看到的
// 配置口径不能一个 binary 一套键名。
type LogConfig struct {
	Format     string `mapstructure:"format"` // json | console
	File       string `mapstructure:"file"`   // 留空写 stdout
	Level      string `mapstructure:"level"`  // debug | info | warn | error
	MaxSizeMB  int    `mapstructure:"max_size_mb"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAgeDays int    `mapstructure:"max_age_days"`
}

// RetentionConfig 对应 manager.yaml 的 retention 段。默认值：logs / metrics
// 15 天，events / conflicts 30 天。
type RetentionConfig struct {
	Logs      time.Duration `mapstructure:"logs"`
	Metrics   time.Duration `mapstructure:"metrics"`
	Events    time.Duration `mapstructure:"events"`
	Conflicts time.Duration `mapstructure:"conflicts"`
}

func (c Config) Validate() error {
	return errors.Join(
		c.HTTP.validate(),
		c.Database.validate(),
		c.AMQP.validate(),
		c.Auth.validate(),
		c.Dispatch.validate(),
		c.BackgroundTasks.validate(),
		c.Pipeline.validate(),
		c.Log.validate(),
		c.Retention.validate(),
	)
}

func (h HTTPConfig) validate() error {
	var errs []error
	if strings.TrimSpace(h.Listen) == "" {
		errs = append(errs, errors.New("config: http.listen 不能为空"))
	} else if _, portStr, err := net.SplitHostPort(h.Listen); err != nil {
		errs = append(errs, fmt.Errorf("config: http.listen 格式不合法（期望 host:port 或 :port）: %w", err))
	} else if port, err := strconv.Atoi(portStr); err != nil || port < 1 || port > 65535 {
		errs = append(errs, fmt.Errorf("config: http.listen 端口不合法: %q", portStr))
	}
	if h.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("config: http.shutdown_timeout 必须为正"))
	}
	return errors.Join(errs...)
}

func (d DatabaseConfig) validate() error {
	if strings.TrimSpace(d.DSN) == "" {
		return errors.New("config: database.dsn 不能为空")
	}
	if _, err := mysql.ParseDSN(d.DSN); err != nil {
		return fmt.Errorf("config: database.dsn 格式不合法: %w", err)
	}
	return nil
}

func (a AMQPConfig) validate() error {
	if strings.TrimSpace(a.URL) == "" {
		return errors.New("config: amqp.url 不能为空")
	}
	u, err := url.Parse(a.URL)
	if err != nil {
		return fmt.Errorf("config: amqp.url 格式不合法: %w", err)
	}
	switch u.Scheme {
	case "amqp", "amqps":
	default:
		return fmt.Errorf("config: amqp.url scheme 必须为 amqp 或 amqps，实际 %q", u.Scheme)
	}
	if u.Host == "" {
		return errors.New("config: amqp.url 缺少 host")
	}
	if a.LogQueueMaxLength <= 0 {
		return errors.New("config: amqp.log_queue_max_length 必须为正整数")
	}
	return nil
}

func (a AuthConfig) validate() error {
	var errs []error
	if a.SessionTTL <= 0 {
		errs = append(errs, errors.New("config: auth.session_ttl 必须为正"))
	}
	if a.DefaultExternalRole != auth.DenyExternalRole && !auth.Role(a.DefaultExternalRole).Valid() {
		errs = append(errs, errors.New("config: auth.default_external_role 必须为 admin、readonly 或 deny"))
	}
	if err := a.Local.validate(); err != nil {
		errs = append(errs, err)
	}
	if err := a.LDAP.validate(); err != nil {
		errs = append(errs, err)
	}
	if err := a.OIDC.validate(); err != nil {
		errs = append(errs, err)
	}
	if err := a.Lockout.validate(); err != nil {
		errs = append(errs, err)
	}
	// 这里选择启动即失败，不是打一条 Warning 后继续启动：这个错误
	// 100% 确定（没有一种方式能登录）、0% 能自愈（不改配置永远不会变成
	// "能登录"），Warning 会被日志淹没而配置错误不会自己消失——一个不能
	// 提供任何登录入口的 manager 不应该假装自己能正常服务。这与
	// "AMQP 连不上不阻塞启动"（cmd/manager/main.go 的既有先例）不是同一类
	// 问题：AMQP 有明确的、会自动生效的重试路径，登录方式全部禁用没有
	// 任何东西会让它自己变对。
	if !a.Local.Enabled && !a.LDAP.Enabled && !a.OIDC.Enabled {
		errs = append(errs, errors.New(
			"config: auth.local/ldap/oidc 的 enabled 不能同时为 false——"+
				"没有任何登录方式，manager 无法提供服务"))
	}
	return errors.Join(errs...)
}

func (l LockoutConfig) validate() error {
	var errs []error
	if l.Threshold <= 0 {
		errs = append(errs, errors.New("config: auth.lockout.threshold 必须为正"))
	}
	if l.Window <= 0 {
		errs = append(errs, errors.New("config: auth.lockout.window 必须为正"))
	}
	if l.LockDuration <= 0 {
		errs = append(errs, errors.New("config: auth.lockout.lock_duration 必须为正"))
	}
	// 静默把 window 钳到 lock_duration（而不是报错）会让运维误以为自己配的
	// 值生效了，属于"静默修正比报错更糟"的一类问题，改用显式拒绝。
	if l.Window > 0 && l.LockDuration > 0 && l.Window > l.LockDuration {
		errs = append(errs, fmt.Errorf(
			"config: auth.lockout.window（%s）不能大于 lock_duration（%s）："+
				"否则锁定期结束后旧的失败计数窗口可能还没过期，第一次重试就会立刻被重新锁定",
			l.Window, l.LockDuration))
	}
	return errors.Join(errs...)
}

func (l LocalAuthConfig) validate() error {
	var errs []error
	// bootstrap_user/bootstrap_password 同时留空在配置层是合法状态，因此
	// 不像其它必填项那样"非空才合法"——是否真的安全取决于本地账号表是否
	// 为空，这个事实配置校验阶段拿不到，交给 auth.BootstrapAdmin 在运行期
	// 区分（表非空返回 BootstrapSkipped；表为空返回 BootstrapNoCredentials，
	// 调用方按启动失败处理，见该类型的注释）。但只配置其中一个几乎总是
	// 运维笔误（例如设置了初始用户名却忘了密码），任由它悄悄退化成同一种
	// "未配置"状态比直接拒绝更难排查，所以只拒绝这一种"半配置"状态。
	if (l.BootstrapUser == "") != (l.BootstrapPassword == "") {
		errs = append(errs, errors.New("config: auth.local.bootstrap_user 与 bootstrap_password 必须同时配置或同时留空"))
	}
	if err := l.Argon2.validate(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func (a Argon2Config) validate() error {
	var errs []error
	if a.MemoryKiB == 0 {
		errs = append(errs, errors.New("config: auth.local.argon2.memory_kib 必须为正"))
	}
	if a.Iterations == 0 {
		errs = append(errs, errors.New("config: auth.local.argon2.iterations 必须为正"))
	}
	if a.Parallelism == 0 {
		errs = append(errs, errors.New("config: auth.local.argon2.parallelism 必须为正"))
	}
	if a.SaltLength == 0 {
		errs = append(errs, errors.New("config: auth.local.argon2.salt_length 必须为正"))
	}
	if a.KeyLength == 0 {
		errs = append(errs, errors.New("config: auth.local.argon2.key_length 必须为正"))
	}
	return errors.Join(errs...)
}

func (l LDAPAuthConfig) validate() error {
	if !l.Enabled {
		return nil
	}
	return auth.ValidateLDAPConfig(l.AuthenticatorConfig("readonly"))
}

// AuthenticatorConfig 给认证模块传只读配置值，默认角色由 auth 顶层提供。
func (l LDAPAuthConfig) AuthenticatorConfig(defaultRole string) auth.LDAPConfig {
	return auth.LDAPConfig{URL: l.URL, BindDN: l.BindDN, BindPassword: l.BindPassword, UserBaseDN: l.UserBaseDN, UserFilter: l.UserFilter, CAFile: l.CAFile, Timeout: l.Timeout, GroupRoleMapping: l.GroupRoleMapping, DefaultExternalRole: defaultRole}
}

func (o OIDCAuthConfig) validate() error {
	if !o.Enabled {
		return nil
	}
	return auth.ValidateOIDCConfig(o.AuthenticatorConfig("readonly"))
}
func (o OIDCAuthConfig) AuthenticatorConfig(defaultRole string) auth.OIDCConfig {
	return auth.OIDCConfig{Issuer: o.Issuer, ClientID: o.ClientID, ClientSecret: o.ClientSecret, RedirectURL: o.RedirectURL, Scopes: o.Scopes, RoleClaim: o.RoleClaim, ClaimRoleMapping: o.ClaimRoleMapping, DefaultExternalRole: defaultRole, CAFile: o.CAFile, Timeout: o.Timeout, StateTTL: o.StateTTL}
}

func (d DispatchConfig) validate() error {
	if d.ReconcileInterval <= 0 {
		return errors.New("config: dispatch.reconcile_interval 必须为正")
	}
	return nil
}

func (b BackgroundTasksConfig) validate() error {
	var errs []error
	if b.ReclaimInterval <= 0 {
		errs = append(errs, errors.New("config: background_tasks.reclaim_interval 必须为正"))
	}
	if b.InspectionInterval <= 0 {
		errs = append(errs, errors.New("config: background_tasks.inspection_interval 必须为正"))
	}
	if b.AlertInterval <= 0 {
		errs = append(errs, errors.New("config: background_tasks.alert_interval 必须为正"))
	}
	if b.PartitionCheckInterval <= 0 {
		errs = append(errs, errors.New("config: background_tasks.partition_check_interval 必须为正"))
	}
	return errors.Join(errs...)
}

func (p PipelineConfig) validate() error {
	var errs []error
	if p.BinlogServerIDRangeMin == 0 {
		errs = append(errs, errors.New("config: pipeline.binlog_server_id_range_min 必须为正（0 是 MySQL 复制协议保留值）"))
	}
	if p.BinlogServerIDRangeMax <= p.BinlogServerIDRangeMin {
		errs = append(errs, errors.New("config: pipeline.binlog_server_id_range_max 必须严格大于 range_min"))
	}
	return errors.Join(errs...)
}

func (l LogConfig) validate() error {
	var errs []error
	switch strings.ToLower(l.Format) {
	case "json", "console":
	default:
		errs = append(errs, fmt.Errorf("config: log.format 必须为 json 或 console，实际 %q", l.Format))
	}
	switch strings.ToLower(l.Level) {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("config: log.level 必须为 debug/info/warn/error 之一，实际 %q", l.Level))
	}
	if l.MaxSizeMB < 0 {
		errs = append(errs, errors.New("config: log.max_size_mb 不能为负"))
	}
	if l.MaxBackups < 0 {
		errs = append(errs, errors.New("config: log.max_backups 不能为负"))
	}
	if l.MaxAgeDays < 0 {
		errs = append(errs, errors.New("config: log.max_age_days 不能为负"))
	}
	return errors.Join(errs...)
}

func (r RetentionConfig) validate() error {
	var errs []error
	check := func(field string, p time.Duration) {
		if p <= 0 {
			errs = append(errs, fmt.Errorf("config: retention.%s 必须为正（当前 %s）", field, p))
		}
	}
	check("logs", r.Logs)
	check("metrics", r.Metrics)
	check("events", r.Events)
	check("conflicts", r.Conflicts)
	return errors.Join(errs...)
}

func (c Config) String() string {
	b, err := json.MarshalIndent(c.redacted(), "", "  ")
	if err != nil {
		return "(config 序列化失败，已隐藏原始内容)"
	}
	return string(b)
}

// LogValue 实现 slog.LogValuer：一旦有代码把 Config 整体传给 slog（例如
// slog.Any("config", cfg)），也会自动走脱敏路径，不依赖调用方记得调用
// String()。
func (c Config) LogValue() slog.Value {
	return slog.AnyValue(c.redacted())
}

// redactedConfig 是 Config 的脱敏镜像，专门用于 String()/LogValue() 输出。
// 刻意不复用 Config 类型本身，避免 LogValue 递归解析自身造成死循环。
type redactedConfig struct {
	HTTP     HTTPConfig `json:"http"`
	Database struct {
		DSN string `json:"dsn"`
	} `json:"database"`
	AMQP struct {
		URL string `json:"url"`
	} `json:"amqp"`
	Auth            redactedAuthConfig    `json:"auth"`
	Dispatch        DispatchConfig        `json:"dispatch"`
	BackgroundTasks BackgroundTasksConfig `json:"background_tasks"`
	Pipeline        PipelineConfig        `json:"pipeline"`
	Log             LogConfig             `json:"log"`
	Retention       RetentionConfig       `json:"retention"`
}

type redactedAuthConfig struct {
	SessionTTL          time.Duration           `json:"session_ttl"`
	Local               redactedLocalAuthConfig `json:"local"`
	LDAP                redactedLDAPAuthConfig  `json:"ldap"`
	OIDC                redactedOIDCAuthConfig  `json:"oidc"`
	DefaultExternalRole string                  `json:"default_external_role"`
	Lockout             LockoutConfig           `json:"lockout"`
}

type redactedLocalAuthConfig struct {
	Enabled bool `json:"enabled"`
	// BootstrapUser 是用户名，不是敏感值，脱敏输出里保留明文，便于运维
	// 核对"引导出来的到底是哪个账号"。
	BootstrapUser     string       `json:"bootstrap_user"`
	BootstrapPassword string       `json:"bootstrap_password"`
	Argon2            Argon2Config `json:"argon2"`
}

type redactedLDAPAuthConfig struct {
	CAFile           string            `json:"ca_file"`
	Timeout          time.Duration     `json:"timeout"`
	Enabled          bool              `json:"enabled"`
	URL              string            `json:"url"`
	BindDN           string            `json:"bind_dn"`
	BindPassword     string            `json:"bind_password"`
	UserBaseDN       string            `json:"user_base_dn"`
	UserFilter       string            `json:"user_filter"`
	GroupRoleMapping map[string]string `json:"group_role_mapping"`
}

type redactedOIDCAuthConfig struct {
	CAFile           string            `json:"ca_file"`
	Timeout          time.Duration     `json:"timeout"`
	StateTTL         time.Duration     `json:"state_ttl"`
	Enabled          bool              `json:"enabled"`
	Issuer           string            `json:"issuer"`
	ClientID         string            `json:"client_id"`
	ClientSecret     string            `json:"client_secret"`
	RedirectURL      string            `json:"redirect_url"`
	Scopes           []string          `json:"scopes"`
	RoleClaim        string            `json:"role_claim"`
	ClaimRoleMapping map[string]string `json:"claim_role_mapping"`
}

func (a AuthConfig) redacted() redactedAuthConfig {
	return redactedAuthConfig{
		SessionTTL: a.SessionTTL,
		Local: redactedLocalAuthConfig{
			Enabled:           a.Local.Enabled,
			BootstrapUser:     a.Local.BootstrapUser,
			BootstrapPassword: redactSecret(a.Local.BootstrapPassword),
			Argon2:            a.Local.Argon2,
		},
		LDAP: redactedLDAPAuthConfig{
			CAFile:           a.LDAP.CAFile,
			Timeout:          a.LDAP.Timeout,
			Enabled:          a.LDAP.Enabled,
			URL:              a.LDAP.URL,
			BindDN:           a.LDAP.BindDN,
			BindPassword:     redactSecret(a.LDAP.BindPassword),
			UserBaseDN:       a.LDAP.UserBaseDN,
			UserFilter:       a.LDAP.UserFilter,
			GroupRoleMapping: a.LDAP.GroupRoleMapping,
		},
		OIDC: redactedOIDCAuthConfig{
			CAFile:           a.OIDC.CAFile,
			Timeout:          a.OIDC.Timeout,
			StateTTL:         a.OIDC.StateTTL,
			Enabled:          a.OIDC.Enabled,
			Issuer:           a.OIDC.Issuer,
			ClientID:         a.OIDC.ClientID,
			ClientSecret:     redactSecret(a.OIDC.ClientSecret),
			RedirectURL:      a.OIDC.RedirectURL,
			Scopes:           a.OIDC.Scopes,
			RoleClaim:        a.OIDC.RoleClaim,
			ClaimRoleMapping: a.OIDC.ClaimRoleMapping,
		},
		DefaultExternalRole: a.DefaultExternalRole,
		Lockout:             a.Lockout,
	}
}

// redactSecret 把非空的密码/令牌类字段替换为 "***"；空值保持空——区分
// "配置了但被隐藏"与"本来就没配置"，对运维排查是否真的配置了该项有意义
// （呼应 redactDSN/redactAMQPURL 对空 DSN/URL 的处理）。
func redactSecret(v string) string {
	if v == "" {
		return ""
	}
	return "***"
}

func (c Config) redacted() redactedConfig {
	r := redactedConfig{
		HTTP:            c.HTTP,
		Auth:            c.Auth.redacted(),
		Dispatch:        c.Dispatch,
		BackgroundTasks: c.BackgroundTasks,
		Pipeline:        c.Pipeline,
		Log:             c.Log,
		Retention:       c.Retention,
	}
	r.Database.DSN = redactDSN(c.Database.DSN)
	r.AMQP.URL = redactAMQPURL(c.AMQP.URL)
	return r
}

// redactDSN 把 MySQL DSN 中的密码替换为 "***"；DSN 本身无法解析时不回显
// 原始内容，只给出无法解析的提示。
func redactDSN(dsn string) string {
	if dsn == "" {
		return ""
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "(dsn 格式无法解析，已隐藏原始内容)"
	}
	if cfg.Passwd != "" {
		cfg.Passwd = "***"
	}
	return cfg.FormatDSN()
}

// redactAMQPURL 把 AMQP URL 中的密码替换为占位符；URL 本身无法解析时不回显
// 原始内容。
//
// 用 "REDACTED" 而不是 "***"：net/url 对 userinfo 里的 "*" 会做百分号
// 转义（结果类似 "%2A%2A%2A"），字母数字占位符不会有这个问题。
func redactAMQPURL(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "(amqp url 格式无法解析，已隐藏原始内容)"
	}
	if u.User != nil {
		if _, hasPassword := u.User.Password(); hasPassword {
			u.User = url.UserPassword(u.User.Username(), "REDACTED")
		}
	}
	return u.String()
}
