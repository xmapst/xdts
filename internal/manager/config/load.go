package config

import (
	"errors"
	"fmt"
	"strings"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// envPrefix 是环境变量覆盖层使用的前缀：XDTS_HTTP_LISTEN、
// XDTS_DATABASE_DSN、XDTS_AUTH_LDAP_BIND_DN 等，由配置键路径按 "." 转
// "_" 大写得到。
const envPrefix = "XDTS"

// LoadOptions 描述三层合并（配置文件 < 环境变量 < 命令行参数）的输入来源。
type LoadOptions struct {
	// ConfigFile 是显式指定的配置文件路径。为空时按默认路径
	// （当前目录 / /etc/xdts）查找 manager.yaml，找不到不算错误
	// （允许纯靠环境变量 + 命令行运行）。
	ConfigFile string
	// Flags 是命令行参数层，可为 nil（例如单元测试、工具场景）。
	// 约定 flag 名与配置键路径一致（如 "http.listen"、"log.level"）。
	Flags *pflag.FlagSet
}

// Load 按「配置文件 < 环境变量 < 命令行参数」的优先级合并并校验配置
// （需求 NFR-OPS-01）。校验失败时返回的错误明确指出是哪个字段。
func Load(opts LoadOptions) (*Config, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	applyDefaults(v)

	if opts.ConfigFile != "" {
		v.SetConfigFile(opts.ConfigFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: 读取配置文件 %s 失败: %w", opts.ConfigFile, err)
		}
	} else {
		v.SetConfigName("manager")
		v.AddConfigPath(".")
		v.AddConfigPath("/etc/xdts")
		if err := v.ReadInConfig(); err != nil {
			if _, ok := errors.AsType[viper.ConfigFileNotFoundError](err); !ok {
				return nil, fmt.Errorf("config: 读取配置文件失败: %w", err)
			}
			// 找不到默认配置文件不算错误：允许纯靠环境变量 + 命令行运行。
		}
	}

	v.SetEnvPrefix(envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	if opts.Flags != nil {
		if err := v.BindPFlags(opts.Flags); err != nil {
			return nil, fmt.Errorf("config: 绑定命令行参数失败: %w", err)
		}
	}

	decodeHook := mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.TextUnmarshallerHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
	)

	var cfg Config
	if err := v.Unmarshal(&cfg, viper.DecodeHook(decodeHook)); err != nil {
		return nil, fmt.Errorf("config: 解析配置失败: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyDefaults 为全部字段设置默认值。必须覆盖每一个叶子键：viper 的
// AutomaticEnv 只对「已知」的键生效，默认值同时也是让环境变量覆盖生效的
// 前提。
func applyDefaults(v *viper.Viper) {
	v.SetDefault("http.listen", ":8080")
	v.SetDefault("http.shutdown_timeout", "15s")
	v.SetDefault("http.cookie_secure", true)

	v.SetDefault("database.dsn", "")

	v.SetDefault("amqp.url", "")
	// 100000 是 xdts.manager.log 队列 x-max-length 的默认容量（配合
	// x-overflow: drop-head），对应中等规模部署的经验值；运维应按中心
	// 实际节点数/日志量通过 amqp.log_queue_max_length 调整。
	v.SetDefault("amqp.log_queue_max_length", 100000)

	v.SetDefault("auth.session_ttl", "12h")
	v.SetDefault("auth.local.enabled", true)
	v.SetDefault("auth.local.bootstrap_user", "")
	v.SetDefault("auth.local.bootstrap_password", "")
	// argon2id 默认参数依据见 internal/manager/config/config.go 的 Argon2Config 注释（OWASP 推荐档位）。
	v.SetDefault("auth.local.argon2.memory_kib", 19*1024)
	v.SetDefault("auth.local.argon2.iterations", 2)
	v.SetDefault("auth.local.argon2.parallelism", 1)
	v.SetDefault("auth.local.argon2.salt_length", 16)
	v.SetDefault("auth.local.argon2.key_length", 32)
	v.SetDefault("auth.ldap.enabled", false)
	v.SetDefault("auth.ldap.url", "")
	v.SetDefault("auth.ldap.ca_file", "")
	v.SetDefault("auth.ldap.timeout", "10s")
	v.SetDefault("auth.ldap.bind_dn", "")
	v.SetDefault("auth.ldap.bind_password", "")
	v.SetDefault("auth.ldap.user_base_dn", "")
	v.SetDefault("auth.ldap.user_filter", "")
	v.SetDefault("auth.ldap.group_role_mapping", map[string]string{})
	v.SetDefault("auth.oidc.enabled", false)
	v.SetDefault("auth.oidc.ca_file", "")
	v.SetDefault("auth.oidc.timeout", "10s")
	v.SetDefault("auth.oidc.state_ttl", "5m")
	v.SetDefault("auth.oidc.issuer", "")
	v.SetDefault("auth.oidc.client_id", "")
	v.SetDefault("auth.oidc.client_secret", "")
	v.SetDefault("auth.oidc.redirect_url", "")
	v.SetDefault("auth.oidc.scopes", []string{"openid", "profile", "email"})
	v.SetDefault("auth.oidc.role_claim", "groups")
	v.SetDefault("auth.oidc.claim_role_mapping", map[string]string{})
	v.SetDefault("auth.default_external_role", "readonly")
	// 登录失败锁定默认值依据见 internal/manager/config/config.go 的 LockoutConfig 注释（OWASP
	// Authentication Cheat Sheet：5 次失败后锁定 15 分钟）。
	v.SetDefault("auth.lockout.threshold", 5)
	v.SetDefault("auth.lockout.window", "15m")
	v.SetDefault("auth.lockout.lock_duration", "15m")

	// 周期端点探测默认关闭；手动探测仍可用。前置条件见
	// internal/manager/config/config.go 的 BackgroundTasksConfig.EndpointProbeEnabled。
	v.SetDefault("background_tasks.endpoint_probe_enabled", false)

	// 默认 15s，与 dispatch.DefaultReconcileInterval取值
	// 一致——两处都要改的理由见 internal/manager/config/config.go 的 DispatchConfig 注释。
	v.SetDefault("dispatch.reconcile_interval", "15s")

	v.SetDefault("background_tasks.reclaim_interval", "30s")
	v.SetDefault("background_tasks.inspection_interval", "1m")
	v.SetDefault("background_tasks.alert_interval", "15s")
	v.SetDefault("background_tasks.partition_check_interval", "24h")

	// Keep binlog server ID defaults here; HTTP receives the validated range rather
	// than providing another default source.
	v.SetDefault("pipeline.binlog_server_id_range_min", 4_000_000_000)
	v.SetDefault("pipeline.binlog_server_id_range_max", 4_294_967_295)

	v.SetDefault("log.format", "json")
	v.SetDefault("log.file", "")
	v.SetDefault("log.level", "info")
	// 与 internal/node/config.applyDefaults 的 log.* 默认值逐字一致，
	// 同时也是 xslog.DefaultMaxSizeMB/DefaultMaxBackups/DefaultMaxAgeDays
	// 本身的取值。
	v.SetDefault("log.max_size_mb", 50)
	v.SetDefault("log.max_backups", 7)
	v.SetDefault("log.max_age_days", 7)

	// 保留期默认值的权威在 internal/manager/config/retention.go 的 DefaultRetention* 四个常量，
	// 这里使用标准 duration 字符串供 mapstructure 解析，
	// 不要在这里写字面量——那会把单一来源重新拆成两份。
	v.SetDefault("retention.logs", DefaultRetentionLogs.String())
	v.SetDefault("retention.metrics", DefaultRetentionMetrics.String())
	v.SetDefault("retention.events", DefaultRetentionEvents.String())
	v.SetDefault("retention.conflicts", DefaultRetentionConflicts.String())
}
