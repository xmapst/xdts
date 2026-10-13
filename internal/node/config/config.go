// Node 配置包括 AMQP、身份和本地状态库、模块关闭预算及资源限制。
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/node/resourcequota"
)

// Config 是 Node 进程的完整配置视图。
type Config struct {
	AMQP       AMQPConfig           `mapstructure:"amqp"`
	Node       NodeConfig           `mapstructure:"node"`
	Store      StoreConfig          `mapstructure:"store"`
	Controller ControllerConfig     `mapstructure:"controller"`
	Resources  resourcequota.Limits `mapstructure:"resources"`
	Log        LogConfig            `mapstructure:"log"`
}

// AMQPConfig 对应 node.yaml 的 amqp 段：本中心 RabbitMQ 地址（NODE-BOOT-01）。
type AMQPConfig struct {
	URL string `mapstructure:"url"`
}

// NodeConfig 指定本地状态目录和注册凭据。身份从 identity.xdoc 恢复，首次由注册取得。
type NodeConfig struct {
	DataDir      string `mapstructure:"data_dir"`
	RegisterCode string `mapstructure:"register_code"`
}

// StoreConfig 对应 node.yaml 的 store 段，控制 store 静态模块的批量提交与
// 关闭行为（NODE-XDOC-04）。
//
// ShutdownTimeout 是 store 模块自己的关闭排空超时（映射到
// xhive.WithCloseDrainTimeout / WithClientCloseTimeout / WithAsyncCloseTimeout），
// 不是整个应用的关闭超时——后者受 xhive 已知缺口限制，包外不可达
// ，不属于本配置能覆盖的范围。
type StoreConfig struct {
	BatchInterval   time.Duration `mapstructure:"batch_interval"`
	BatchMaxOps     int           `mapstructure:"batch_max_ops"`
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// ControllerConfig 对应 node.yaml 的 controller 段。
//
// 两者语义不同且合适取值可能方向相反：store 那个是"排空在途异步任务，含最后一次
// 批量提交"，要够长以免丢掉最后一批写入；本键是"等控制队列 handler 的在途调用返回"，
// 要够短以免关不掉。复用的真正代价不是语义不同，是它会建立一条没人知道的耦合
// ——将来有人因为落盘慢把 store.shutdown_timeout 调大，会连带把 controller 的关停
// 窗口一起调大，而他不会知道。
//
// 等的是 controller 控制队列 handler（onDelivery）同步部分返回：信封解析 +
// 按 Properties.Type 分派投递，毫秒级。不是等那次 config.pull 完成——真正的
// 网络拉取在 AsyncGo 里（internal/node/controller/dispatch.go onConfigChanged、internal/node/controller/pull.go resolveStartupConfig），
// 受 defaultTargetedPullTimeout / defaultStartupPullTimeout（各 30s）约束，不在
// 本窗口内。把本键按那两个 30s 去设是一个很容易犯的错：它会让每次关停多等 25s，
// 却一秒也不会让那次 RPC 多跑完。
type ControllerConfig struct {
	// ShutdownTimeout 是 controller 关闭控制队列消费者的等待上限，映射到
	// transport.Consumer.CloseWithTimeout。
	//
	// **注意 <=0 不是"不限时"**：按 transport.Consumer.CloseWithTimeout 的文档，
	// timeout<=0 表示**立刻强制关闭**。所以消费者关闭处必须写成双分支（>0 才调
	// CloseWithTimeout，否则退回不带超时的 Close），不能图省事把值直接传进去
	// ——见 controller.Module.Close 与 amqpin.Module.Close。
	ShutdownTimeout time.Duration `mapstructure:"shutdown_timeout"`
}

// LogConfig 对应 node.yaml 的 log 段。Format 取 xslog 的两种格式
// （console/json，见 github.com/xmapst/xslog），满足需求 C-06。
type LogConfig struct {
	Format     string `mapstructure:"format"` // console | json
	File       string `mapstructure:"file"`   // 留空写 stdout
	Level      string `mapstructure:"level"`  // debug | info | warn | error
	MaxSizeMB  int    `mapstructure:"max_size_mb"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAgeDays int    `mapstructure:"max_age_days"`
}

func (c Config) Validate() error {
	return errors.Join(
		c.AMQP.validate(),
		c.Node.validate(),
		c.Store.validate(),
		c.Controller.validate(),
		c.Resources.Validate(),
		c.Log.validate(),
	)
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
	return nil
}

func (n NodeConfig) validate() error {
	var errs []error
	if strings.TrimSpace(n.DataDir) == "" {
		errs = append(errs, errors.New("config: node.data_dir 不能为空"))
	}

	if strings.TrimSpace(n.RegisterCode) == "" {
		errs = append(errs, errors.New("config: node.register_code 不能为空（NODE-BOOT-01：推荐用环境变量 XDTS_NODE_REGISTER_CODE 注入）"))
	}
	return errors.Join(errs...)
}

func (s StoreConfig) validate() error {
	var errs []error
	if s.BatchInterval <= 0 {
		errs = append(errs, errors.New("config: store.batch_interval 必须为正"))
	}
	if s.BatchMaxOps <= 0 {
		errs = append(errs, errors.New("config: store.batch_max_ops 必须为正"))
	}
	if s.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("config: store.shutdown_timeout 必须为正"))
	}
	return errors.Join(errs...)
}

// validate 要求 controller.shutdown_timeout 为正，与 store 同口径。
//
// 为什么不允许 <=0 通过：按 transport.Consumer.CloseWithTimeout 的语义，<=0 是
// **立刻强制关闭**，不是"不限时"。如果在配置层放行 0，写配置的人多半以为自己得到
// 的是"不限时"，实际得到的是"不等在途 handler 直接掀掉"——**两个预期恰好相反**。
// 所以这里让它在启动时就报错，不留那个陷阱。
func (c ControllerConfig) validate() error {
	var errs []error
	if c.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("config: controller.shutdown_timeout 必须为正"))
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

func (c Config) String() string {
	b, err := json.MarshalIndent(c.redacted(), "", "  ")
	if err != nil {
		return "(config 序列化失败，已隐藏原始内容)"
	}
	return string(b)
}

// LogValue 实现 slog.LogValuer：一旦有代码把 Config 整体传给 slog（例如
// slog.Any("config", cfg)），也会自动走脱敏路径，不依赖调用方记得调用 String()。
func (c Config) LogValue() slog.Value {
	return slog.AnyValue(c.redacted())
}

// redactedConfig 是 Config 的脱敏镜像，专门用于 String()/LogValue() 输出。
// 刻意不复用 Config 类型本身，避免 LogValue 递归解析自身造成死循环。
type redactedConfig struct {
	AMQP struct {
		URL string `json:"url"`
	} `json:"amqp"`
	Node struct {
		DataDir      string `json:"data_dir"`
		RegisterCode string `json:"register_code"`
	} `json:"node"`
	Store     StoreConfig          `json:"store"`
	Resources resourcequota.Limits `json:"resources"`
	Log       LogConfig            `json:"log"`
}

func (c Config) redacted() redactedConfig {
	var r redactedConfig
	r.AMQP.URL = redactAMQPURL(c.AMQP.URL)
	r.Node.DataDir = c.Node.DataDir
	if c.Node.RegisterCode != "" {
		r.Node.RegisterCode = "***"
	}
	r.Store = c.Store
	r.Resources = c.Resources
	r.Log = c.Log
	return r
}

// redactAMQPURL 把 AMQP URL 中的密码替换为占位符；URL 本身无法解析时不回显
// 原始内容。
//
// 用 "REDACTED" 而不是 "***"：net/url 对 userinfo 里的 "*" 会做百分号转义
// （结果类似 "%2A%2A%2A"），字母数字占位符不会有这个问题。
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
