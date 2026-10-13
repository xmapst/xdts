package config

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"

	mapstructure "github.com/go-viper/mapstructure/v2"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/xmapst/xdts/internal/node/resourcequota"
)

// envPrefix 是环境变量覆盖层使用的前缀：XDTS_NODE_DATA_DIR、
// XDTS_NODE_REGISTER_CODE、XDTS_AMQP_URL 等，由配置键路径按 "." 转 "_" 大写
// 得到（与 internal/manager/config 的约定一致）。
const envPrefix = "XDTS"

// LoadOptions 描述三层合并（配置文件 < 环境变量 < 命令行参数）的输入来源。
type LoadOptions struct {
	// ConfigFile 是显式指定的配置文件路径。为空时按默认路径（当前目录 /
	// /etc/xdts）查找 node.yaml，找不到不算错误（允许纯靠环境变量 + 命令行
	// 运行——register_code 本就推荐走环境变量，见 NODE-BOOT-01）。
	ConfigFile string
	// Flags 是命令行参数层，可为 nil（例如单元测试场景）。约定 flag 名与
	// 配置键路径一致（如 "node.data_dir"、"log.level"）。
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
		v.SetConfigName("node")
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
		var bindErr error
		opts.Flags.VisitAll(func(flag *pflag.Flag) {
			if flag.Name == "config" || bindErr != nil {
				return
			}
			bindErr = v.BindPFlag(flag.Name, flag)
		})
		if bindErr != nil {
			return nil, fmt.Errorf("config: 绑定命令行参数失败: %w", bindErr)
		}
	}

	decodeHook := mapstructure.ComposeDecodeHookFunc(
		strictUnsignedResourceValue,
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.TextUnmarshallerHookFunc(),
	)

	var cfg Config
	if err := v.UnmarshalExact(&cfg, viper.DecodeHook(decodeHook)); err != nil {
		return nil, fmt.Errorf("config: 解析配置失败: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyDefaults 为全部字段设置默认值。必须覆盖每一个叶子键：viper 的
// AutomaticEnv 只对「已知」的键生效，默认值同时也是让环境变量覆盖生效的前提。
func applyDefaults(v *viper.Viper) {
	v.SetDefault("amqp.url", "")

	v.SetDefault("node.data_dir", "")
	v.SetDefault("node.register_code", "")

	v.SetDefault("store.batch_interval", "100ms")
	v.SetDefault("store.batch_max_ops", 200)
	v.SetDefault("store.shutdown_timeout", "30s")
	// controller.shutdown_timeout 取 5s 而不是跟 store 一样的 30s：本键等的是控制
	// 队列 handler 同步部分返回（毫秒级），那次 config.pull 的 30s 在 AsyncGo 里、
	// 不在这个窗口内（理由见 internal/node/config/config.go ControllerConfig 文档）。
	v.SetDefault("controller.shutdown_timeout", "5s")
	resources := resourcequota.DefaultLimits()
	v.SetDefault("resources.max_pipelines", resources.MaxPipelines)
	v.SetDefault("resources.max_buffer_bytes", resources.MaxBufferBytes)
	v.SetDefault("resources.pipeline_buffer_bytes", resources.PipelineBufferBytes)
	v.SetDefault("resources.snapshot_concurrency", resources.SnapshotConcurrency)

	v.SetDefault("log.format", "json")
	v.SetDefault("log.file", "")
	v.SetDefault("log.level", "info")
	v.SetDefault("log.max_size_mb", 50)
	v.SetDefault("log.max_backups", 7)
	v.SetDefault("log.max_age_days", 7)
}

// YAML floating values must not silently truncate or wrap into resource limits.
func strictUnsignedResourceValue(from, to reflect.Type, value any) (any, error) {
	if from == nil || to.Kind() != reflect.Uint64 {
		return value, nil
	}
	v := reflect.ValueOf(value)
	switch from.Kind() {
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n >= 18446744073709551616.0 || math.Trunc(n) != n {
			return nil, errors.New("resource limit must be a nonnegative integer")
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v.Int() < 0 {
			return nil, errors.New("resource limit must be a nonnegative integer")
		}
	}
	return value, nil
}
