package config

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/xmapst/xslog"
)

// SetupLogger 是 Node 日志初始化的唯一入口，满足需求 C-06（统一
// log/slog + github.com/xmapst/xslog）。
//
// 返回的 io.Closer 对应日志输出目标：写文件时是 xslog 的轮转文件句柄，需要在
// 重载 / 退出时关闭；写 stdout 时是空操作，调用方始终可以无条件 Close()。
func SetupLogger(cfg LogConfig) (*slog.Logger, io.Closer, error) {
	format := strings.ToLower(cfg.Format)
	if format != "json" && format != "console" {
		return nil, nil, fmt.Errorf("config: 未知的日志格式 %q", cfg.Format)
	}
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, nil, err
	}

	var (
		out    io.Writer
		closer io.Closer = nopCloser{}
	)
	if strings.TrimSpace(cfg.File) == "" {
		out = os.Stdout
	} else {
		f, ferr := xslog.OpenFile(cfg.File,
			xslog.WithMaxSizeMB(cfg.MaxSizeMB),
			xslog.WithMaxBackups(cfg.MaxBackups),
			xslog.WithMaxAgeDays(cfg.MaxAgeDays),
			xslog.WithCompression(xslog.CompressionGzip),
		)
		if ferr != nil {
			return nil, nil, fmt.Errorf("config: 打开日志文件 %s 失败: %w", cfg.File, ferr)
		}
		out, closer = f, f
	}

	opts := []xslog.Option{
		xslog.WithWriter(out),
		xslog.WithLevel(level),
		// 默认不设时 Writer 层的写入失败会被静默吞掉（已在 xslog 勘察中实测确认），
		// 这里显式挂上，避免日志系统自身故障时完全没有信号。
		xslog.WithErrorHandler(func(werr error) {
			// 用标准库 slog 直接写 stderr：此刻正是"日志系统本身坏了"，不能再
			// 依赖被这段代码构建、可能本来就是问题源头的 handler。
			slog.New(slog.NewTextHandler(os.Stderr, nil)).Error("xslog write failed",
				slog.Any("error", werr))
		}),
	}

	var handler slog.Handler
	switch format {
	case "console":
		handler = xslog.NewConsoleHandler(opts...)
	case "json":
		handler = xslog.NewJSONHandler(opts...)
	}

	return slog.New(handler), closer, nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("config: 未知的日志级别 %q", level)
	}
}

// nopCloser 用于 stdout 场景：调用方总是可以无条件调用 Close()。
type nopCloser struct{}

func (nopCloser) Close() error { return nil }
