package http

import (
	"log/slog"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// 使用 slog 包级函数而非注入的 *slog.Logger：SIGHUP 重载日志配置时只需要
// 调用 slog.SetDefault 换掉全局 logger，这里和其余中间件都不用跟着改
// （cmd/manager/main.go 的重载逻辑）。
func AccessLog() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		if raw := c.Request.URL.RawQuery; raw != "" && !strings.HasPrefix(path, "/api/v1/auth/") {
			path += "?" + raw
		}

		c.Next()

		// 鉴权模块接入前，authPlaceholder 不写入 ctxKeyUsername，这里恒为空。
		username, _ := c.Get(ctxKeyUsername)
		usernameStr, _ := username.(string)

		slog.LogAttrs(c.Request.Context(), slog.LevelInfo, "http request",
			slog.String("method", c.Request.Method),
			slog.String("path", path),
			slog.Int("status", c.Writer.Status()),
			slog.Duration("latency", time.Since(start)),
			slog.String("user", usernameStr),
			slog.String("request_id", RequestIDFromContext(c)),
			slog.String("client_ip", c.ClientIP()),
		)
	}
}
