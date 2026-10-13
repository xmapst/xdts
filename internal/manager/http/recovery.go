package http

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/gin-gonic/gin"
)

// Recovery 捕获 handler 中的 panic，把堆栈写进 slog，只回给客户端一个不含
// 内部细节的 500 JSON（需求 MGR-WEB-04、NFR-SEC-07：错误响应不回显内部
// 细节，内部细节只进日志）。
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			r := recover()
			if r == nil {
				return
			}
			slog.LogAttrs(c.Request.Context(), slog.LevelError, "http handler panic",
				slog.Any("recovered", r),
				slog.String("method", c.Request.Method),
				slog.String("path", c.Request.URL.Path),
				slog.String("request_id", RequestIDFromContext(c)),
				slog.String("stack", string(debug.Stack())),
			)
			if !c.Writer.Written() {
				respondError(c, http.StatusInternalServerError, "internal_error", "服务器内部错误", nil)
			}
			c.Abort()
		}()
		c.Next()
	}
}
