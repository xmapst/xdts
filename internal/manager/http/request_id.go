package http

import (
	"uuid"

	"github.com/gin-gonic/gin"
)

// HeaderRequestID 是请求 ID 在请求头/响应头中使用的字段名。
const HeaderRequestID = "X-Request-Id"

const ctxKeyRequestID = "xdts.request_id"

// RequestID 为每个请求生成（或透传客户端已提供的）请求 ID：写回响应头，
// 并放进 gin.Context 供访问日志与错误响应使用（需求 MGR-WEB-04）。
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := c.GetHeader(HeaderRequestID)
		if id == "" {
			id = uuid.NewV7().String()
		}
		c.Set(ctxKeyRequestID, id)
		c.Writer.Header().Set(HeaderRequestID, id)
		c.Next()
	}
}

// RequestIDFromContext 取出当前请求的请求 ID；不存在时返回空字符串。
func RequestIDFromContext(c *gin.Context) string {
	v, ok := c.Get(ctxKeyRequestID)
	if !ok {
		return ""
	}
	id, _ := v.(string)
	return id
}
