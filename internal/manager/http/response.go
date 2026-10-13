// Package http 提供 Manager 的 gin HTTP 服务：路由、中间件与统一响应封装。
package http

import (
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"
)

// Envelope 是统一的 HTTP 响应体结构。成功响应携带 Data，失败响应携带
// Error；两者互斥。RequestID 便于客户端在工单/日志里关联具体请求。
type Envelope struct {
	Success   bool       `json:"success"`
	Data      any        `json:"data,omitempty"`
	Error     *ErrorBody `json:"error,omitempty"`
	RequestID string     `json:"request_id,omitempty"`
}

// ErrorBody 是失败响应体的错误部分。Message 是可以回显给用户的文案；
// 任何内部实现细节（SQL 报错、栈信息等）绝不放进这里，只写日志
//
// Details 是可选的结构化补充信息，只用于业务上明确要求返回结构化清单而
// 不是单一文案的场景（例如 DELETE /nodes/{id} 被阻断时， 明确
// 要求响应体是"阻断者完整清单"而不是计数或布尔）。多数调用方不需要它，
// 继续用 Fail()；需要时用 FailWithDetails()。
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Details any    `json:"details,omitempty"`
}

// OK 写入成功响应。
func OK(c *gin.Context, data any) {
	OKStatus(c, http.StatusOK, data)
}

func OKStatus(c *gin.Context, status int, data any) {
	c.JSON(status, Envelope{
		Success:   true,
		Data:      data,
		RequestID: RequestIDFromContext(c),
	})
}

// Fail 写入业务预期内的错误响应（如参数校验失败），message 可以回显。
func Fail(c *gin.Context, status int, code, message string) {
	respondError(c, status, code, message, nil)
}

// FailWithDetails 与 Fail 相同，额外带一段结构化 details（见 ErrorBody
// 的注释）。
func FailWithDetails(c *gin.Context, status int, code, message string, details any) {
	respondError(c, status, code, message, details)
}

// FailInternal 处理非预期错误：完整错误信息只进日志，响应体只给不含内部
// 细节的通用提示（需求 NFR-SEC-07）。
func FailInternal(c *gin.Context, err error, code string) {
	slog.LogAttrs(c.Request.Context(), slog.LevelError, "http internal error",
		slog.String("code", code),
		slog.String("path", c.Request.URL.Path),
		slog.String("request_id", RequestIDFromContext(c)),
		slog.Any("error", err),
	)
	respondError(c, http.StatusInternalServerError, code, "服务器内部错误", nil)
}

// NotFound 写入 /api 路径下未匹配路由的 JSON 404。
func NotFound(c *gin.Context) {
	respondError(c, http.StatusNotFound, "not_found", "资源不存在", nil)
}

func respondError(c *gin.Context, status int, code, message string, details any) {
	c.JSON(status, Envelope{
		Success:   false,
		Error:     &ErrorBody{Code: code, Message: message, Details: details},
		RequestID: RequestIDFromContext(c),
	})
}
