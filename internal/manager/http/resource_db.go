package http

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

// requireDB 在管理库不可用时返回 503，调用方必须立即结束请求。
// 允许无数据库构造 Server，便于独立验证鉴权和健康检查路由。
func requireDB(c *gin.Context, db *gorm.DB) bool {
	if db == nil {
		Fail(c, http.StatusServiceUnavailable, "database_unavailable", "服务暂不可用，请稍后重试")
		return false
	}
	return true
}
