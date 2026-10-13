package http

import "github.com/gin-gonic/gin"

// healthHandler 是给编排系统用的存活探针（liveness）端点：恒报进程
// 存活，不检查任何下游依赖。
func healthHandler(c *gin.Context) {
	OK(c, gin.H{"status": "ok"})
}
