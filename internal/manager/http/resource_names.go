package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/repo"
)

func writeResourceNameError(c *gin.Context, err error) bool {
	switch {
	case errors.Is(err, repo.ErrResourceNameInvalid):
		Fail(c, http.StatusBadRequest, "invalid_name", "名称不能为空，且不能超过 128 个字符")
	case errors.Is(err, repo.ErrNodeNameConflict):
		Fail(c, http.StatusConflict, "node_name_conflict", "节点名称已存在")
	case errors.Is(err, repo.ErrCenterNameConflict):
		Fail(c, http.StatusConflict, "center_name_conflict", "中心名称已存在")
	case errors.Is(err, repo.ErrEndpointNameConflict):
		Fail(c, http.StatusConflict, "endpoint_name_conflict", "端点名称已存在")
	default:
		return false
	}
	return true
}
