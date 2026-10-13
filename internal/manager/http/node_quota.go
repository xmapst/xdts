package http

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/xmapst/xdts/internal/manager/repo"
)

func writeNodeQuotaError(c *gin.Context, err error) bool {
	var quota *repo.NodePipelineQuotaError
	if !errors.As(err, &quota) {
		return false
	}
	FailWithDetails(c, http.StatusConflict, "node_pipeline_quota_exceeded", "节点 pipeline 配额不足，请提高配额或等待流水线回收完成", map[string]any{
		"nodeId": quota.NodeID.String(), "maxPipelines": quota.Limit, "assignedPipelines": quota.Assigned,
	})
	return true
}
