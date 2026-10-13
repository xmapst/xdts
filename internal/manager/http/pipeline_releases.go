// 只读、不分页游标、只按 config_version 从新到旧展示：与 pipelineHandler.
// list 的既有分页写法完全一致（page/pageSize + paginated[T]，见
// internal/manager/http/pagination.go），不新发明形状。
package http

import (
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/redact"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// maxPipelineReleaseMessageLen 是 pipelineReleaseView.Message 暴露给前端
// 前的长度上限（rune 数）。message 内容来自节点自己上报的
// ConfigApplyResult.Error（见 internal/manager/dispatch/ctl_config_apply_result.go），可能
// 是 internal/rule.Compile 这类 errors.Join 拼接出的多行详情，理论上没有
// 长度上限；截断只是防御性的"别把一个异常巨大的字符串原样吐给前端"，跟
// 下面 redact.PotentialCredentials 要防的"内容里混进凭据"是两个独立的
// 问题，不能互相替代。
const maxPipelineReleaseMessageLen = 2000

// sanitizePipelineReleaseMessage 是 pipelineReleaseView.Message 的唯一
// 产出入口：先脱敏、再截断——顺序不能反，反过来可能把凭据正好截断在
// 长度上限的边界上，留下半截明文。
func sanitizePipelineReleaseMessage(raw string) string {
	msg := redact.PotentialCredentials(raw)
	runes := []rune(msg)
	if len(runes) > maxPipelineReleaseMessageLen {
		msg = string(runes[:maxPipelineReleaseMessageLen]) + "...(truncated)"
	}
	return msg
}

// pipelineReleaseView 是 GET /pipelines/:id/releases 的单条记录形状。
//
// 不暴露 ConfigDigest：一段 sha256 十六进制摘要对运维排障没有可操作的
// 意义（与 pipelineView 不暴露 binlogServerId 同一个"内部机制字段不该
// 出现在响应里"的判断，见该类型的注释），只在 dispatch 包内部用来比较
// 两次组装结果是否相同。
type pipelineReleaseView struct {
	ID            string     `json:"id"`
	ConfigVersion uint64     `json:"configVersion"`
	Side          string     `json:"side"`
	NodeID        string     `json:"nodeId"`
	Status        string     `json:"status"`
	Message       string     `json:"message,omitempty"`
	CreatedAt     time.Time  `json:"createdAt"`
	AppliedAt     *time.Time `json:"appliedAt,omitempty"`
}

func toPipelineReleaseView(r model.PipelineRelease) pipelineReleaseView {
	return pipelineReleaseView{
		ID:            r.ID.String(),
		ConfigVersion: r.ConfigVersion,
		Side:          string(r.Side),
		NodeID:        r.NodeID.String(),
		Status:        string(r.Status),
		Message:       sanitizePipelineReleaseMessage(r.Message),
		CreatedAt:     r.CreatedAt,
		AppliedAt:     r.AppliedAt,
	}
}

// 配置回执按 config_version 分组，再按对应侧展示应用结果。
func (h *pipelineHandler) listReleases(c *gin.Context) {
	if !requireDB(c, h.db) {
		return
	}
	id, err := uuidx.Parse(c.Param("id"))
	if err != nil {
		Fail(c, http.StatusBadRequest, "invalid_id", "id 格式不合法")
		return
	}

	ctx := c.Request.Context()
	if _, err := repo.GetPipeline(ctx, h.db, id); err != nil {
		if errors.Is(err, repo.ErrPipelineNotFound) {
			Fail(c, http.StatusNotFound, "not_found", "pipeline 不存在")
			return
		}
		FailInternal(c, err, "get_pipeline_failed")
		return
	}

	releases, err := repo.ListPipelineReleases(ctx, h.db, id)
	if err != nil {
		FailInternal(c, err, "list_pipeline_releases_failed")
		return
	}

	views := make([]pipelineReleaseView, 0, len(releases))
	for _, r := range releases {
		views = append(views, toPipelineReleaseView(r))
	}

	page := parsePositiveIntOrDefault(c.Query("page"), 1)
	pageSize := min(parsePositiveIntOrDefault(c.Query("pageSize"), defaultListPageSize), maxListPageSize)

	total := len(views)
	start := min((page-1)*pageSize, total)
	end := min(start+pageSize, total)

	OK(c, paginated[pipelineReleaseView]{
		Items:    views[start:end],
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	})
}
