package logship

import (
	"context"
	"log/slog"
	"strings"

	"github.com/xmapst/xhive/chanrpc"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/safeerror"
)

// Handler 是把 log/slog 记录桥接到 logship 模块的 slog.Handler 实现（NODE-RPT-04"本地
// 文件轮转 + 上报双 Handler"、NODE-RPT-06"支持 slog 的直接注入"）。
//
// 典型用法：上层（cmd/node）用一个小的 fan-out combinator（xslog 本身不提供多目的地
// 分发，见团队勘察结论）把本地文件/控制台 Handler 与 logship.NewHandler(...) 组合成
// slog.SetDefault 的最终 Handler，本包本身不做这层组合，只提供"能不能独立作为一个
// slog.Handler 工作"这一半——组合逻辑属于 cmd/node 的装配职责，不属于 logship 包本身。
type Handler struct {
	mod    *Module
	client *chanrpc.Client
	level  slog.Leveler

	attrs       []slog.Attr
	groupPrefix string
}

// 返回的 Handler 拥有 client 的生命周期：调用方必须在不再需要时调用 Close()
// （通常是进程退出前，与 logship 模块本身的 Close 顺序无关——client 是独立的外部
// 调用方句柄，不是模块内部状态）。WithAttrs/WithGroup 派生出的 Handler 共享同一个
// client，不需要（也不应该）单独 Close。
func NewHandler(mod *Module, level slog.Leveler) *Handler {
	if level == nil {
		level = slog.LevelInfo
	}
	return &Handler{mod: mod, client: chanrpc.NewClient(), level: level}
}

// Close 释放根 Handler 持有的 chanrpc.Client。只应该对 NewHandler 直接返回的那个根
// 实例调用一次；WithAttrs/WithGroup 派生的实例共享同一个 client，调用它们的 Close
// 没有意义（未导出共享标记，调用方按约定使用）。
func (h *Handler) Close() { h.client.Close() }

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level.Level()
}

// Handle transfers an independently owned, flattened and sanitized log record
// to the logship actor. Handler attributes and groups are applied before transfer.
func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	if h.mod.ChanRPC().IsClosed() || h.client.IsClosed() {
		return nil
	}
	fields := make(map[string]string, r.NumAttrs()+len(h.attrs))
	var pipelineID, module string
	apply := func(a slog.Attr) bool {
		key := h.groupPrefix + a.Key
		value := publicLogValue(a)
		switch key {
		case "pipeline_id":
			pipelineID = value
		case "module":
			module = value
		default:
			fields[key] = value
		}
		return true
	}
	for _, a := range h.attrs {
		apply(a)
	}
	r.Attrs(apply)

	rec := &iproto.LogRecord{
		TsMs:       r.Time.UnixMilli(),
		Level:      r.Level.String(),
		Message:    r.Message,
		PipelineId: pipelineID,
		Module:     module,
		Fields:     fields,
	}
	h.client.Cast(h.mod.ChanRPC(), LogRecordMsg{Record: rec})
	return nil
}

// publicLogValue is the log shipping boundary. Local handlers still receive
// the original record, while arbitrary error chains cannot leave this node.
func publicLogValue(a slog.Attr) string {
	v := a.Value.Resolve()
	if v.Kind() == slog.KindAny {
		if err, ok := v.Any().(error); ok {
			return safeerror.Public(err)
		}
	}
	key := strings.ToLower(a.Key)
	if dot := strings.LastIndexByte(key, '.'); dot >= 0 {
		key = key[dot+1:]
	}
	if strings.HasSuffix(key, "_error") || key == "error" || key == "reason" ||
		key == "detail" || key == "dsn" || key == "password" || key == "token" ||
		key == "secret" || key == "credential" || key == "authorization" {
		return safeerror.PublicText(v.String())
	}
	return v.String()
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	n := *h
	n.attrs = append(append([]slog.Attr{}, h.attrs...), attrs...)
	return &n
}

func (h *Handler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	n := *h
	n.groupPrefix = h.groupPrefix + name + "."
	return &n
}
