package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// onLogBatch 处理 LogBatch（NODE-RPT-04；MGR-MON-02/09）。
//
// 一条 AMQP 消息对应一次 InsertLogs 调用，不在多条 AMQP 消息之间攒批：
//  1. xdts.manager.log 队列本身允许丢（x-max-length + drop-head，
//     ManagerLogQueueOptions），没有必要为了"攒更大批量提高吞吐"额外引入
//     跨消息状态与失败处理复杂度；
//  2. 这样每条 AMQP 投递与一次落库调用一一对应，process()（internal/manager/ingest/handler.go）
//     "处理完就 Ack，用日志+计数器代替 Nack"的策略不需要再考虑"这一批里
//     有几条 AMQP 消息该一起重投"这层额外维度——批次内部的分块提交语义
//     （InsertLogs 自己的 500 行 chunk）已经是本函数需要面对的全部复杂度。
func (s *Service) onLogBatch(ctx context.Context, env *iproto.Envelope, headerNodeID uuidx.UUID) error {
	var m iproto.LogBatch
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &m); err != nil {
		return fmt.Errorf("ingest: 解码 LogBatch 失败: %w", err)
	}
	if err := checkBodyNodeID(m.GetNodeId(), headerNodeID); err != nil {
		return err
	}

	records := m.GetRecords()
	if len(records) == 0 {
		return nil
	}
	rows := make([]model.Log, 0, len(records))
	for i, r := range records {
		level, err := parseLogLevel(r.GetLevel())
		if err != nil {
			return fmt.Errorf("ingest: LogBatch 第 %d 条记录: %w", i, err)
		}

		var pipelineID uuidx.UUID
		if pid := r.GetPipelineId(); pid != "" {
			pipelineID, err = uuidx.Parse(pid)
			if err != nil {
				return fmt.Errorf("ingest: LogBatch 第 %d 条记录 pipeline_id 不是合法 UUID: %w", i, err)
			}
		}

		var attrs model.JSON
		if fields := r.GetFields(); len(fields) > 0 {
			raw, err := json.Marshal(fields)
			if err != nil {
				return fmt.Errorf("ingest: LogBatch 第 %d 条记录 fields 序列化失败: %w", i, err)
			}
			attrs = model.JSON(raw)
		}

		rows = append(rows, model.Log{
			Ts:         time.UnixMilli(r.GetTsMs()),
			NodeID:     headerNodeID,
			PipelineID: pipelineID,
			Level:      level,
			Module:     r.GetModule(),
			Message:    r.GetMessage(),
			Attrs:      attrs,
			Source:     model.LogSourceNode,
		})
	}

	if err := s.opts.InsertLogs(ctx, s.opts.DB, rows); err != nil {
		return fmt.Errorf("ingest: 批量写入日志失败（记录数=%d）: %w", len(rows), err)
	}
	return nil
}

// parseLogLevel 把 LogRecord.level（协议注释："DEBUG/INFO/WARN/ERROR"）
// 转换为 model.LogLevel（小写取值）。两套词表除大小写外完全一致，不需要
// 像 parseEventLevel 那样再做一层拼写别名映射。
func parseLogLevel(s string) (model.LogLevel, error) {
	lv := model.LogLevel(strings.ToLower(strings.TrimSpace(s)))
	if !lv.Valid() {
		return "", fmt.Errorf("ingest: 未知的日志级别 %q", s)
	}
	return lv, nil
}
