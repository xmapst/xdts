package probe

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/datacheck"
	"github.com/xmapst/xdts/internal/iproto"
)

func (m *Module) probeDataCheck(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Error: &iproto.Error{Code: code, Message: message}}
	}
	args, ep := req.GetDataCheck(), req.GetEndpoint()
	if args == nil || ep == nil || strings.TrimSpace(ep.Host) == "" || ep.Port == 0 || ep.Port > 65535 || req.CenterCode != m.opts.Center || req.PipelineId == "" {
		return fail("DATA_CHECK_INPUT_INVALID", "校验端点、中心或表配置不合法")
	}
	// Existing probe connections do not implement TLS or custom driver options.
	if ep.GetTls().GetEnabled() || len(ep.GetParams()) != 0 {
		return fail("DATA_CHECK_CONNECTION_UNSUPPORTED", "校验探测暂不支持此端点的 TLS 或自定义连接参数")
	}
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", probeMySQLDSN(ep))
	if err != nil {
		return fail("DATA_CHECK_CONNECT_FAILED", "无法连接校验端点")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	result, err := datacheck.NewScanner(db).Scan(ctx, datacheck.LocalTable{Schema: args.Schema, Table: args.Table, Columns: append([]string(nil), args.Columns...)}, datacheck.Options{SampleModulo: args.SampleModulo})
	if err != nil {
		switch {
		case errors.Is(err, datacheck.ErrConfig):
			return fail("DATA_CHECK_CONFIG_INVALID", "校验范围或抽样配置不合法")
		case errors.Is(err, datacheck.ErrSchema):
			return fail("DATA_CHECK_SCHEMA_INVALID", "校验表必须为 InnoDB，并且包含完整主键及有效对应列")
		case errors.Is(err, context.DeadlineExceeded):
			return fail("DATA_CHECK_TIMEOUT", "单表校验超过 120 秒，请缩小范围后重试")
		default:
			return fail("DATA_CHECK_READ_FAILED", "校验读取失败，请检查权限、连接或字段内容")
		}
	}
	raw, err := json.Marshal(result)
	if err != nil {
		return fail("DATA_CHECK_RESULT_INVALID", "校验结果无法编码")
	}
	return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Success: true, Result: &iproto.ProbeResult_DataCheck{DataCheck: &iproto.DataCheckDigest{ResultJson: raw}}}
}
