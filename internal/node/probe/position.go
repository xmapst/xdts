package probe

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/position"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/pkg/uuidx"
)

func (m *Module) probePosition(ctx context.Context, req *iproto.ProbeRequest) *iproto.ProbeResult {
	fail := func(code, message string) *iproto.ProbeResult {
		return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Error: &iproto.Error{Code: code, Message: message}}
	}
	ep, args := req.GetEndpoint(), req.GetPosition()
	if ep == nil || strings.TrimSpace(ep.GetHost()) == "" || ep.GetPort() == 0 || ep.GetPort() > 65535 || args == nil {
		return fail("POSITION_INPUT_INVALID", "位点探测参数不合法")
	}
	if (args.Kind == iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP || args.Kind == iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS) && args.GetBinlogServerId() == 0 {
		return fail("POSITION_ID_REQUIRED", "复制协议位点探测需要独立的 server_id")
	}
	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	db, err := sql.Open("mysql", probeMySQLDSN(ep))
	if err != nil {
		return fail("POSITION_INPUT_INVALID", "端点连接参数不合法")
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)
	conn := position.Conn{Host: ep.Host, Port: uint16(ep.Port), User: ep.Username, Password: ep.Password, ServerID: args.BinlogServerId}
	out := &iproto.PositionResult{}
	if args.IncludeCheckpoint {
		id, err := uuidx.Parse(args.CheckpointPipelineId)
		if err != nil || id.IsZero() || m.opts.Store == nil {
			return fail("POSITION_CHECKPOINT_UNAVAILABLE", "源节点检查点不可用")
		}
		var doc map[string]any
		err = m.opts.Store.Get(queryCtx, "checkpoints", args.CheckpointPipelineId+".source", &doc)
		if err != nil && !errors.Is(err, xdoc.ErrNotFound) {
			return fail("POSITION_CHECKPOINT_UNAVAILABLE", "源节点检查点读取失败")
		}
		if err == nil {
			run, valid := store.CheckpointRun(doc)
			if !valid || run > args.CheckpointRunId {
				return fail("POSITION_CHECKPOINT_CHANGED", "源节点检查点运行标识无效")
			}
		}
		if err == nil && store.MatchesCheckpointRun(doc, args.CheckpointRunId) {
			revision, valid := store.CheckpointPositionRevision(doc)
			epoch, epochValid := store.CheckpointEpoch(doc)
			set, present := store.CheckpointPublishedGTID(doc)
			_, parseErr := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
			if !valid || !epochValid || !present || parseErr != nil || revision > args.PositionRevision {
				return fail("POSITION_CHECKPOINT_CHANGED", "源节点检查点不属于当前位点修订")
			}
			out.PublishedGtidSet, out.CheckpointEpoch, out.CheckpointPositionRevision = &set, epoch, revision
		}
	}
	switch args.GetKind() {
	case iproto.InitPositionKind_INIT_POSITION_KIND_LATEST:
		out.GtidSet, err = position.ResolveLatest(queryCtx, db)
	case iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET:
		var warning string
		out.GtidSet, warning, err = position.ResolveGTIDSet(queryCtx, db, args.GtidSet)
		if warning != "" {
			out.Warnings = []string{warning}
		}
	case iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS:
		if args.GetBinlogOffset() > math.MaxUint32 {
			return fail("POSITION_INPUT_INVALID", "偏移超过复制协议范围")
		}
		out.GtidSet, err = position.ResolveFileOffset(queryCtx, db, conn, args.BinlogFile, uint32(args.BinlogOffset))
		out.BinlogFile, out.BinlogOffset = args.BinlogFile, args.BinlogOffset
	case iproto.InitPositionKind_INIT_POSITION_KIND_TIMESTAMP:
		var found position.TimestampResult
		found, err = position.ResolveTimestamp(queryCtx, db, conn, args.TsMs)
		out.GtidSet, out.BinlogFile, out.BinlogOffset, out.TxnCommitTsMs = found.GTIDSet, found.File, uint64(found.Offset), found.CommitTSMS
	default:
		return fail("POSITION_INPUT_INVALID", "未知位点类型")
	}
	if err != nil {
		if boundary, ok := errors.AsType[*position.BoundaryError](err); ok {
			out.Boundary = &iproto.PositionBoundary{File: boundary.File, Requested: uint64(boundary.Requested), Preceding: uint64(boundary.Preceding)}
			if boundary.FollowingValid {
				following := uint64(boundary.Following)
				out.Boundary.Following = &following
			}
			return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Error: &iproto.Error{Code: "POSITION_BOUNDARY", Message: "偏移未对齐事务起点，请选择前后最近边界"}, Result: &iproto.ProbeResult_Position{Position: out}}
		}
		switch {
		case errors.Is(err, position.ErrTimestampNotFound):
			return fail("POSITION_TIMESTAMP_NOT_FOUND", "截止位置之前没有符合该时间点的事务")
		case errors.Is(err, position.ErrTimestampHistory):
			return fail("POSITION_TIMESTAMP_HISTORY", "保留的 binlog 无法证明该时间点的完整历史")
		case errors.Is(err, position.ErrTimestampLimit):
			return fail("POSITION_SCAN_LIMIT", "时间点扫描超过 256 文件或 512 MiB 上限")
		case errors.Is(err, position.ErrPurgedNotSubset):
			return fail("POSITION_PURGED", "起点缺少已清理的 binlog 历史")
		default:
			return fail("POSITION_QUERY_FAILED", "源节点未能完整验证起始位点，请检查数据库连接、复制权限及保留日志")
		}
	}
	return &iproto.ProbeResult{TaskId: req.TaskId, Type: req.Type, Success: true, Result: &iproto.ProbeResult_Position{Position: out}}
}
