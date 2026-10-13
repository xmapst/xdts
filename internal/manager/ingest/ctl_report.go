package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
	"gorm.io/gorm"
)

// checkBodyNodeID 校验消息体内自报的 node_id（如果消息类型带这个字段）与
// x-node-id header 解析出的节点身份一致。
//
// 这是防御性的完整性检查，不是安全边界本身：两者并不参与同一次签名计算
// （internal/iproto/sign.go 的 SignParams 只覆盖 MessageID/TimestampMS/Body 三者的哈希，不
// 含 AMQP headers 或 Properties.Type），真正的身份认定始终是"用 header 里
// 的 node_id 对应的 key 验签能不能通过"（internal/manager/ingest/verify.go）——伪造 header 冒充
// 别的节点在没有对应 token 的情况下算不出通过验签的签名。这里额外校验
// body 与 header 是否一致，只是为了在"同一个节点，body 字段写错了"这类
// bug 场景下尽早发现，不是防篡改。
func checkBodyNodeID(bodyNodeID string, headerNodeID uuidx.UUID) error {
	if bodyNodeID == "" {
		return nil
	}
	id, err := uuidx.Parse(bodyNodeID)
	if err != nil {
		return fmt.Errorf("ingest: 消息体 node_id 不是合法 UUID: %w", err)
	}
	if id != headerNodeID {
		return fmt.Errorf("ingest: 消息体 node_id(%s) 与 %s header(%s) 不一致", id, iproto.HeaderNodeID, headerNodeID)
	}
	return nil
}

// 修法是两端共用同一个类型，不是把键名对齐——对齐只修得好今天这一次，
// 下次任一端改字段仍然不报错。共用之后单边改键编译不过。
//
// 必须走 model.EncodeNodeRuntime 落库，不要自己 json.Marshal：它在副本上
// 把 nil Pipelines 规范成 `[]`，因为 `nil` 在读取侧的含义是"这份 runtime 不是
// 当前版本节点写的"，而当前版本的写入方永远不该产出它。绕过编码器，这个
// 保证就从结构性的退回成靠人记得的。

// onNodeHeartbeat 处理 NodeHeartbeat（NODE-RPT-01；MGR-NODE-04/05）。
func (s *Service) onNodeHeartbeat(ctx context.Context, env *iproto.Envelope, headerNodeID uuidx.UUID) error {
	var m iproto.NodeHeartbeat
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &m); err != nil {
		return fmt.Errorf("ingest: 解码 NodeHeartbeat 失败: %w", err)
	}
	if err := checkBodyNodeID(m.GetNodeId(), headerNodeID); err != nil {
		return err
	}

	entries := m.GetPipelines()
	// 非 nil 空切片：承载 0 条线路时要落 `[]` 而不是 `null`，见上方文档与
	// model.EncodeNodeRuntime。这里用 make 而不是 var，编码器还会再兜一次底。
	pipelines := make([]model.NodeRuntimePipeline, 0, len(entries))
	for _, p := range entries {
		pipelines = append(pipelines, model.NodeRuntimePipeline{
			PipelineID:    p.GetPipelineId(),
			ConfigVersion: p.GetConfigVersion(),
			Role:          p.GetRole().String(),
		})
	}

	res := m.GetResource()
	usage := &model.NodeResourceUsage{
		CPUPercent: res.GetCpuPercent(),
		MemBytes:   res.GetMemBytes(),
		Goroutines: res.GetGoroutines(),
	}
	// 以下四个字段（mem_percent/mem_rss_bytes/mem_total_bytes/uptime_seconds）
	// 在 proto 里都是 `optional`，presence 必须逐个显式转写。
	//
	// 复制值而不是复制指针：不把落库结构指进一个解码出来的 proto 消息里，
	// 接收方应当拿到独立所有权。
	//
	// res 需要判 nil：GetResource() 在节点没发 resource 时返回 nil，而下面是
	// 直接字段访问，不像 Get 访问器那样对 nil 接收者安全。
	if res != nil {
		if v := res.MemPercent; v != nil {
			c := *v
			usage.MemPercent = &c
		}
		if v := res.MemRssBytes; v != nil {
			c := *v
			usage.MemRssBytes = &c
		}
		if v := res.MemTotalBytes; v != nil {
			c := *v
			usage.MemTotalBytes = &c
		}
	}

	rt := model.NodeRuntime{
		NodeVersion: m.GetNodeVersion(),
		Resource:    usage,
		Pipelines:   pipelines,
	}
	// m 是值类型（var m iproto.NodeHeartbeat），不会为 nil，可直接取字段。
	if v := m.UptimeSeconds; v != nil {
		c := *v
		rt.UptimeSeconds = &c
	}
	// pipelines_known_count 同为 NodeHeartbeat **顶层** optional 字段（字段 8），
	// 不在 ResourceUsage 里，因此不在上面那个 res != nil 块内。
	//
	// **必须原样转写节点发来的值，绝不能用 uint32(len(pipelines)) 算。**
	// 本字段在 model 里的职责是 Pipelines 数组的 presence 载体与**校验和**，
	// 消费方要验算 PipelinesKnownCount == len(Pipelines)、不一致时判定"这组数
	// 不可信"。若本跳改成由 len 算出来填，两边就**永远相等**，校验和恒真、
	// 再也抓不到任何东西——而它看起来仍然完全正常地工作着。
	//
	// 它与 Pipelines 的三态是互补的：Pipelines 区分"这份 runtime 是不是当前
	// 版本节点写的"，本字段区分"当前版本节点此刻知不知道"。proto3 的 repeated
	// 没有 has 位，"确实承载 0 条"与"镜像链路又断了"在 wire 上是同一串零字节，
	// 这个 presence 就是唯一能分开它们的东西——落成非指针，它就白加了。
	if v := m.PipelinesKnownCount; v != nil {
		c := *v
		rt.PipelinesKnownCount = &c
	}
	if ls := m.GetLocalStore(); ls != nil {
		rt.LocalStore = &model.NodeLocalStoreStats{
			StoreSizeBytes:       ls.GetStoreSizeBytes(),
			LastCommitDurationMs: ls.GetLastCommitDurationMs(),
		}
	}

	raw, err := model.EncodeNodeRuntime(&rt)
	if err != nil {
		return fmt.Errorf("ingest: 序列化心跳 runtime 失败: %w", err)
	}

	if err := s.opts.RecordNodeHeartbeat(ctx, s.opts.DB, headerNodeID, s.now(), raw); err != nil {
		return fmt.Errorf("ingest: 记录心跳失败: %w", err)
	}
	return nil
}

// onPipelineStatus 处理 PipelineStatus（NODE-RPT-02；MGR-PL-04）。
//
// `PIPELINE_ROLE_UNSPECIFIED` 必须拒绝、不能静默当成 source——这里没
// 有"哪个更常见就默认成哪个"的推断，
// 与 pipelineActualStateFromProto 拒绝未知枚举取值是同一条道理——见
// pipelineSideFromProto。拒绝后 process()（internal/manager/ingest/handler.go）会记
// "persist failed" 错误日志，本函数不重复记录。
func (s *Service) onPipelineStatus(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var m iproto.PipelineStatus
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &m); err != nil {
		return fmt.Errorf("ingest: 解码 PipelineStatus 失败: %w", err)
	}
	pipelineID, err := uuidx.Parse(m.GetPipelineId())
	if err != nil {
		return fmt.Errorf("ingest: PipelineStatus.pipeline_id 不是合法 UUID: %w", err)
	}
	state, err := pipelineActualStateFromProto(m.GetState())
	if err != nil {
		return err
	}
	side, err := pipelineSideFromProto(m.GetRole())
	if err != nil {
		return err
	}
	status, err := snapshotStatusFromProto(m.GetSnapshotState())
	if err != nil {
		return err
	}
	r := &model.PipelineStatusRecord{
		PipelineID: pipelineID, Side: side, NodeID: nodeID, RunID: m.GetRunId(),
		ConfigVersion: m.GetConfigVersion(), State: state, LastError: m.GetLastError(),
		SnapshotStatus: status, ObservedAtMS: m.GetTsMs(), SessionID: m.GetStatusSessionId(),
		SessionStartedAtMS: m.GetStatusSessionStartedAtMs(), Sequence: m.GetStatusSeq(),
		RuntimeInstanceID: m.GetRuntimeInstanceId(),
	}
	return s.opts.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		pipeline, err := repo.LockPipelineIncludingDeleted(ctx, tx, pipelineID)
		if err != nil {
			return err
		}
		if (side == model.SideSource && pipeline.SourceNodeID != nodeID) || (side == model.SideTarget && pipeline.TargetNodeID != nodeID) {
			return fmt.Errorf("ingest: pipeline status ownership mismatch")
		}
		if pipeline.DeletedAt.Valid {
			return nil
		}
		previous, err := repo.GetPipelineStatusRecord(ctx, tx, pipelineID, side)
		if err != nil {
			return err
		}
		apply, err := validateStatusObservation(pipeline, previous, r)
		if err != nil || !apply {
			return err
		}
		// Validation and all resulting writes share one row lock and transaction.
		if status != "" {
			if err := repo.RecordSnapshotState(ctx, tx, pipelineID, m.GetRunId(), status); err != nil {
				return err
			}
		}
		if err := s.opts.SetPipelineRoleActualState(ctx, tx, pipelineID, side, state, m.GetLastError()); err != nil {
			return fmt.Errorf("ingest: 按侧写回 pipeline 实际状态失败: %w", err)
		}
		if fullOnlyStopObservation(pipeline, r) {
			if _, err := repo.BumpPipelineCmdSeq(ctx, tx, pipelineID, pipeline.LastCmdSeq); err != nil {
				return err
			}
			if err := repo.SetPipelineDesiredState(ctx, tx, pipelineID, model.DesiredStateStopped); err != nil {
				return err
			}
		}
		return repo.SavePipelineStatusRecord(ctx, tx, r)
	})
}

func pipelineSideFromProto(v iproto.PipelineRole) (model.Side, error) {
	switch v {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		return model.SideSource, nil
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		return model.SideTarget, nil
	default:
		// 含 PIPELINE_ROLE_UNSPECIFIED。
		return "", fmt.Errorf("ingest: 未知的 PipelineRole 取值 %d", v)
	}
}

// pipelineActualStateFromProto 把 proto/report.proto 的 PipelineState 映射为
// model.ActualState。两套词表除 UNSPECIFIED 外逐一对应（proto/common.proto 注释
// 与 internal/manager/model/pipeline.go 的取值表原本就是同一份状态词表）。
func pipelineActualStateFromProto(v iproto.PipelineState) (model.ActualState, error) {
	switch v {
	case iproto.PipelineState_PIPELINE_STATE_DRAFT:
		return model.ActualStateDraft, nil
	case iproto.PipelineState_PIPELINE_STATE_PRECHECKING:

		return model.ActualStatePrechecking, nil
	case iproto.PipelineState_PIPELINE_STATE_DEPLOYING:
		return model.ActualStateDeploying, nil
	case iproto.PipelineState_PIPELINE_STATE_MIGRATING:

		return model.ActualStateMigrating, nil
	case iproto.PipelineState_PIPELINE_STATE_SNAPSHOTTING:
		// 全量读取和目标快照接收已上报该状态。
		return model.ActualStateSnapshotting, nil
	case iproto.PipelineState_PIPELINE_STATE_CATCHING_UP:
		// 目标侧快照收敛后由 amqpin 上报增量追赶，水位收敛后转为 RUNNING。
		return model.ActualStateCatchingUp, nil
	case iproto.PipelineState_PIPELINE_STATE_RUNNING:
		return model.ActualStateRunning, nil
	case iproto.PipelineState_PIPELINE_STATE_PAUSED:
		return model.ActualStatePaused, nil
	case iproto.PipelineState_PIPELINE_STATE_ERROR:
		return model.ActualStateError, nil
	case iproto.PipelineState_PIPELINE_STATE_STOPPED:
		return model.ActualStateStopped, nil
	default:
		// 含 PIPELINE_STATE_UNSPECIFIED："对协议…中的未知枚举
		// 和非法数值应明确拒绝"，不静默写入权威状态。
		return "", fmt.Errorf("ingest: 未知的 PipelineState 取值 %d", v)
	}
}

func (s *Service) onPipelineProgress(ctx context.Context, env *iproto.Envelope, nodeID uuidx.UUID) error {
	var m iproto.PipelineProgress
	if err := iproto.Decode(env.Body, env.Properties.ContentEncoding, &m); err != nil {
		return fmt.Errorf("ingest: 解码 PipelineProgress 失败: %w", err)
	}
	pipelineID, err := uuidx.Parse(m.GetPipelineId())
	if err != nil {
		return fmt.Errorf("ingest: PipelineProgress.pipeline_id 不是合法 UUID: %w", err)
	}
	record, err := progressRecord(&m, nodeID)
	if err != nil {
		return err
	}
	return s.opts.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := repo.ValidateProgressOwnershipTx(ctx, tx, record)
		if err != nil || !current {
			return err
		}
		if snapshot := m.GetSnapshot(); snapshot != nil {
			finalTotal, finalDone, err := iproto.SnapshotProgressCounts(snapshot)
			if err != nil {
				return err
			}
			var extension *model.SnapshotPlanExtensionProgress
			if ext := snapshot.GetExtension(); ext != nil {
				if err := repo.ValidateSnapshotExtensionSupport(ctx, tx, pipelineID); err != nil {
					return err
				}
				extension = &model.SnapshotPlanExtensionProgress{Sequence: ext.GetSequence(), TotalChunks: ext.GetTotalChunks(), DoneChunks: ext.GetDoneChunks()}
			}
			switch m.GetRole() {
			case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
				if finalDone != finalTotal || len(snapshot.GetTables()) != 0 {
					return fmt.Errorf("ingest: snapshot catchup progress ownership or shape mismatch")
				}
				if err := repo.RecordSnapshotCatchupProgressWithExtension(ctx, tx, pipelineID, m.GetRunId(), snapshot.GetTotalChunks(), snapshot.GetCatchupRemainingTxns(), m.GetTsMs(), extension); err != nil {
					return err
				}
			case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
				if snapshot.GetCatchupRemainingTxns() != 0 {
					return fmt.Errorf("ingest: snapshot source progress ownership or shape mismatch")
				}
				var tables []model.PipelineSnapshotTable
				for _, table := range snapshot.GetTables() {
					status := model.SnapshotTableStatusPending
					switch table.GetStatus() {
					case iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_PENDING:
					case iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_READING:
						status = model.SnapshotTableStatusReading
					case iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_DONE:
						status = model.SnapshotTableStatusDone
					case iproto.SnapshotTableStatus_SNAPSHOT_TABLE_STATUS_FAILED:
						status = model.SnapshotTableStatusFailed
					default:
						return fmt.Errorf("ingest: invalid snapshot table status")
					}
					var total, done *uint64
					var rate *float64
					if table.TotalChunks != nil {
						v := *table.TotalChunks
						total = &v
					}
					if table.DoneChunks != nil {
						v := *table.DoneChunks
						done = &v
					}
					if table.RowsPerSec != nil {
						v := *table.RowsPerSec
						rate = &v
					}
					tables = append(tables, model.PipelineSnapshotTable{PipelineID: pipelineID, RunID: m.GetRunId(), SchemaName: table.GetSchema(), Table: table.GetTable(), EstRows: table.GetEstimatedRows(), DoneRows: table.GetDoneRows(), Status: status, TotalChunks: total, DoneChunks: done, RowsPerSec: rate, ObservedAtMS: m.GetTsMs()})
				}
				if err := repo.RecordSnapshotProgressWithExtension(ctx, tx, pipelineID, m.GetRunId(), snapshot.GetTotalChunks(), snapshot.GetDoneChunks(), tables, extension); err != nil {
					return err
				}
			default:
				return fmt.Errorf("ingest: snapshot progress role invalid")
			}
		}

		persist := s.opts.RecordPipelineProgress
		if cp := m.GetSourceCheckpoint(); cp != nil {
			if m.GetRole() != iproto.PipelineRole_PIPELINE_ROLE_SOURCE || cp.PipelineId != m.PipelineId || cp.RunId != m.RunId || cp.NodeId != nodeID.String() || cp.ObservedTsMs > m.TsMs || iproto.ValidateSourceCheckpoint(cp) != nil {
				return fmt.Errorf("ingest: invalid source recovery observation")
			}
			if err := repo.RecordSourceCheckpoint(ctx, tx, nodeID, cp); err != nil {
				return err
			}
		}
		if persist == nil {
			persist = repo.RecordPipelineProgress
		}
		return persist(ctx, tx, record)
	})
}

// progressRecord preserves complete diagnostics and validates floating point and
// unsigned values before narrowing them to history columns.
func progressRecord(m *iproto.PipelineProgress, nodeID uuidx.UUID) (model.PipelineProgressRecord, error) {
	id, err := uuidx.Parse(m.GetPipelineId())
	if err != nil {
		return model.PipelineProgressRecord{}, fmt.Errorf("ingest: invalid progress pipeline id")
	}
	side, err := pipelineSideFromProto(m.GetRole())
	if err != nil {
		return model.PipelineProgressRecord{}, err
	}
	values := model.ProgressValues{
		ReadGTIDSet: m.GetReadGtidSet(), PublishedGTIDSet: m.GetPublishedGtidSet(), AppliedGTIDSet: m.GetAppliedGtidSet(),
		SourceEventTSMS: m.GetSourceEventTsMs(), LagMS: m.GetLagMs(), RowsPerSec: m.GetRowsPerSec(), TxnsPerSec: m.GetTxnsPerSec(), BytesPerSec: m.GetBytesPerSec(),
		InflightBatches: m.GetInflightBatches(), InflightBytes: m.GetInflightBytes(), DataQueueBacklog: m.GetDataQueueBacklog(), ErrorCount: m.GetErrorCount(), ConflictCount: m.GetConflictCount(), ModuleQueues: []model.ProgressQueue{},
	}
	if m.MetricsObservedTsMs != nil {
		v := *m.MetricsObservedTsMs
		values.MetricsObservedTSMS = &v
	}
	if m.QueueObservedTsMs != nil {
		v := *m.QueueObservedTsMs
		values.QueueObservedTSMS = &v
	}
	for _, q := range m.GetModuleQueues() {
		if q == nil {
			return model.PipelineProgressRecord{}, fmt.Errorf("ingest: nil module queue")
		}
		values.ModuleQueues = append(values.ModuleQueues, model.ProgressQueue{Module: q.GetModule(), Len: q.GetLen(), Cap: q.GetCap()})
	}
	if m.GetSnapshot() != nil {
		raw, err := protojson.Marshal(m.GetSnapshot())
		if err != nil {
			return model.PipelineProgressRecord{}, err
		}
		values.Snapshot = model.JSON(raw)
	}
	r := model.PipelineProgressRecord{PipelineID: id, NodeID: nodeID, Side: side, RunID: m.GetRunId(), ObservedAt: time.UnixMilli(m.GetTsMs()), Values: values}
	return r, r.Validate()
}
