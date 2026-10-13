package task

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/pkg/uuidx"
)

// DefaultInspectionInterval 是巡检循环未显式配置时的轮询间隔。Broker
// Management API 查询是轻量只读调用，1 分钟足够及时发现故障（界面"数据
// 新鲜度"不会长期显示明显滞后的探测结果），也不会对各中心的 Management
// API 造成有意义的轮询压力。
const DefaultInspectionInterval = time.Minute

// IProbeDispatcher 定义巡检派发 Broker 探测并等待对应 task_id 回执的能力。
// 生产实现为 RPCProbeDispatcher；探测结果由巡检循环验证并持久化。
type IProbeDispatcher interface {
	// DispatchBrokerProbe 向 node 下发一次 PROBE_TYPE_BROKER 探测请求
	// （center 提供 Broker Management API 凭据与 center_code），taskID
	// 由调用方生成，写入 proto ProbeRequest.task_id（既有协议约定：节点
	// 回执必须带同一 task_id）。
	//
	// 返回的 error 只表示"请求本身没有走完全程"（签名/发布/超时等 RPC
	// 层失败），不代表探测这项业务失败——业务失败通过返回值
	// ProbeResult.Success=false 表达，调用方必须分别处理这两种失败
	// （一种没有 ProbeResult 可用，另一种有）。
	DispatchBrokerProbe(ctx context.Context, center model.Center, node model.Node, taskID string) (*iproto.ProbeResult, error)

	// 错误语义与 DispatchBrokerProbe 相同：返回的 error 只表示"请求本身没有走完
	// 全程"，业务失败由 ProbeResult.Success=false 表达，两者必须分别处理。
	DispatchConnTest(ctx context.Context, endpoint model.Endpoint, node model.Node, taskID string) (*iproto.ProbeResult, error)
	DispatchCatalog(ctx context.Context, endpoint model.Endpoint, node model.Node, taskID string) (*iproto.ProbeResult, error)
}

// InspectionOptions 配置巡检循环。
type InspectionOptions struct {
	DB *gorm.DB

	Dispatcher IProbeDispatcher
	// Interval <=0 时使用 DefaultInspectionInterval。
	Interval time.Duration

	EndpointProbeEnabled bool
}

// InspectionLoop 是巡检节点自动选择 + 派发 + 结果落库的后台任务，模式
// 与 PartitionLoop/reclaim.Loop 一致：常驻 goroutine + 定时 tick + 可测试的
// RunOnceAt。
type InspectionLoop struct {
	db       *gorm.DB
	interval time.Duration

	// dispatcherMu 保护 dispatcher：cmd/manager/main.go 在 AMQP 连接建立
	// 后台重试成功时会调用 SetDispatcher 热插入真正的派发能力（与
	// reclaim.Loop.SetQueues 同一个理由），这与 tick 所在的常驻 goroutine
	// 并发访问同一个字段，需要加锁。
	dispatcherMu sync.RWMutex
	dispatcher   IProbeDispatcher

	// endpointProbeEnabled 在构造时定死，不像 dispatcher 那样热插——它是
	// 运维旋钮不是就绪状态，改它要重启（与其余配置项一致），因此不加锁。
	endpointProbeEnabled bool
	manualQueue          chan manualEndpointProbe
	manualMu             sync.Mutex
	manualPending        map[uuidx.UUID]int
	precheckSlots        chan struct{}
}

func NewInspectionLoop(opts InspectionOptions) *InspectionLoop {
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultInspectionInterval
	}
	return &InspectionLoop{
		db:                   opts.DB,
		dispatcher:           opts.Dispatcher,
		interval:             interval,
		endpointProbeEnabled: opts.EndpointProbeEnabled,
		manualQueue:          make(chan manualEndpointProbe, 64),
		manualPending:        make(map[uuidx.UUID]int),
		precheckSlots:        make(chan struct{}, 4),
	}
}

// SetDispatcher 在巡检循环已经在运行之后热插入（或替换）探测派发能力，
// 典型场景与 reclaim.Loop.SetQueues 完全一致：`cmd/manager/main.go` 里 AMQP
// 连接一开始
// 未接通、循环先以 nil Dispatcher 启动，等后台的连接重试 goroutine 真正
// 连上 AMQP 之后再调用它，循环不需要重启、下一次 tick 就会用上新的实现。
func (l *InspectionLoop) SetDispatcher(d IProbeDispatcher) {
	l.dispatcherMu.Lock()
	l.dispatcher = d
	l.dispatcherMu.Unlock()
}

func (l *InspectionLoop) dispatcherSnapshot() IProbeDispatcher {
	l.dispatcherMu.RLock()
	defer l.dispatcherMu.RUnlock()
	return l.dispatcher
}

// RunOnce 对全部中心各跑一轮巡检，用 time.Now() 作为基准（控制层读取
// 当前时间）。
func (l *InspectionLoop) RunOnce(ctx context.Context) error {
	return l.RunOnceAt(ctx, time.Now())
}

// RunOnceAt 是 RunOnce 的可测试版本：now 由调用方显式传入，测试用它构造
// 确定性的探测结果落库时间戳。每个中心独立处理，一个中心失败（Warn/Error
// 级别日志，取决于失败类型，见各子函数）不影响其它中心继续尝试——与
// PartitionLoop.RunOnceAt 同一个模式（"两张表的分区状态互不相干"对应这里
// "各中心的巡检指派/探测互不相干"）。
//
// 返回 errors.Join 合并后的错误，供测试判断"这一轮是否发生过失败"；生产
// 路径（Run）不使用返回值，每一轮都是对上一轮失败的自动重试。
func (l *InspectionLoop) RunOnceAt(ctx context.Context, now time.Time) error {
	centers, err := repo.ListCenters(ctx, l.db)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: list centers failed", slog.Any("error", err))
		return err
	}

	dispatcher := l.dispatcherSnapshot()

	var errs []error
	for _, center := range centers {
		if err := l.inspectCenter(ctx, center, dispatcher, now); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// inspectCenter 处理单个中心的一轮巡检：校验/修复巡检节点指派 → 若有可用
// 节点则派发探测 → 落库结果。
func (l *InspectionLoop) inspectCenter(ctx context.Context, center model.Center, dispatcher IProbeDispatcher, now time.Time) error {
	node, err := l.ensureInspector(ctx, &center, now)
	if err != nil {
		return err
	}
	if node == nil {

		return nil
	}

	if dispatcher == nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: dispatcher not configured, skip probes",
			slog.String("center_id", center.ID.String()), slog.String("center_code", center.Code))
		return nil
	}

	brokerErr := l.probeBrokerFor(ctx, center, node, dispatcher, now)
	endpointErr := l.probeEndpoints(ctx, center, *node, dispatcher, now)
	return errors.Join(brokerErr, endpointErr)
}

// probeBrokerFor 是原 inspectCenter 的 broker 探测段，语义逐字不变，只是
// 拆出来好与端点探测并列。
func (l *InspectionLoop) probeBrokerFor(ctx context.Context, center model.Center, node *model.Node, dispatcher IProbeDispatcher, now time.Time) error {
	taskID := uuidx.New().String()
	result, err := dispatcher.DispatchBrokerProbe(ctx, center, *node, taskID)
	if err != nil {
		// Verify reply identity and signature before persisting observations.
		// Invalid replies cannot overwrite the previously trusted endpoint metadata.
		code := "DISPATCH_FAILED"
		message := "探测请求未能完成往返，详见 manager 日志"
		switch {
		case errors.Is(err, iproto.ErrSignatureInvalid):
			code = "SIGNATURE_INVALID"
			message = "探测回执验签未通过（签名与节点密钥不匹配），详见 manager 日志"

			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: recorded signature-invalid probe result",
				slog.String("center_id", center.ID.String()), slog.String("node_id", node.ID.String()),
				slog.String("task_id", taskID))
		case errors.Is(err, iproto.ErrTimestampOutOfWindow):
			code = "TIMESTAMP_OUT_OF_WINDOW"
			message = "探测回执验签未通过（时间戳超出允许窗口，可能是时钟漂移或重放），详见 manager 日志"
			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: recorded timestamp-out-of-window probe result",
				slog.String("center_id", center.ID.String()), slog.String("node_id", node.ID.String()),
				slog.String("task_id", taskID))
		case errors.Is(err, ErrProbeResultUnverified):
			message = "探测回执验签未通过，详见 manager 日志"
			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: broker probe reply verify failed",
				slog.String("center_id", center.ID.String()), slog.String("node_id", node.ID.String()),
				slog.String("task_id", taskID))
		default:
			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: broker probe dispatch failed",
				slog.String("center_id", center.ID.String()), slog.String("node_id", node.ID.String()),
				slog.String("task_id", taskID), slog.Any("error", err))
		}

		if recErr := l.recordFailure(ctx, center.ID, now, code, message); recErr != nil {
			return recErr
		}
		return err
	}

	if result.GetTaskId() != taskID {
		// 理论上不应该发生：node/probe 的硬约束 2 要求回执必须带与请求
		// 相同的 task_id，RPCClient 自己也按 correlation_id 匹配回复。
		// 这里只做一次防御性的 Warn 记录，不阻断落库——correlation_id
		// 匹配才是权威的关联机制，task_id 不一致更可能是节点侧实现有
		// 缺陷，需要被看到，但不应该让探测结果因此丢失。
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: probe result task_id mismatch",
			slog.String("center_id", center.ID.String()), slog.String("node_id", node.ID.String()),
			slog.String("expected_task_id", taskID), slog.String("got_task_id", result.GetTaskId()))
	}

	return l.persistResult(ctx, center.ID, now, result)
}

// ensureInspector validates or replaces the center's assigned live probe node.
func (l *InspectionLoop) ensureInspector(ctx context.Context, center *model.Center, now time.Time) (*model.Node, error) {
	nodes, err := repo.ListNodes(ctx, l.db, center.ID)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: list nodes failed",
			slog.String("center_id", center.ID.String()), slog.Any("error", err))
		return nil, err
	}

	if !center.InspectorNodeID.IsZero() {
		for _, n := range nodes {
			if n.ID == center.InspectorNodeID {
				if n.Status == model.NodeStatusOnline {
					return &n, nil
				}
				break
			}
		}
	}

	candidate := pickInspectorCandidate(nodes)
	if candidate == nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: no online node available for inspector auto-select",
			slog.String("center_id", center.ID.String()), slog.String("center_code", center.Code))
		return nil, nil
	}

	if candidate.ID == center.InspectorNodeID {
		// 当前指派的节点这一刻状态不是 Online（上面的循环没有 return），
		// 但候选算出来仍是同一个节点（比如它是唯一在线节点、或者恰好
		// 又变回了在线）——不需要写一次没有变化的更新。
		return candidate, nil
	}

	if err := repo.SetCenterInspectorNode(ctx, l.db, center.ID, candidate.ID, center.LockVersion); err != nil {
		if errors.Is(err, repo.ErrCenterVersionConflict) {

			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: inspector auto-switch lost to concurrent update, skip this round",
				slog.String("center_id", center.ID.String()), slog.String("center_code", center.Code))
			return nil, nil
		}
		slog.LogAttrs(ctx, slog.LevelError, "inspection: set inspector node failed",
			slog.String("center_id", center.ID.String()), slog.String("candidate_node_id", candidate.ID.String()),
			slog.Any("error", err))
		return nil, err
	}

	slog.LogAttrs(ctx, slog.LevelInfo, "inspection: inspector auto-switched",
		slog.String("center_id", center.ID.String()), slog.String("center_code", center.Code),
		slog.String("node_id", candidate.ID.String()))
	return candidate, nil
}

func pickInspectorCandidate(nodes []model.Node) *model.Node {
	var online []model.Node
	for _, n := range nodes {
		if n.Status == model.NodeStatusOnline {
			online = append(online, n)
		}
	}
	if len(online) == 0 {
		return nil
	}
	sort.Slice(online, func(i, j int) bool { return online[i].ID.Compare(online[j].ID) < 0 })
	return &online[0]
}

// probeFailureResult/probeFailureError 是探测未成功（不论是请求本身没走完
// RPC 全程，还是节点侧执行后回执 Success=false）时落库到
// centers.last_broker_probe_result 的 JSON 结构。
type probeFailureResult struct {
	Success bool               `json:"success"`
	Error   probeFailureDetail `json:"error"`
}

type probeFailureDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (l *InspectionLoop) persistResult(ctx context.Context, centerID uuidx.UUID, now time.Time, result *iproto.ProbeResult) error {
	var (
		payload []byte
		err     error
	)
	if result.GetSuccess() {
		payload, err = json.Marshal(result.GetBroker())
	} else {
		payload, err = json.Marshal(probeFailureResult{
			Success: false,
			Error: probeFailureDetail{
				Code:    result.GetError().GetCode(),
				Message: result.GetError().GetMessage(),
			},
		})
	}
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: marshal probe result failed",
			slog.String("center_id", centerID.String()), slog.Any("error", err))
		return err
	}
	return l.recordProbeResult(ctx, centerID, now, payload)
}

// recordFailure 落库一次"请求本身没有走完全程"的探测失败（见
// inspectCenter 里 DispatchBrokerProbe 返回 error 的分支）。code/message
// 是本包自定义的诊断信息，不携带任何底层 RPC 错误细节（那些只进日志，
// 见调用点注释）。
func (l *InspectionLoop) recordFailure(ctx context.Context, centerID uuidx.UUID, now time.Time, code, message string) error {
	payload, err := json.Marshal(probeFailureResult{
		Success: false,
		Error:   probeFailureDetail{Code: code, Message: message},
	})
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: marshal dispatch failure result failed",
			slog.String("center_id", centerID.String()), slog.Any("error", err))
		return err
	}
	return l.recordProbeResult(ctx, centerID, now, payload)
}

func (l *InspectionLoop) recordProbeResult(ctx context.Context, centerID uuidx.UUID, now time.Time, payload []byte) error {
	if err := repo.RecordCenterBrokerProbeResult(ctx, l.db, centerID, now, model.JSON(payload)); err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: record broker probe result failed",
			slog.String("center_id", centerID.String()), slog.Any("error", err))
		return err
	}
	return nil
}

// Run 阻塞运行巡检循环直到 ctx 被取消；调用方应该在自己的 goroutine 里
// 启动它。启动时立即跑一轮（不等第一个 interval），与 PartitionLoop/
// reclaim.Loop 同一个理由：进程刚启动时不应该空等一整个 interval 才开始
// 第一次巡检。
func (l *InspectionLoop) Run(ctx context.Context) {
	dataCheckDone := make(chan struct{})
	go func() { defer close(dataCheckDone); l.runDataChecks(ctx) }()
	defer func() { <-dataCheckDone }()
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		l.runManualEndpointProbes(ctx)
	}()
	defer func() { <-workerDone }()
	_ = l.RunOnce(ctx)
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = l.RunOnce(ctx)
		}
	}
}

// endpointProbeEnabled 为 false 时本函数立即返回，不是"派发了但丢弃结果"
// ——闸门的意义就是让合入这件事不产生任何运行期行为，否则"默认关"只是把
// 副作用换了个地方。默认关的理由只写在
// config.BackgroundTasksConfig.EndpointProbeEnabled 上，本处不复述。
//
// 端点列表来自 repo.ListEndpoints(ctx, db, center.ID)，节点来自
// ensureInspector（同样只在本中心内选），所以今天走不到那条分支。仍然显式
// 判一次：这是 C-19 的硬约束，把它写成代码而不是写成"由调用链天然保证"，
// 是因为后者在有人改动调用链时不会报错。判据本身也因此可测。
//
// 失败经 UpdateEndpointProbeFailure 只写探测元数据，保留上次有效的
// version/server_uuid/lower_case_table_names，界面可以看到本次失败。
func (l *InspectionLoop) probeEndpoints(ctx context.Context, center model.Center, node model.Node, dispatcher IProbeDispatcher, now time.Time) error {
	if !l.endpointProbeEnabled {
		return nil
	}

	endpoints, err := repo.ListEndpoints(ctx, l.db, center.ID)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: list endpoints failed",
			slog.String("center_id", center.ID.String()), slog.Any("error", err))
		return err
	}

	var errs []error
	for _, ep := range endpoints {
		if l.manualProbePending(ep.ID) {
			continue
		}
		if ep.CenterID != node.CenterID {
			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: refuse cross-center endpoint probe dispatch",
				slog.String("endpoint_id", ep.ID.String()),
				slog.String("endpoint_center_id", ep.CenterID.String()),
				slog.String("node_id", node.ID.String()),
				slog.String("node_center_id", node.CenterID.String()))
			continue
		}

		taskID := uuidx.New().String()
		result, err := dispatcher.DispatchConnTest(ctx, ep, node, taskID)
		if err != nil {
			slog.LogAttrs(ctx, slog.LevelWarn, "inspection: endpoint conn test dispatch failed",
				slog.String("endpoint_id", ep.ID.String()), slog.String("node_id", node.ID.String()),
				slog.String("task_id", taskID), slog.Any("error", err))
			errs = append(errs, err)
			if recErr := l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "DISPATCH_FAILED", "探测请求未能完成往返"); recErr != nil {
				errs = append(errs, recErr)
			}
			continue
		}
		if err := l.persistEndpointResult(ctx, ep, node, now, taskID, result); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// persistEndpointResult 把一次 conn_test 成功或失败结果写回 endpoints 表。
func (l *InspectionLoop) persistEndpointResult(ctx context.Context, ep model.Endpoint, node model.Node, now time.Time, taskID string, result *iproto.ProbeResult) error {
	if !result.GetSuccess() {
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: endpoint conn test reported failure",
			slog.String("endpoint_id", ep.ID.String()), slog.String("node_id", node.ID.String()),
			slog.String("error_code", result.GetError().GetCode()))
		return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID,
			result.GetError().GetCode(), result.GetError().GetMessage())
	}

	ct := result.GetConnTest()
	if ct == nil {
		// success=true 但 oneof 里不是 conn_test：节点侧实现有缺陷，或者
		// 回执被张冠李戴。不能当成"探到了空值"写进去——那会把好值抹掉。
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: endpoint conn test result missing conn_test payload",
			slog.String("endpoint_id", ep.ID.String()), slog.String("node_id", node.ID.String()))
		return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "INVALID_REPLY", "探测回执缺少连接测试结果")
	}
	if ct.GetVersion() == "" || ct.GetServerUuid() == "" {
		return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "INVALID_REPLY", "探测回执缺少版本或 server_uuid")
	}

	// lower_case_table_names 由节点按原始字符串("0"/"1"/"2")回传，用 mysqlx
	// 现成的解析函数转换，不在本包自己写映射——节点侧 internal/node/probe/conntest.go 落库前
	// 已经用同一个函数校验过一次，这里再解析一次是因为协议传的是字符串、
	// 落库要的是枚举，不是重复校验。
	raw, ok := ct.GetKeyParams()["lower_case_table_names"]
	if !ok {
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: endpoint conn test result missing lower_case_table_names",
			slog.String("endpoint_id", ep.ID.String()), slog.String("node_id", node.ID.String()))
		return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "INVALID_REPLY", "探测回执缺少 lower_case_table_names")
	}
	lctn, err := mysqlx.ParseLowerCaseTableNames(raw)
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelWarn, "inspection: endpoint conn test returned unparsable lower_case_table_names",
			slog.String("endpoint_id", ep.ID.String()), slog.String("node_id", node.ID.String()),
			slog.Any("error", err))
		return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "INVALID_REPLY", "探测回执的 lower_case_table_names 不合法")
	}

	payload, err := json.Marshal(endpointProbeSuccess(taskID, ct, lctn))
	if err != nil {
		slog.LogAttrs(ctx, slog.LevelError, "inspection: marshal endpoint conn test result failed",
			slog.String("endpoint_id", ep.ID.String()), slog.Any("error", err))
		return err
	}

	probeWrite := repo.EndpointProbeResult{
		ProbeNodeID:         node.ID,
		ProbedAt:            now,
		Version:             ct.GetVersion(),
		ServerUUID:          ct.GetServerUuid(),
		GroupUUID:           ct.GetKeyParams()["group_replication_group_name"],
		LowerCaseTableNames: lctn,
		Detail:              model.JSON(payload),
	}
	for range 3 {
		probeWrite.ExpectedLockVersion = ep.LockVersion
		err := repo.UpdateEndpointProbeResult(ctx, l.db, ep.ID, probeWrite)
		if err == nil {
			return nil
		}
		if errors.Is(err, repo.ErrOriginUUIDConflict) {
			return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "ORIGIN_UUID_CONFLICT", "实例或组 UUID 已被其他来源登记，请核对实例身份与中心归属")
		}
		if errors.Is(err, repo.ErrEndpointOriginInvalid) {
			return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "INVALID_ORIGIN_UUID", "探测回执的实例或组 UUID 不合法")
		}
		if errors.Is(err, repo.ErrEndpointProbeNodeInvalid) {
			return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "PROBE_NODE_CHANGED", "探测节点已不可用或中心归属改变，请重新探测")
		}
		if !errors.Is(err, repo.ErrEndpointVersionConflict) {
			return err
		}
		current, err := repo.GetEndpoint(ctx, l.db, ep.ID)
		if err != nil {
			return err
		}
		if current.CenterID != ep.CenterID || current.Host != ep.Host || current.Port != ep.Port || current.Username != ep.Username || current.Password != ep.Password || !bytes.Equal(current.TLS, ep.TLS) || !bytes.Equal(current.Params, ep.Params) {
			return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "ENDPOINT_CHANGED", "探测期间端点连接信息已修改，请重新探测")
		}
		ep = *current
	}
	return l.persistEndpointFailure(ctx, ep.ID, node.ID, now, taskID, "WRITE_CONFLICT", "探测结果写入冲突，请重新探测")
}
