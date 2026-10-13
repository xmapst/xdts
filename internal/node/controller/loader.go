package controller

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/xmapst/xhive"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpin"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpout"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/pipeline/snapreader"
	"github.com/xmapst/xdts/internal/node/pipeline/snapwriter"
	"github.com/xmapst/xdts/internal/node/resourcequota"
	"github.com/xmapst/xdts/internal/transport"
)

// prodLoader owns production module construction, Node quota admissions and
// per-pipeline target writers. Configuration/admission failures have isolated
// tests; actual module lifetimes are verified by MySQL/RabbitMQ flow tests.
type prodLoader struct {
	nc nodeContext

	// writers 记录每条目标端 pipeline 当前持有的 *mysqlout.Writer，供 Unload 时找到
	// 并关闭——amqpin.iApplier 接口只有 ApplyBatch，没有 Close（见该接口文档），
	// amqp_in 自己不知道也不负责关闭底层连接，必须由构造它的一方（本类型）在卸载时
	// 显式关闭。加锁：LoadTarget/Unload 可能被 internal/node/controller/apply.go 通过 Skeleton.AsyncGo 在不同
	// pipeline 各自独立的 goroutine 里并发调用（AsyncGo 的 f 在独立 goroutine 执行，
	// 见 xhive 文档），这份 map 因此是真正跨 goroutine 共享的可变状态，需要加锁
	// （"只有确实被多个 goroutine 访问的数据才加锁"，这里满足这个
	// 前提）。
	mu              sync.Mutex
	writers         map[string]*mysqlout.Writer
	publishers      map[string]*transport.Publisher
	snapshots       map[string]bool
	targetSnapshots map[string]bool
	admissions      map[appliedKey]*resourcequota.Admission
}

func newProdLoader(nc nodeContext) *prodLoader {
	if nc.Quota == nil {
		// Independently constructed production loaders still enforce defaults.
		nc.Quota, _ = resourcequota.New(resourcequota.DefaultLimits())
	}
	return &prodLoader{nc: nc, writers: make(map[string]*mysqlout.Writer), publishers: make(map[string]*transport.Publisher), snapshots: make(map[string]bool), targetSnapshots: make(map[string]bool), admissions: make(map[appliedKey]*resourcequota.Admission)}
}

// LoadSource 见 iPipelineLoader 文档。amqpout.Module 必须先于 binlogin.Module 构造
// 完成：binlogin.Options.Sink 要求一个已经存在的 binlogin.ISink 实现，二者的
// AddDynamicModules 调用顺序（amqp_out 在前）与这个依赖顺序一致，但即使顺序颠倒，
// Sink 字段本身在两个 Module 各自构造完成的那一刻就已经是一个有效的 Go 指针
// ——真正要求顺序的是"先构造出 outMod 实例"，不是"outMod 必须先 OnInit"。
func (l *prodLoader) LoadSource(ctx context.Context, item *iproto.PipelineConfigItem, startGTIDSet *string) (loadErr error) {
	admission, err := l.admit(item.GetPipelineId(), iproto.PipelineRole_PIPELINE_ROLE_SOURCE)
	if err != nil {
		return err
	}
	defer func() {
		if loadErr != nil {
			l.releaseAdmission(item.GetPipelineId(), iproto.PipelineRole_PIPELINE_ROLE_SOURCE, admission)
		}
	}()
	binOpts, outOpts, err := buildSourceOptions(item, l.nc, startGTIDSet)
	if err != nil {
		return err
	}
	if err := importStateBeforeAssembly(ctx, l.nc.Store, l.nc.NodeID, item); err != nil {
		return err
	}
	binOpts.InstanceID = iproto.PipelineInstance(ctx)
	// Source data has its own publisher/channel; report and control messages
	// retain the main process publisher. Unload closes this owned channel only
	// after the source stream and output module have returned.
	publisher, err := transport.NewPublisher(l.nc.Conn)
	if err != nil {
		return err
	}
	defer func() {
		if loadErr != nil {
			publisher.Close()
			return
		}
		l.mu.Lock()
		l.publishers[item.GetPipelineId()] = publisher
		l.mu.Unlock()
	}()
	outOpts.Publisher = publisher
	if needsSnapshot(item) {
		return l.loadSnapshotSource(ctx, item, binOpts, outOpts)
	}
	if err := l.prepareSourceRecovery(ctx, item, &binOpts); err != nil {
		return err
	}

	outMod := amqpout.New(outOpts)
	binOpts.Sink = outMod
	binMod := binlogin.New(binOpts)

	results, err := xhive.AddDynamicModules(outMod, binMod)
	if err != nil {
		return fmt.Errorf("controller: 装配 pipeline %q 源端动态模块失败: %w", item.GetPipelineId(), err)
	}
	if err := applyAddResults(results); err != nil {
		return fmt.Errorf("controller: 装配 pipeline %q 源端动态模块失败: %w", item.GetPipelineId(), err)
	}
	// A successful configuration receipt promises an active source, including
	// its schema baseline and live binlog subscription. The background stream
	// signals readiness only after receiving its first real server event.
	readyCtx, cancelReady := context.WithTimeout(ctx, 30*time.Second)
	err = binMod.WaitReady(readyCtx)
	cancelReady()
	if err != nil {
		xhive.RemoveDynamicModule(binlogin.ModuleName(item.GetPipelineId()))
		xhive.RemoveDynamicModule(amqpout.ModuleName(item.GetPipelineId()))
		return fmt.Errorf("controller: incremental subscription not ready: %w", err)
	}
	return nil
}

// LoadTarget 见 iPipelineLoader 文档。mysqlout.New 的目标库拨号是真实网络 IO，调用方
// （internal/node/controller/apply.go）必须已经把本次调用安排在 Skeleton.AsyncGo 的独立 goroutine 里，本方法
// 自身不做任何"是否在事件循环里"的判断（无法判断，也不是本类型的职责）。
func (l *prodLoader) LoadTarget(ctx context.Context, item *iproto.PipelineConfigItem) (loadErr error) {
	admission, err := l.admit(item.GetPipelineId(), iproto.PipelineRole_PIPELINE_ROLE_TARGET)
	if err != nil {
		return err
	}
	defer func() {
		if loadErr != nil {
			l.releaseAdmission(item.GetPipelineId(), iproto.PipelineRole_PIPELINE_ROLE_TARGET, admission)
		}
	}()
	mysqlOpts, amqpinOpts, err := buildTargetOptions(item, l.nc)
	if err != nil {
		return err
	}
	if err := importStateBeforeAssembly(ctx, l.nc.Store, l.nc.NodeID, item); err != nil {
		return err
	}
	amqpinOpts.InstanceID = iproto.PipelineInstance(ctx)
	amqpinOpts.ConfigVersion = item.GetConfigVersion()

	if err := l.prepareReinitTarget(ctx, item, mysqlOpts); err != nil {
		return err
	}
	if item.GetTarget().GetSchemaPlan() != nil {
		if err := l.executeSchemaPlan(ctx, item, mysqlOpts, false); err != nil {
			return err
		}
		if !targetNeedsSnapshot(item) {
			if err := l.executeSchemaPlan(ctx, item, mysqlOpts, true); err != nil {
				return err
			}
		}
	}
	if targetNeedsSnapshot(item) {
		return l.loadSnapshotTarget(ctx, item, mysqlOpts, amqpinOpts)
	}
	if err := l.declareDataQueue(item); err != nil {
		return err
	}

	writer, err := mysqlout.New(ctx, mysqlOpts)
	if err != nil {
		return fmt.Errorf("controller: 连接 pipeline %q 目标库失败: %w", item.GetPipelineId(), err)
	}
	amqpinOpts.Applier = writer

	amqpinMod := amqpin.New(amqpinOpts)

	writer.SetConflictSink(amqpinMod.ReportConflict)

	results, err := xhive.AddDynamicModules(amqpinMod)
	if err != nil {
		_ = writer.Close()
		return fmt.Errorf("controller: 装配 pipeline %q 目标端动态模块失败: %w", item.GetPipelineId(), err)
	}
	if err := applyAddResults(results); err != nil {
		_ = writer.Close()
		return fmt.Errorf("controller: 装配 pipeline %q 目标端动态模块失败: %w", item.GetPipelineId(), err)
	}

	l.mu.Lock()
	l.writers[item.GetPipelineId()] = writer
	l.mu.Unlock()
	return nil
}

// declareDataQueue creates the incremental queue. Snapshot target startup also
// creates the snapshot queue in loadSnapshotTarget before starting its consumer.
func (l *prodLoader) declareDataQueue(item *iproto.PipelineConfigItem) error {
	dc, err := transport.NewDeclarator(l.nc.Conn)
	if err != nil {
		return fmt.Errorf("controller: 创建拓扑声明器失败: %w", err)
	}
	defer dc.Close()

	if err := dc.DeclareExchange(transport.DataExchangeOptions()); err != nil {
		return fmt.Errorf("controller: 声明 %s 交换机失败: %w", transport.ExchangeData, err)
	}
	deliveryLimit := int(item.GetTarget().GetQueueDecl().GetIncrementalDeliveryLimit())
	if err := dc.DeclareQueue(transport.PipelineDataQueueOptions(item.GetPipelineId(), deliveryLimit)); err != nil {
		return fmt.Errorf("controller: 声明 pipeline %q 数据队列失败: %w", item.GetPipelineId(), err)
	}
	queue := transport.PipelineDataQueue(item.GetPipelineId())
	if err := dc.BindQueue(transport.ExchangeData, queue, transport.RoutingKeyPipelineData(item.GetPipelineId())); err != nil {
		return fmt.Errorf("controller: 绑定 pipeline %q 数据队列失败: %w", item.GetPipelineId(), err)
	}
	return nil
}

// Unload confirms module removal before releasing the role's Writer.
// An unconfirmed removal leaves ownership uncertain and blocks unsafe reassembly.
func (l *prodLoader) Unload(pipelineID string, role iproto.PipelineRole) (unloadErr error) {
	l.mu.Lock()
	admission := l.admissions[appliedKey{pipelineID: pipelineID, role: role}]
	l.mu.Unlock()
	defer func() {
		if unloadErr == nil {
			l.releaseAdmission(pipelineID, role, admission)
		}
	}()
	switch role {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		l.mu.Lock()
		hasSnapshot := l.snapshots[pipelineID]
		l.mu.Unlock()
		if hasSnapshot {
			if !xhive.RemoveDynamicModule(snapreader.ModuleName(pipelineID)) {
				return fmt.Errorf("controller: snapshot module shutdown not confirmed")
			}
			l.mu.Lock()
			delete(l.snapshots, pipelineID)
			l.mu.Unlock()
		}
		binOK := xhive.RemoveDynamicModule(binlogin.ModuleName(pipelineID))
		// Both producers must finish before their owned AMQP publisher closes.
		outOK := xhive.RemoveDynamicModule(amqpout.ModuleName(pipelineID))
		if !binOK {
			return fmt.Errorf("controller: pipeline %q 的 binlogin 模块未能确认完整销毁"+
				"（RemoveDynamicModule 返回 false，可能是等待 goroutine 退出超时、框架已跳过"+
				"OnDestroy/Close，也可能是重复调用；无法从公开 API 区分，按更安全的一种处理），"+
				"底层 MySQL 复制连接可能仍然存活，不应视为已安全卸载", pipelineID)
		}
		if !outOK {
			return fmt.Errorf("controller: source output shutdown not confirmed")
		}
		l.mu.Lock()
		publisher := l.publishers[pipelineID]
		delete(l.publishers, pipelineID)
		l.mu.Unlock()
		if publisher != nil {
			publisher.Close()
		}
		return nil
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		l.mu.Lock()
		hasTargetSnapshot := l.targetSnapshots[pipelineID]
		l.mu.Unlock()
		if hasTargetSnapshot {
			if !xhive.RemoveDynamicModule(snapwriter.ModuleName(pipelineID)) {
				return fmt.Errorf("controller: snapshot target shutdown not confirmed")
			}
			if !xhive.RemoveDynamicModule(amqpin.ModuleName(pipelineID)) {
				return fmt.Errorf("controller: incremental target shutdown not confirmed")
			}
			l.mu.Lock()
			delete(l.targetSnapshots, pipelineID)
			l.mu.Unlock()
		}
		// Confirm worker removal before closing a Writer that the worker may still own.
		if !hasTargetSnapshot {
			if !xhive.RemoveDynamicModule(amqpin.ModuleName(pipelineID)) {
				l.mu.Lock()
				hasWriter := l.writers[pipelineID] != nil
				l.mu.Unlock()
				if hasWriter {
					return fmt.Errorf("controller: incremental target shutdown not confirmed")
				}
			}
		}
		l.mu.Lock()
		writer := l.writers[pipelineID]
		delete(l.writers, pipelineID)
		l.mu.Unlock()
		if writer == nil {
			return nil
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("controller: 关闭 pipeline %q 目标库连接失败: %w", pipelineID, err)
		}
		return nil
	default:
		return fmt.Errorf("controller: Unload 收到未知 role %s（pipeline_id=%s）", role, pipelineID)
	}
}

func (l *prodLoader) admit(pipelineID string, role iproto.PipelineRole) (*resourcequota.Admission, error) {
	roleName := ""
	switch role {
	case iproto.PipelineRole_PIPELINE_ROLE_SOURCE:
		roleName = "source"
	case iproto.PipelineRole_PIPELINE_ROLE_TARGET:
		roleName = "target"
	}
	a, err := l.nc.Quota.Admit(pipelineID, roleName)
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	l.admissions[appliedKey{pipelineID: pipelineID, role: role}] = a
	l.mu.Unlock()
	return a, nil
}

func (l *prodLoader) releaseAdmission(pipelineID string, role iproto.PipelineRole, a *resourcequota.Admission) {
	if a == nil {
		return
	}
	l.mu.Lock()
	key := appliedKey{pipelineID: pipelineID, role: role}
	if l.admissions[key] == a {
		delete(l.admissions, key)
		a.Release()
	}
	l.mu.Unlock()
}

// applyAddResults 检查 xhive.AddDynamicModules 的结果：只要有一个模块失败，就把
// 本次调用里已经成功启动的模块全部移除，不留下"半条 pipeline 在跑"的状态——
// 调用方（LoadSource/LoadTarget）据此把整次装配当成单一原子操作对待，成功要么
// 全部模块都起来，要么全部回滚，不需要自己判断"哪些起来了、要不要善后"。
func applyAddResults(results []xhive.AddDynamicModuleResult) error {
	var errs []error
	for _, r := range results {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Name, r.Err))
		}
	}
	if len(errs) == 0 {
		return nil
	}
	for _, r := range results {
		if r.Err == nil {
			xhive.RemoveDynamicModule(r.Name)
		}
	}
	return errors.Join(errs...)
}
