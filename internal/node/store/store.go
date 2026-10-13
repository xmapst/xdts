// Package store owns Node identity and state databases. Writes are serialized
// through the store actor and committed before critical acknowledgements.
// Store starts before its clients and closes after their final writes.
//
// xdoc updates replace whole documents; nil pointers round-trip as nil.
// Document/map keys cannot contain dots. Published disk field names and record
// identities are preserved across migrations; clients transfer independent values.
package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofrs/flock"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xhive"
	"github.com/xmapst/xhive/chanrpc"
	"github.com/xmapst/xhive/timer"
)

const (
	identityFileName = "identity.xdoc"
	stateFileName    = "state.xdoc"
	lockFileName     = ".lock"
	corruptDirName   = "corrupt"
	backupDirName    = "backup"

	timerKindFlush = "store.flush"
)

// Options 是构造 Module 所需的运行参数，由上层从 internal/node/config.Config
// 摘取字段构造；store 包本身不依赖 config 包，保持可独立测试。
type Options struct {
	// DataDir 是本地状态库所在目录（NODE-BOOT-01）。
	DataDir string
	// RegisterCode 同时是注册凭据与本地状态库加密口令（NODE-XDOC-03），
	// 只在打开 xdoc 时使用，不写入任何文件、日志与上报。
	RegisterCode string
	// BatchInterval 是批量提交的触发周期（NODE-XDOC-04 默认 100ms）。
	BatchInterval time.Duration
	// BatchMaxOps 是累计多少条写入触发一次立即提交（不必等到下个周期）。
	BatchMaxOps int
	// ShutdownTimeout 只作用于 store 模块关闭，不能代替整个应用的关闭预算。
	ShutdownTimeout time.Duration

	now func() time.Time
}

func (o Options) nowFunc() func() time.Time {
	if o.now != nil {
		return o.now
	}
	return time.Now
}

// Module 是 store 静态模块。
type Module struct {
	*xhive.Skeleton

	opts Options
	now  func() time.Time

	dataLock *flock.Flock

	identityDB  *xdoc.DB
	stateDB     *xdoc.DB
	initialized bool
	closed      bool

	linear *xhive.LinearContext

	// pending 只在本模块事件循环（ChanRPC handler / Timer handler）里被
	// 读写，不需要加锁（模块内部状态只在循环内访问）。
	pending []*pendingOp

	stats statsCounters

	// initErr 记录 OnInit 失败的原因，只在 OnInit 内写入一次，其余时间只读。
	// xhive.Run 本身不向调用方传回模块初始化失败的具体原因（探测已确认：
	// 失败时 Run 静默清理已启动的模块后直接返回，不返回 error），上层
	// （cmd/node）据此判断是否需要以非零码退出进程，见 InitErr 的说明。
	initErr error

	// rebuilds 记录本次 OnInit 期间发生过的自动重建（NODE-XDOC-06），见
	// RebuildEvents 方法文档。只在 OnInit 内的 selfCheck→rebuildFile 调用链里
	// 追加（该调用链是 OnInit 的一部分，此时只有构造它的这个 goroutine 在跑，
	// 不需要加锁），OnInit 返回之后只读。
	rebuilds []RebuildRecord
}

// RebuildRecord 描述一次自动重建（NODE-XDOC-06）。本包不依赖 internal/iproto
// （保持"协议无关的本地持久化层"这条既有边界，见包文档与 internal/node/store/selfcheck.go 现状：
// 全包没有一处 import iproto），字段全部是纯字符串，把"翻译成
// LocalStoreRebuildEventDetail 协议消息"这件事留给消费方（reporter，见
// RebuildEvents 方法文档）。
type RebuildRecord struct {
	// File 是被重建的文件名（identityFileName 或 stateFileName 的取值，即
	// "identity.xdoc"/"state.xdoc"）。
	File string
	// Level 是这次重建的严重度，取值 "INFO"/"WARN"/"ERROR"，与本包在对应
	// 场景里选择的 slog 级别一致（internal/node/store/selfcheck.go rebuildFile/fallbackToEmpty）：
	// 无数据丢失的重建=INFO，有数据丢失的重建=WARN，重建失败降级为空库=ERROR。
	Level string
	// Reason 是面向运维的完整描述（是否有数据丢失、涉及哪些集合，或重建失败的
	// 具体原因），供 reporter 直接用作上报事件的详情文本。
	Reason string

	BackupPath string
}

// store 的 Priority 是 5，reporter 是 20；xhive 的静态模块启动严格按 Priority
// 升序、单个 for 循环依次调用每个模块的 OnInit，全部 OnInit 成功之后才会开始
// 并发拉起各模块的 Serve goroutine（已读 vendored xhive github.com/xmapst/xhive/module.go start() 源码
// 确认，见该函数"按上面排好序的 staticModules 顺序依次初始化"一节）。这意味着
// reporter.OnInit 开始执行的那一刻，store.OnInit（含它触发的全部 rebuildFile
// 调用）已经完整跑完、返回——本方法因此不需要也不应该加锁：调用方看到的
// m.rebuilds 不会是"部分写入"的中间状态，且这之后也不会再有任何写者（selfCheck
// 只在 OnInit 里被调用一次）。
//
// 调用点只应该是 reporter.OnInit（或同样运行在"晚于 store OnInit"这个时间点的
// 逻辑）——这不是一个可以在任意时刻安全调用的通用只读方法，是利用了 xhive
// 启动顺序这个具体保证的窄接口，其它调用点（例如某个动态模块的 OnInit，动态
// 模块的装配时机与静态模块的启动顺序无关）不能想当然地复用同一个安全性论证。
func (m *Module) RebuildEvents() []RebuildRecord {
	return m.rebuilds
}

// recordRebuild 供 internal/node/store/selfcheck.go 在 rebuildFile/fallbackToEmpty 完成一次重建
// （无论成功还是失败）后调用，把结果追加进 m.rebuilds。
func (m *Module) recordRebuild(file, level, reason, backupPath string) {
	m.rebuilds = append(m.rebuilds, RebuildRecord{
		File:       file,
		Level:      level,
		Reason:     reason,
		BackupPath: backupPath,
	})
}

func New(opts Options) *Module {
	m := &Module{
		opts: opts,
		now:  opts.nowFunc(),
	}
	skeletonOpts := []xhive.SkeletonOption{
		xhive.WithCloseDrainTimeout(opts.ShutdownTimeout),
		xhive.WithClientCloseTimeout(opts.ShutdownTimeout),
		xhive.WithAsyncCloseTimeout(opts.ShutdownTimeout),
	}
	m.Skeleton = xhive.NewSkeleton(ModuleName, skeletonOpts...)
	return m
}

// Priority 固定为 5：store 必须比 controller（10）等其他静态模块更早初始化、
// 更晚关闭。
func (m *Module) Priority() uint { return 5 }

// InitErr 返回 OnInit 的失败原因；OnInit 成功时为 nil。
//
// 用途：xhive.Run 对模块初始化失败没有任何返回值可供调用方判定（已实测
// 确认），上层必须自己拿到这个原因才能决定日志与进程退出码，Module 是
// 唯一持有这份信息的地方。只在 xhive.Run 返回之后读取才是并发安全的——此时
// OnInit 所在的 goroutine 已经通过 errCh/stopped 的 channel 操作与调用方
// 建立了 happens-before 关系。
func (m *Module) InitErr() error { return m.initErr }

type Stats struct {
	CommitCount        int64
	FailedOps          int64
	LastCommitAt       time.Time
	LastCommitDuration time.Duration

	StoreSizeBytes uint64
}

func (m *Module) Stats() Stats {
	return Stats{
		CommitCount:        m.stats.commitCount.Load(),
		FailedOps:          m.stats.failedOps.Load(),
		LastCommitAt:       time.UnixMicro(m.stats.lastCommitAtMicro.Load()),
		LastCommitDuration: time.Duration(m.stats.lastCommitDurationNs.Load()),
		StoreSizeBytes:     m.storeSizeBytes(),
	}
}

// storeSizeBytes 对 identity.xdoc/state.xdoc 各做一次 os.Stat 求和。用 DataDir
// 拼路径而不是问 identityDB/stateDB 要句柄，是因为这样可以安全地从任意 goroutine
// 调用（不触碰 m.identityDB/m.stateDB 这两个只应在事件循环生命周期内被赋值/关闭的
// 字段），与 Stats() 方法本身"可从任意 goroutine 安全调用"的既有承诺保持一致。
func (m *Module) storeSizeBytes() uint64 {
	var total uint64
	for _, name := range [...]string{identityFileName, stateFileName} {
		info, err := os.Stat(filepath.Join(m.opts.DataDir, name))
		if err != nil {
			continue
		}
		total += uint64(info.Size())
	}
	return total
}

// OnInit 完成本地状态库的自检 + 打开（NODE-XDOC-06/11）、data_dir 加锁
// （NODE-XDOC-02）、ChanRPC 处理器与批量提交定时器的注册。
//
// 任一步失败都会回滚本方法已分配的资源后返回 error（IModule.OnInit 契约：
// 框架不会对失败的模块调用 OnDestroy/Close）。
func (m *Module) OnInit() error {
	if err := m.Prepare(); err != nil {
		return err
	}
	m.initialized = true
	m.linear = m.NewLinearContext()

	for _, handler := range []struct {
		request any
		fn      func(*chanrpc.CallInfo) *chanrpc.RetInfo
	}{
		{Op{}, m.onOp},
		{StateExportRequest{}, m.onStateExport},
		{StateImportRequest{}, m.onStateImport},
	} {
		if err := m.RegisterChanRPC(handler.request, handler.fn); err != nil {
			_ = m.identityDB.Close()
			_ = m.stateDB.Close()
			m.releaseLockBestEffort()
			wrapped := fmt.Errorf("store: 注册 ChanRPC 处理器失败: %w", err)
			m.initErr = wrapped
			return wrapped
		}
	}

	m.RegisterTimer(timerKindFlush, func(_ int64, _ map[string]string) { m.flush() })
	// onOp is the only pending writer and immediately flushes at a threshold
	// of one (including the existing zero-value behavior). There is nothing
	// left for a periodic timer in this mode, even while commits are in flight.
	if m.opts.BatchMaxOps > 1 {
		m.NewTimer(timerKindFlush, m.opts.BatchInterval, timer.WithTicker())
	}
	return nil
}

// Prepare opens the store during the single-owner startup phase. OnInit reuses
// the same handles; business modules must not start before identity is resolved.
func (m *Module) Prepare() error {
	if m.closed {
		return errors.New("store: store already closed")
	}
	if m.identityDB != nil {
		return nil
	}
	if err := m.acquireLock(); err != nil {
		m.initErr = err
		return err
	}

	identityDB, stateDB, err := m.selfCheck(context.Background())
	if err != nil {
		m.releaseLockBestEffort()
		m.initErr = err
		return err
	}
	m.identityDB = identityDB
	m.stateDB = stateDB

	return nil
}

// OnDestroy 把在途批次提交（NFR-REL-05：完成在途批次、落盘位点）。
//
// 真正等待这次提交完成的是框架随后调用的 Close（见其说明）：flush 通过
// LinearContext 把提交动作丢给后台 goroutine 串行执行，OnDestroy 本身不阻塞。
func (m *Module) OnDestroy() {
	m.flush()
}

// Close 释放 client 资源并关闭本地状态库，是整个应用关闭序列里最后落地的一步
func (m *Module) Close() error {
	if m.closed {
		return nil
	}
	m.closed = true
	var errs []error
	if err := m.Skeleton.Close(); err != nil {
		errs = append(errs, fmt.Errorf("skeleton: %w", err))
	}
	if m.stateDB != nil {
		if err := m.stateDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("state.xdoc: %w", err))
		}
	}
	if m.identityDB != nil {
		if err := m.identityDB.Close(); err != nil {
			errs = append(errs, fmt.Errorf("identity.xdoc: %w", err))
		}
	}
	if m.dataLock != nil {
		if err := m.dataLock.Unlock(); err != nil {
			errs = append(errs, fmt.Errorf("data_dir 解锁: %w", err))
		}
		_ = m.dataLock.Close()
	}
	return errors.Join(errs...)
}

// 读句柄在 store 初始化完成后共享；业务模块通过 Client 投递写操作，
// 不会绕过 store 的事件循环直接写入。
func (m *Module) Get(ctx context.Context, collection, id string, out any) error {
	fk, ok := collectionFiles[collection]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownCollection, collection)
	}
	if id == "" {
		return ErrEmptyID
	}
	doc, err := m.dbFor(fk).Collection(collection).FindByID(ctx, xdoc.String(id))
	if err != nil {
		return err
	}
	return xdoc.UnmarshalValue(doc.Value(), out)
}

func (m *Module) dbFor(fk fileKind) *xdoc.DB {
	if fk == fileIdentity {
		return m.identityDB
	}
	return m.stateDB
}

// acquireLock 给 data_dir 加 flock，防止两个节点进程打开同一个库
// （NODE-XDOC-02）。
func (m *Module) acquireLock() error {
	if strings.TrimSpace(m.opts.DataDir) == "" {
		return errors.New("store: data_dir 不能为空")
	}
	if err := os.MkdirAll(m.opts.DataDir, 0o750); err != nil {
		return fmt.Errorf("store: 创建 data_dir %q 失败: %w", m.opts.DataDir, err)
	}
	fl := flock.New(filepath.Join(m.opts.DataDir, lockFileName))
	ok, err := fl.TryLock()
	if err != nil {
		return fmt.Errorf("store: 获取 data_dir 锁失败: %w", err)
	}
	if !ok {
		return fmt.Errorf("store: data_dir %q 已被另一个节点进程占用（NODE-XDOC-02）", m.opts.DataDir)
	}
	m.dataLock = fl
	return nil
}

func (m *Module) releaseLockBestEffort() {
	if m.dataLock == nil {
		return
	}
	if err := m.dataLock.Unlock(); err != nil {
		// OnInit 失败路径本身已经在向上传播一个更重要的错误，这里只记日志。
		_ = err
	}
	_ = m.dataLock.Close()
	m.dataLock = nil
}
