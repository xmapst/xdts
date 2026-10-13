package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xhive/chanrpc"
)

// pendingOp 是一条已经通过校验、正在等待下一次批量提交的写入。
type pendingOp struct {
	collection string
	id         string
	file       fileKind
	doc        *xdoc.Document
	critical   bool
	insertOnly bool
	// replier 仅 critical 为 true 时非空，由 flush 提交完成后在 ackBatch 里
	// 回包（NODE-XDOC-04：关键写入提交成功后才回包）。
	replier *chanrpc.Replier
	// err 由 commitBatch 写入、ackBatch 读出，两者分别是同一次
	// LinearContext.Go 提交的 f 与 cb，framework 保证 f 完整执行完毕之后
	// cb 才会被调度，因此这里不需要额外同步。
	err error
}

// statsCounters 是 Stats 的存储形式，全部使用原子类型以支持跨 goroutine 读取
// （commitBatch 在后台 goroutine 里写，Stats() 可能被其他模块的 goroutine读）。
type statsCounters struct {
	commitCount          atomic.Int64
	failedOps            atomic.Int64
	lastCommitAtMicro    atomic.Int64
	lastCommitDurationNs atomic.Int64
}

// onOp 是 Op 消息的 ChanRPC 处理函数，在 store 模块事件循环里串行执行
// （handler 在 Server 所属模块事件循环中串行执行）。
func (m *Module) onOp(ci *chanrpc.CallInfo) *chanrpc.RetInfo {
	op, ok := ci.RequestAs[Op]()
	if !ok {
		return &chanrpc.RetInfo{Err: fmt.Errorf("store: 请求类型不是 store.Op，实际 %T", ci.Request)}
	}
	if err := op.validate(); err != nil {
		return &chanrpc.RetInfo{Err: err}
	}

	doc, err := buildDoc(op)
	if err != nil {
		// 只在非关键路径记日志：关键写入的错误已经原样回给调用方（下面那个
		// return 会被 CallWithContext 收到），再记一条会让同一次失败在日志里
		// 出现两次，而调用方通常还会自己记一条。
		if !op.Critical {
			slog.Error("store: encode document failed, non-critical write dropped",
				slog.String("collection", op.Collection),
				slog.String("id", op.ID),
				slog.Any("error", err))
		}
		return &chanrpc.RetInfo{Err: fmt.Errorf("store: 编码文档失败: %w", err)}
	}

	p := &pendingOp{
		collection: op.Collection,
		id:         op.ID,
		file:       op.file(),
		doc:        doc,
		critical:   op.Critical,
		insertOnly: op.InsertOnly,
	}

	var ret *chanrpc.RetInfo
	if op.Critical {
		// 关键写入：Hold 住，提交成功后才在 ackBatch 里回包
		// （NODE-XDOC-04）。
		p.replier = ci.Hold()
	} else {
		// 普通写入：立即回包，只表示"已接受进入下一次批量提交"，不代表
		// 已经落盘。
		ret = &chanrpc.RetInfo{Ack: OpAck{Committed: false}}
	}

	m.pending = append(m.pending, p)
	if len(m.pending) >= m.opts.BatchMaxOps {
		m.flush()
	}
	return ret
}

// buildDoc 把 Op.Doc 编码为 *xdoc.Document，并强制写入 "_id" 字段为 Op.ID
// （store 的所有集合都按业务定的 key 写入，不使用 xdoc 的自动主键生成）。
func buildDoc(op Op) (*xdoc.Document, error) {
	var doc *xdoc.Document
	if existing, ok := op.Doc.(*xdoc.Document); ok {
		// 调用方直接传了一篇文档：Clone 一份，避免修改调用方仍持有的原始
		// 指针（跨模块消息投递后不应再被调用方修改，见 Op 的注释，这里额外
		// 防御一层，防止调用方违反约定时连累到 store 内部状态）。
		doc = existing.Clone()
	} else {
		d, err := xdoc.MarshalDocument(op.Doc)
		if err != nil {
			return nil, err
		}
		doc = d
	}
	doc.Set("_id", xdoc.String(op.ID))
	return doc, nil
}

// 批处理以线性异步上下文提交，保持写入顺序而不阻塞事件循环。
func (m *Module) flush() {
	if len(m.pending) == 0 {
		return
	}
	batch := m.pending
	m.pending = nil

	m.linear.Go(
		func() { m.commitBatch(batch) },
		func() { m.ackBatch(batch) },
	)
}

// commitBatch 在独立 goroutine 中运行（LinearContext 保证与其他 flush 调用
// 严格串行，不会并发访问 identityDB/stateDB），按文件分组各开一个事务提交。
// 一个文件的提交失败不影响另一个文件。
func (m *Module) commitBatch(batch []*pendingOp) {
	start := m.now()

	byFile := make(map[fileKind][]*pendingOp, 2)
	for _, p := range batch {
		byFile[p.file] = append(byFile[p.file], p)
	}

	for fk, items := range byFile {
		db := m.dbFor(fk)
		ctx := context.Background()
		err := db.Transaction(ctx, func(tx *xdoc.Tx) error {
			for _, p := range items {
				if p.insertOnly {
					_, findErr := tx.Collection(p.collection).FindByID(ctx, xdoc.String(p.id))
					if findErr == nil {
						p.err = ErrAlreadyExists
						continue
					}
					if !errors.Is(findErr, xdoc.ErrNotFound) {
						return findErr
					}
					if _, insertErr := tx.Collection(p.collection).InsertOne(ctx, p.doc); insertErr != nil {
						return insertErr
					}
					continue
				}
				if _, uerr := tx.Collection(p.collection).Upsert(ctx, p.doc); uerr != nil {
					return fmt.Errorf("collection %q id %q: %w", p.collection, p.id, uerr)
				}
			}
			return nil
		})
		for _, p := range items {
			if err != nil {
				p.err = err
			}
		}
		if err != nil {
			slog.Error("batch commit failed",
				slog.Int("file", int(fk)), slog.Int("ops", len(items)), slog.Any("error", err))
		}
	}

	dur := m.now().Sub(start)
	m.stats.commitCount.Add(1)
	m.stats.lastCommitAtMicro.Store(m.now().UnixMicro())
	m.stats.lastCommitDurationNs.Store(int64(dur))
}

// ackBatch 回到 store 模块事件循环执行（经 async 回调队列），对关键写入
// 回包，对失败的非关键写入只记日志（调用方已经在入队时收到成功应答，无法
// 再撤回——设计对"非关键写入立即回包"允许这个代价，见 Op 的说明）。
func (m *Module) ackBatch(batch []*pendingOp) {
	for _, p := range batch {
		if p.replier == nil {
			if p.err != nil {
				m.stats.failedOps.Add(1)
				slog.Error("non-critical write commit failed, caller already received success ack",
					slog.String("collection", p.collection), slog.String("id", p.id), slog.Any("error", p.err))
			}
			continue
		}
		if err := p.replier.Ret(&chanrpc.RetInfo{
			Ack: OpAck{Committed: p.err == nil},
			Err: p.err,
		}); err != nil {
			slog.Warn("critical write reply failed",
				slog.String("collection", p.collection), slog.String("id", p.id), slog.Any("error", err))
		}
		if p.err != nil {
			m.stats.failedOps.Add(1)
		}
	}
}
