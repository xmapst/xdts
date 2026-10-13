package store

import (
	"errors"
	"fmt"
)

// ModuleName 是 store 模块在 xhive 里注册的名字，其他模块通过
// xhive.ChanRPC(store.ModuleName) 或自己 Skeleton 的 Cast/AsyncCall
// 向它投递 Op（store 模块名固定为 "store"）。
const ModuleName = "store"

// fileKind 标识一个集合归属于哪个 xdoc 文件。
type fileKind int

const (
	fileIdentity fileKind = iota
	fileState
)

// collectionFiles 定义集合所属的状态文件；未登记的集合名在入队前拒绝。
var collectionFiles = map[string]fileKind{
	"identity":                   fileIdentity,
	"configs":                    fileState,
	"checkpoints":                fileState,
	"snapshot_runs":              fileState,
	"snapshot_targets":           fileState,
	"snapshot_chunks":            fileState,
	"schema_migrations":          fileState,
	"schema_history":             fileState,
	"skipped_transactions":       fileState,
	"skipped_transaction_sets":   fileState,
	"publication_window_cursors": fileState,
	"outbox":                     fileState,
	"runtime":                    fileState,

	"cmd_dedup": fileState,
}

// ErrUnknownCollection 表示 Op.Collection 未在 collectionFiles 中登记。
var ErrUnknownCollection = errors.New("store: 未登记的集合名")

// ErrEmptyID 表示 Op.ID 为空。store 的所有集合都按业务定的 key 写入
// （node_id、pipeline_id.role 等），不使用 xdoc 的自动主键生成，以保持
// Upsert 幂等，因此 ID 必须由调用方显式提供。
var ErrEmptyID = errors.New("store: Op.ID 不能为空")

// ErrAlreadyExists is returned by an InsertOnly operation. The existing
// document remains authoritative and the caller can read it after this reply.
var ErrAlreadyExists = errors.New("store: document already exists")

// Op 投递后发送方不得修改文档，接收方持有独立所有权。
type Op struct {
	// Collection 必须是 collectionFiles 登记的名字之一。
	Collection string
	// ID 是文档主键，写入时会覆盖 Doc 里可能带的同名字段。
	ID string
	// Doc 是待写入的字段集合：map[string]any 或任意可被
	// xdoc.MarshalDocument 编码的结构体。对结构体，磁盘键由 bson tag
	// 或 Go 字段名决定，json tag 不参与；改已发布字段名须迁移存档。
	Doc any
	// Critical 为 true 时该写入必须等待所在批次事务提交成功后才回包
	// （NODE-XDOC-04：身份、分块计划、epoch 递增等关键写入）。
	Critical bool
	// InsertOnly atomically creates an immutable document if absent. It prevents
	// a timed-out old writer and its replacement from overwriting one another's
	// snapshot plans. Requires Critical so the result can be observed.
	InsertOnly bool
}

// OpAck 是 Op 的应答。
type OpAck struct {
	// Committed 表示这次写入是否已经完成事务提交并落盘。非 Critical 写入
	// 立即回包，此时恒为 false——立即回包只表示"已接受进入下一次批量提交"，
	// 不代表已经落盘；调用方如果需要"落盘后才能用新值"的语义必须把
	// Op.Critical 设为 true（NODE-XDOC-04）。
	Committed bool
}

// validate 校验 Op 本身的边界输入，不涉及任何 IO。
func (op Op) validate() error {
	fk, ok := collectionFiles[op.Collection]
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownCollection, op.Collection)
	}
	_ = fk
	if op.ID == "" {
		return ErrEmptyID
	}
	if op.Doc == nil {
		return errors.New("store: Op.Doc 不能为空")
	}
	if op.InsertOnly && !op.Critical {
		return errors.New("store: InsertOnly requires a critical write")
	}
	return nil
}

// file 返回该 Op 目标集合所属的文件；调用前必须先 validate() 通过。
func (op Op) file() fileKind {
	return collectionFiles[op.Collection]
}
