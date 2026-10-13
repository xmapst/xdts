package amqpin

import (
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/resourcequota"
)

// reassemblyState 缓冲一次大事务分片重组进行中的状态。只由 handle 所在的这一个
// goroutine 读写 保证任意时刻只有一个
// goroutine 在跑 handle），不需要加锁。
type reassemblyState struct {
	// shell 是第一片解码后的 DataBatch：PipelineId/RunId/Epoch/BeginGtidSet/
	// EndGtidSet 及 Txns[0] 除 Rows 外的字段，在 binlogin 对同一事务的每一片里
	// 完全相同（internal/node/pipeline/binlogin/session.go emitTxn 的实现），因此直接复用第一片解出来的结构，
	// 只在 append 时把后续分片的 Rows 接到 Txns[0].Rows 后面，不需要单独拆字段
	// 再拼一遍。finish 时清空 Chunk 字段，得到一个与"未分片"情形结构上完全一致
	// 的完整 DataBatch，交给 ApplyBatch 的路径不需要区分它是不是重组出来的。
	shell      *iproto.DataBatch
	total      uint32
	handles    []iAckHandle
	superseded []iAckHandle
	buffers    []*resourcequota.Buffer
}

func (r *reassemblyState) release() {
	for _, b := range r.buffers {
		b.Release()
	}
	r.buffers = nil
	r.shell = nil
}

// newReassemblyState 从收到的第一片（index 必须为 0，调用方负责校验）构造重组
// 状态。
func newReassemblyState(batch *iproto.DataBatch, handle iAckHandle) *reassemblyState {
	return &reassemblyState{
		shell:   batch,
		total:   batch.GetChunk().GetTotal(),
		handles: []iAckHandle{handle},
	}
}

// gtid 返回本次重组关联的事务 GTID，用于校验后续分片是否属于同一个事务、以及
// 冻结日志里点明是哪个事务卡住了。
func (r *reassemblyState) gtid() string {
	return r.shell.GetTxns()[0].GetGtid()
}

// nextExpectedIndex 是下一片应该携带的 Chunk.Index（已收 N 片，下一片理应是第
// N 片，从 0 开始编号）。
func (r *reassemblyState) nextExpectedIndex() uint32 {
	return uint32(len(r.handles))
}

// append 把一片已确认属于本次重组、顺序也对的分片并入 shell，返回是否已收全。
// 调用方必须先校验 gtid/index/total 均匹配（见 handleChunk），本方法不重复校验。
func (r *reassemblyState) append(batch *iproto.DataBatch, handle iAckHandle) (done bool) {
	txn := r.shell.Txns[0]
	txn.Rows = append(txn.Rows, batch.GetTxns()[0].GetRows()...)
	r.handles = append(r.handles, handle)
	return uint32(len(r.handles)) >= r.total
}

// finish 收全后拼出最终可直接交给 ApplyBatch 的完整 DataBatch（清空 Chunk 字段），
// 并返回需要在应用成功后统一 Ack 的全部 delivery。
func (r *reassemblyState) finish() (*iproto.DataBatch, []iAckHandle) {
	r.shell.Chunk = nil
	return r.shell, append(r.handles, r.superseded...)
}
