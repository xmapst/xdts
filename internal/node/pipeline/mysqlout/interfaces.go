package mysqlout

import (
	"context"

	"github.com/xmapst/xdts/internal/iproto"
)

// 命名与形状特意不是"ReceiveBatch(ctx) (*DataBatch, error)"这种 pull 语法——
// 数据流向是 amqp_in 推给 mysql_out，不是 mysql_out 去队列里取，理由见 internal/node/pipeline/mysqlout/doc.go。
type IBatchApplier interface {
	// ApplyBatch 把一个 DataBatch 里的全部事务按源端提交顺序依次写入目标库。
	// 支持 DML、按策略执行的 DDL 及冲突处理；非法消息和不能安全改写的语句
	// 返回明确错误。显式过滤、跳过与实际应用由扩展结果接口分别报告。
	ApplyBatch(ctx context.Context, batch *iproto.DataBatch) error
}
