package transport

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/xmapst/xamqp"
)

// Conn 包装 xamqp.Conn，是本包发布者/消费者/声明器共用的连接句柄。
//
// "句柄注入"：xamqp 连接在 main 创建后注入模块；本类型只是薄封装，
// 生命周期（何时创建、何时关闭）由调用方（cmd/node、cmd/manager 的装配代码）决定，
// 本包不持有全局单例。
//
// 重连策略提醒：Conn 使用指数退避（ConnectionOptions.ReconnectInterval
// 为基础值，最大 60 秒，xamqp 内部实现），依附于此 Conn 的 Consumer 自身的恢复循环则固定
// 3 秒重试一次——这是 xamqp 库自身两套不一致的策略，本包如实沿用，不做掩盖或统一。
type Conn struct {
	xc               *xamqp.Conn
	queueInspectBusy atomic.Bool
	queueProbeBusy   atomic.Bool
}

// NewConn 创建到单个 RabbitMQ 地址的连接（生产环境的多节点故障转移由前置负载均衡负责，
// xamqp 本身只接受单个地址，见 xamqp.NewConn 文档）。
func NewConn(url string, reconnectInterval time.Duration) (*Conn, error) {
	var opts []func(*xamqp.ConnectionOptions)
	if reconnectInterval > 0 {
		opts = append(opts, xamqp.WithConnectionOptionsReconnectInterval(reconnectInterval))
	}
	xc, err := xamqp.NewConn(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("transport: 连接 RabbitMQ 失败: %w", err)
	}
	return &Conn{xc: xc}, nil
}

func (c *Conn) Close() error {
	if c == nil || c.xc == nil {
		return nil
	}
	return c.xc.Close()
}

// IsClosed 返回连接是否已关闭。
func (c *Conn) IsClosed() bool {
	if c == nil || c.xc == nil {
		return true
	}
	return c.xc.IsClosed()
}
