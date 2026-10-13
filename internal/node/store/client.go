package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xmapst/xhive/chanrpc"
)

// IReader 提供只读状态访问；*Module 实现该接口。
type IReader interface {
	Get(ctx context.Context, collection, id string, out any) error
}

// Client 是 store 模块写入接口的进程内客户端，面向两类调用方：
//
// PutCriticalAsync 为每次请求建立独立客户端及响应循环，不与模块事件循环
// 或其它工作者共享响应队列。调用方通过结果通道在自己的工作上下文中继续流程。
type Client struct {
	server *chanrpc.Server
	reader IReader
	rpc    *chanrpc.Client
}

// NewClient 构造一个外部 goroutine 可用的 store 客户端。server 通常来自
// xhive.ChanRPC(store.ModuleName)；reader 通常就是 *store.Module 本身。
func NewClient(server *chanrpc.Server, reader IReader) *Client {
	return &Client{
		server: server,
		reader: reader,
		rpc:    chanrpc.NewClient(),
	}
}

// Close 释放底层 chanrpc.Client 资源。
func (c *Client) Close() {
	c.rpc.Close()
}

// Put 是非关键写入：Cast 单向投递，不等待、也不保证送达。
func (c *Client) Put(op Op) error {
	if err := op.validate(); err != nil {
		return err
	}
	c.rpc.Cast(c.server, op)
	return nil
}

// PutCritical 是关键写入：阻塞等待 store 完成批量提交后的应答
// （NODE-XDOC-04）。
func (c *Client) PutCritical(ctx context.Context, op Op) error {
	op.Critical = true
	if err := op.validate(); err != nil {
		return err
	}
	ret := c.rpc.CallWithContext(ctx, c.server, op)
	if ret == nil {
		return errors.New("store: 未收到应答")
	}
	if ret.Err != nil {
		return ret.Err
	}
	ack, ok := ret.AckAs[OpAck]()
	if !ok {
		return fmt.Errorf("store: 应答类型不是 store.OpAck，实际 %T", ret.Ack)
	}
	if !ack.Committed {
		return errors.New("store: 关键写入未确认提交")
	}
	return nil
}

// PutCriticalAsync submits a critical write without a synchronous RPC. The
// result is emitted only after durable acknowledgment or context cancellation.
// Doc ownership transfers when this method succeeds, as with Put.
func (c *Client) PutCriticalAsync(ctx context.Context, op Op) (<-chan error, error) {
	op.Critical = true
	if ctx == nil {
		return nil, errors.New("store: critical write context required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := op.validate(); err != nil {
		return nil, err
	}
	client := chanrpc.NewClient()
	result := make(chan error, 1)
	var once sync.Once
	emit := func(err error) { once.Do(func() { result <- err }) }
	if err := client.AsyncCall(c.server, op, func(ret *chanrpc.RetInfo) {
		if ret.Err != nil {
			emit(ret.Err)
			return
		}
		ack, ok := ret.AckAs[OpAck]()
		if !ok || !ack.Committed {
			emit(errors.New("store: critical write was not confirmed committed"))
			return
		}
		emit(nil)
	}); err != nil {
		client.Close()
		return nil, err
	}
	go func() {
		defer client.Close()
		select {
		case <-client.NotEmpty():
			if ret, ok := client.Pop(); ok {
				client.AsyncCallback(ret)
			}
		case <-ctx.Done():
			emit(ctx.Err())
		}
	}()
	return result, nil
}

// Get 直接读共享句柄（不经过 ChanRPC），语义与 Module.Get 一致。
func (c *Client) Get(ctx context.Context, collection, id string, out any) error {
	return c.reader.Get(ctx, collection, id, out)
}
