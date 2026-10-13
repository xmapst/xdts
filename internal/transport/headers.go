package transport

import (
	"maps"

	amqp "github.com/rabbitmq/amqp091-go"
)

// HeadersFromMap 把 iproto.Envelope 的中立 headers（map[string]any，iproto 包不依赖
// xamqp/amqp091-go）转换为 amqp091-go 的 amqp.Table。amqp.Table 的底层类型就是
// map[string]any，这里做一次浅拷贝而不是直接类型转换，避免调用方在发布之后继续持有并
// 修改原 map 时，意外改动已经交给 xamqp 的那一份（例如 xamqp 内部在重连重放时可能复用
// Publishing 结构体）。
func HeadersFromMap(m map[string]any) amqp.Table {
	if len(m) == 0 {
		return nil
	}
	t := make(amqp.Table, len(m))
	maps.Copy(t, m)
	return t
}

// HeadersToMap 是 HeadersFromMap 的反向转换，供消费侧从 amqp.Delivery.Headers 构造
// iproto.Envelope 使用，同样做浅拷贝。
func HeadersToMap(t amqp.Table) map[string]any {
	if len(t) == 0 {
		return nil
	}
	m := make(map[string]any, len(t))
	maps.Copy(m, t)
	return m
}
