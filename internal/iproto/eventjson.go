package iproto

import (
	"bytes"
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// MarshalEventDetailJSON encodes an event's oneof detail for events.payload.
// Scalar and enum defaults are emitted; absent nested messages remain absent.
// Compact output keeps the persisted JSON shape consistent across producers.
// A nil detail returns (nil, nil).
func MarshalEventDetailJSON(detail proto.Message) ([]byte, error) {
	if detail == nil {
		return nil, nil
	}
	raw, err := protojson.MarshalOptions{
		UseProtoNames:     true,
		EmitDefaultValues: true,
	}.Marshal(detail)
	if err != nil {
		return nil, fmt.Errorf("iproto: 序列化事件详情失败: %w", err)
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		// protojson 的输出必然是合法 JSON，走到这里说明上游库行为变了，
		// 不静默返回未压实的字节——那会让两种形状同时进同一张表。
		return nil, fmt.Errorf("iproto: 压实事件详情 JSON 失败: %w", err)
	}
	return buf.Bytes(), nil
}
