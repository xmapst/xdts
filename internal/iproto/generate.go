// Package iproto 定义控制面、上报面和数据面协议，以及信封、版本校验、签名和编解码。
// proto/*.proto 是生成源；执行 go generate 前需将 protoc 与 protoc-gen-go 加入 PATH。
package iproto

//go:generate protoc --proto_path=../../proto --go_out=paths=source_relative:. ../../proto/common.proto ../../proto/data.proto ../../proto/config.proto ../../proto/control.proto ../../proto/report.proto
