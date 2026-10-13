package schemamig

import "errors"

// 以下均为哨兵错误，配合 errors.Is 判定；具体上下文（列名、对象名等）通过
// fmt.Errorf("...: %w", ...) 包装后随 error 返回，不丢失错误链。
//
// 注意区分"返回 error"与"Result.Blocked=true"：前者表示调用方给的输入本身
// 不满足函数前置条件（如 Kind 与 DDL 实际类型不符、Mapping 配置非法），
// 属于调用方的编程错误；后者表示输入合法但源端定义本身无法被安全改写，
// 属于业务结果，不是错误——两者都不应该被静默吞掉或互相冒充。
var (
	// ErrUnsupportedObjectKind 表示调用方传入了本包不认识的 ObjectKind。
	ErrUnsupportedObjectKind = errors.New("schemamig: 不支持的对象类型")
	// ErrDDLKindMismatch 表示解析出的语句类型与调用方声明的 Kind 不一致
	// （例如声明 ObjectTable 但传入的文本实际是 CREATE VIEW）。
	ErrDDLKindMismatch = errors.New("schemamig: DDL 语句类型与声明的对象类型不一致")
	// ErrEmptyDDL 表示传入的定义文本为空。
	ErrEmptyDDL = errors.New("schemamig: 定义文本为空")
)
