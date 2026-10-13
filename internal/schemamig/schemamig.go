package schemamig

import "fmt"

// Rewrite 是本包唯一的公开入口：按 req.Kind 分发到对应对象类型的改写管线
// （ADR-08：表/视图走 tidb AST，存储过程/函数/触发器/事件走 ANTLR）。
func Rewrite(req Request) (Result, error) {
	if req.DDL == "" {
		return Result{}, ErrEmptyDDL
	}
	if req.Options.SourceDefaultSchema == "" {
		req.Options.SourceDefaultSchema = req.DefaultSchema
	}
	switch req.Kind {
	case ObjectTable:
		return rewriteTable(req)
	case ObjectView:
		return rewriteView(req)
	case ObjectProcedure:
		return rewriteProcedure(req)
	case ObjectFunction:
		return rewriteFunction(req)
	case ObjectTrigger:
		return rewriteTrigger(req)
	case ObjectEvent:
		return rewriteEvent(req)
	default:
		return Result{}, fmt.Errorf("%w: %v", ErrUnsupportedObjectKind, req.Kind)
	}
}
