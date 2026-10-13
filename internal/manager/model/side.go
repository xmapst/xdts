package model

// Side 标识 pipeline 的源端或目标端角色，供 pipeline_releases 与
// pipeline_checkpoints 共用。
//
// 这是管理库持久化层自己的类型，与 internal/iproto 或 internal/rule 中
// 可能存在的同名协议类型无关，不跨模块共享（跨模块结构
// 放 internal/iproto，不把某模块的私有模型提升为共享契约；反之亦然，
// 数据库落盘类型也不必依赖协议包）。
type Side string

const (
	SideSource Side = "source"
	SideTarget Side = "target"
)

// Valid 校验 side 取值。
func (s Side) Valid() bool {
	switch s {
	case SideSource, SideTarget:
		return true
	default:
		return false
	}
}
