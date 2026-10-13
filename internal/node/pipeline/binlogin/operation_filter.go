package binlogin

import "github.com/xmapst/xdts/internal/iproto"

type iRowOperationFilter interface {
	AllowInsert() bool
	AllowUpdate() bool
	AllowDelete() bool
}

// The compiled source Plan provides both object and operation filters. Custom
// table filters without operation switches retain the established allow-all
// default. Filtered events still finish their transaction and publish a watermark.
func (a *aggregator) allowRowOperation(kind iproto.ChangeType) bool {
	filter, ok := a.tableFilter.(iRowOperationFilter)
	if !ok {
		return true
	}
	switch kind {
	case iproto.ChangeType_CHANGE_TYPE_INSERT:
		return filter.AllowInsert()
	case iproto.ChangeType_CHANGE_TYPE_UPDATE:
		return filter.AllowUpdate()
	case iproto.ChangeType_CHANGE_TYPE_DELETE:
		return filter.AllowDelete()
	default:
		return false
	}
}
