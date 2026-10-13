package logship

import (
	"errors"

	"github.com/xmapst/xdts/internal/iproto"
)

// ModuleName 是 logship 模块在 xhive 里注册的名字（静态模块，Priority 20，
// 与 reporter 同优先级、完全独立的通道与队列）。
const ModuleName = "logship"

// LogRecordMsg 由 slog Handler 投递给 logship；发送方投递后不再修改记录。
type LogRecordMsg struct {
	Record *iproto.LogRecord
}

func (m LogRecordMsg) validate() error {
	if m.Record == nil {
		return errors.New("logship: LogRecordMsg.Record 不能为空")
	}
	return nil
}
