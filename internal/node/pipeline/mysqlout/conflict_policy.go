package mysqlout

import (
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
)

func (w *Writer) lwwTimestampColumn(rc *iproto.RowChange) string {
	if w.opts.ConflictPolicy.GetMode() != iproto.ConflictMode_CONFLICT_MODE_LWW {
		return ""
	}
	for _, table := range w.opts.ConflictPolicy.GetLwwTables() {
		schema, name, cols := w.opts.Mapping.TargetNames(table.GetSchema(), table.GetTable(), []string{table.GetTimestampColumn()}, w.opts.SchemaOptions.SourceCaseSensitive)
		if (w.opts.SchemaOptions.SourceCaseSensitive && schema == rc.GetSchema() && name == rc.GetTable()) || (!w.opts.SchemaOptions.SourceCaseSensitive && strings.EqualFold(schema, rc.GetSchema()) && strings.EqualFold(name, rc.GetTable())) {
			return cols[0]
		}
	}
	return ""
}
