package mysqlout

import (
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"google.golang.org/protobuf/proto"
)

type conflictMask struct {
	schema, table string
	columns       map[string]bool
}

// Called after Writer options acquire ownership, including at a runtime mapping
// boundary. The caller clears the previous compiled masks before replacement.
func (w *Writer) compileConflictMasks() {
	for _, rule := range w.opts.ConflictPolicy.GetMaskRules() {
		schema, table, columns := w.opts.Mapping.TargetNames(rule.GetSchema(), rule.GetTable(), rule.GetColumns(), w.opts.SchemaOptions.SourceCaseSensitive)
		mask := conflictMask{schema: schema, table: table, columns: make(map[string]bool, len(columns))}
		for _, column := range columns {
			mask.columns[strings.ToLower(column)] = true
		}
		w.conflictMasks = append(w.conflictMasks, mask)
	}
}

// emitConflict transfers a report copy. It never changes the transaction's
// row values, which are still needed for policy comparison and SQL writes.
func (w *Writer) emitConflict(detail *iproto.ConflictEventDetail) {
	if w.conflictSink == nil || detail == nil {
		return
	}
	report := proto.Clone(detail).(*iproto.ConflictEventDetail)
	rc := report.GetChange()
	masked := make(map[string]bool)
	for _, rule := range w.conflictMasks {
		matches := rule.schema == rc.GetSchema() && rule.table == rc.GetTable()
		if !w.opts.SchemaOptions.SourceCaseSensitive {
			matches = strings.EqualFold(rule.schema, rc.GetSchema()) && strings.EqualFold(rule.table, rc.GetTable())
		}
		if matches {
			for column := range rule.columns {
				masked[column] = true
			}
		}
	}
	if len(masked) > 0 && rc != nil {
		for i, column := range rc.GetColumnNames() {
			if !masked[strings.ToLower(column)] {
				continue
			}
			for _, row := range []*iproto.Row{rc.GetBefore(), rc.GetAfter(), report.GetTargetRow()} {
				if row != nil && i < len(row.Values) {
					row.Values[i] = &iproto.ColumnValue{Value: &iproto.ColumnValue_Text{Text: "***"}}
				}
			}
		}
		report.RedactedChange, report.Change = rc, nil
		report.RedactedTargetRow, report.TargetRow = report.TargetRow, nil
	}
	w.conflictSink(report)
}
