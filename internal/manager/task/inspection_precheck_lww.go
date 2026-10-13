package task

type precheckLWWFallbackTable struct {
	SourceSchema string `json:"source_schema"`
	SourceTable  string `json:"source_table"`
	TargetSchema string `json:"target_schema"`
	TargetTable  string `json:"target_table"`
}

// The selected scope already resolved case sensitivity and name mappings.
func (s *pipelinePrecheckScope) lwwFallbackTables() []precheckLWWFallbackTable {
	tables := make([]precheckLWWFallbackTable, 0)
	for index, source := range s.source.GetTables() {
		if source.GetLwwTimestampColumn() != "" {
			continue
		}
		target := s.target.GetTables()[index]
		tables = append(tables, precheckLWWFallbackTable{SourceSchema: source.GetSchema(), SourceTable: source.GetTable(), TargetSchema: target.GetSchema(), TargetTable: target.GetTable()})
	}
	return tables
}
