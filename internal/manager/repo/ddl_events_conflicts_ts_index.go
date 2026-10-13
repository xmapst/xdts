package repo

// eventsConflictsTsIndexMigration adds timestamp indexes for bounded observation queries.
// InnoDB includes the primary key in secondary indexes, supporting (ts, id) ordering.
func eventsConflictsTsIndexMigration() migration {
	return migration{
		Version:     31,
		Description: "add (ts) index to events/conflicts for ListEvents/ListConflicts ORDER BY ts DESC,id DESC pagination (HTTP layer uses page/pageSize offset pagination, not the repo-layer BeforeTs/BeforeID cursor fields; single-column ts suffices either way: InnoDB secondary index implicitly carries PK id); COUNT paths unaffected, still use existing composite-index skip scan",
		Statements: []string{
			"ALTER TABLE events ADD INDEX idx_events_ts (ts)",
			"ALTER TABLE conflicts ADD INDEX idx_conflicts_ts (ts)",
		},
	}
}
