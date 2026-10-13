package repo

// 节点注销按资源记录回收状态，复用流水线回收字段的形状和语义。
func nodesCleanupFieldsMigration() migration {
	return migration{
		Version:     29,
		Description: "add nodes cleanup fields",
		Statements: []string{`
ALTER TABLE nodes
  ADD COLUMN cleanup_state VARCHAR(16) NOT NULL DEFAULT 'done',
  ADD COLUMN cleanup_detail JSON NULL,
  ADD COLUMN cleanup_attempts INT NOT NULL DEFAULT 0,
  ADD COLUMN cleanup_last_error TEXT NULL,
  ADD COLUMN cleanup_last_attempt_at DATETIME(3) NULL,
  ADD INDEX idx_nodes_deleted_cleanup_state (deleted_at, cleanup_state)`,
		},
	}
}
