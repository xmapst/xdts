package repo

// Make recovery exhaustion nullable: NULL means absent and numeric zero is valid.
// Convert zero sentinels during this migration; subsequent zero records retain meaning.

func pipelineRecoveryExhaustedNullableMigration() migration {
	return migration{
		Version:     41,
		Description: "make pipelines recovery_exhausted_cmd_seq nullable",
		Statements: []string{`
ALTER TABLE pipelines
  MODIFY COLUMN recovery_exhausted_cmd_seq BIGINT UNSIGNED NULL`,
		},
	}
}

func pipelineRecoveryExhaustedZeroToNullMigration() migration {
	return migration{
		Version:     42,
		Description: "rewrite legacy recovery_exhausted_cmd_seq 0 to NULL",
		Statements: []string{`
UPDATE pipelines SET recovery_exhausted_cmd_seq = NULL WHERE recovery_exhausted_cmd_seq = 0`,
		},
	}
}
