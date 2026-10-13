package repo

// Add independent role observations and backfill aggregate state.
func pipelineRoleActualStateFieldMigration() migration {
	return migration{
		Version:     38,
		Description: "add pipelines source/target actual_state and last_error fields",
		Statements: []string{`
ALTER TABLE pipelines
  ADD COLUMN source_actual_state VARCHAR(16) NOT NULL DEFAULT 'draft',
  ADD COLUMN source_last_error TEXT NULL,
  ADD COLUMN target_actual_state VARCHAR(16) NOT NULL DEFAULT 'draft',
  ADD COLUMN target_last_error TEXT NULL,
  ADD INDEX idx_pipelines_desired_source_actual_state (desired_state, source_actual_state),
  ADD INDEX idx_pipelines_desired_target_actual_state (desired_state, target_actual_state)`,
			`
UPDATE pipelines SET
  source_actual_state = actual_state, source_last_error = last_error,
  target_actual_state = actual_state, target_last_error = last_error`,
		},
	}
}
