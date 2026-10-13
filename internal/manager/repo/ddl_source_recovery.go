package repo

func sourceRecoveryStatesMigration() migration {
	return migration{Version: 68, Description: "persist source recovery checkpoints and epoch ceilings", Statements: []string{`
CREATE TABLE IF NOT EXISTS source_recovery_states (
 pipeline_id BINARY(16) NOT NULL PRIMARY KEY,
 epoch_ceiling BIGINT UNSIGNED NOT NULL,
 checkpoint_json JSON NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}

func sourceEpochLeasesMigration() migration {
	return migration{Version: 69, Description: "persist idempotent source epoch range requests", Statements: []string{`
CREATE TABLE IF NOT EXISTS source_epoch_leases (
 request_id BINARY(16) NOT NULL PRIMARY KEY,
 pipeline_id BINARY(16) NOT NULL,
 request_digest CHAR(64) NOT NULL,
 reply_json JSON NOT NULL,
 KEY idx_source_epoch_leases_pipeline (pipeline_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
