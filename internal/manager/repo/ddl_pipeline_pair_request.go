package repo

func pipelinePairRequestsMigration() migration {
	return migration{Version: 51, Description: "create pipeline_pair_requests", Statements: []string{`
CREATE TABLE IF NOT EXISTS pipeline_pair_requests (
 id BINARY(16) NOT NULL,
 created_at DATETIME(3) NOT NULL,
 updated_at DATETIME(3) NOT NULL,
 requested_by BINARY(16) NOT NULL,
 digest CHAR(64) NOT NULL,
 forward_id BINARY(16) NOT NULL,
 reverse_id BINARY(16) NOT NULL,
 PRIMARY KEY(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`}}
}
