package repo

import (
	"fmt"
	"time"
)

// pipelineMetricsMigration 建 pipeline_metrics 表（需求
// MGR-MON-09）。分区策略与 logsMigration 相同，见其注释。
func pipelineMetricsMigration(now time.Time) migration {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS pipeline_metrics (
  id BINARY(16) NOT NULL,
  ts DATETIME(3) NOT NULL,
  pipeline_id BINARY(16) NOT NULL,
  delay_ms BIGINT NOT NULL DEFAULT 0,
  rps DOUBLE NOT NULL DEFAULT 0,
  bps DOUBLE NOT NULL DEFAULT 0,
  queue_depth BIGINT NOT NULL DEFAULT 0,
  errors BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (id, ts),
  KEY idx_pipeline_metrics_pipeline_ts (pipeline_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
%s`, initialPartitionClause(now, DefaultPartitionLookahead))

	return migration{
		Version:     26,
		Description: "create pipeline_metrics (partitioned)",
		Statements:  []string{ddl},
	}
}
