package repo

import (
	"fmt"
	"time"
)

// logsMigration 建 logs 表（需求 MGR-MON-02/09）。
//
// 按天 RANGE COLUMNS(ts) 分区，主键 (id, ts)（MySQL 要求分区列
// 包含在每个唯一键中）。初始分区覆盖 [now, now+7 天)，之后由
// EnsureFuturePartitions 按天续建。
func logsMigration(now time.Time) migration {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS logs (
  id BINARY(16) NOT NULL,
  ts DATETIME(3) NOT NULL,
  node_id BINARY(16) NULL,
  pipeline_id BINARY(16) NULL,
  level VARCHAR(8) NOT NULL,
  module VARCHAR(64) NOT NULL DEFAULT '',
  message TEXT NOT NULL,
  attrs JSON NULL,
  source VARCHAR(8) NOT NULL,
  PRIMARY KEY (id, ts),
  KEY idx_logs_ts (ts),
  KEY idx_logs_pipeline_ts (pipeline_id, ts),
  KEY idx_logs_node_ts (node_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
%s`, initialPartitionClause(now, DefaultPartitionLookahead))

	return migration{
		Version:     25,
		Description: "create logs (partitioned)",
		Statements:  []string{ddl},
	}
}
