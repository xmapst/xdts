package repo

// alertsDedupeFieldsMigration 增加告警去重键、命中次数和最后命中时间。
// 仅 firing 记录参与去重，普通索引配合事务行锁允许恢复后的主体再次触发新记录。
// 空去重键表示不参与去重；初始命中次数为 1，未知最后命中时刻为 NULL。
func alertsDedupeFieldsMigration() migration {
	return migration{
		Version:     44,
		Description: "add alerts dedupe fields",
		Statements: []string{`
ALTER TABLE alerts
  ADD COLUMN dedupe_key VARCHAR(191) NOT NULL DEFAULT '',
  ADD COLUMN hit_count BIGINT NOT NULL DEFAULT 1,
  ADD COLUMN last_seen_at DATETIME(3) NULL,
  ADD KEY idx_alerts_dedupe_key (dedupe_key)`,
		},
	}
}
