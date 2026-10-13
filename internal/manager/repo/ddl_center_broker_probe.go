package repo

// 两列命名与类型照抄 datasourcesMigration（v5，先后改名为 data_endpoints
// 表【v32】、endpoints 表【v34】）里 last_probe_at/last_probe_result 的
// 既有写法：DATETIME(3) NULL 表示「从未
// 探测过」用 NULL 而不是零值时间（避免 Go 零值 0001-01-01 落库时超出
// MySQL DATETIME 取值范围报错）；JSON NULL 承载探测结果全文，不建二级
// 索引、不做强类型列展开——具体 schema 由探测协议约定，仓储层不关心。
//
// 纯新增列，不改写 centersMigration（v3）本身：字段变更一律通过追加新
// 版本迁移完成，保持已应用版本可审计、可重放，与本文件其余迁移（v27~v32）
// 的既有约定一致。
//
// 幂等性说明：与 v27/v29/v30/v32（ALTER TABLE ADD COLUMN/RENAME 系列）
// 相同，依赖 Migrate() 按 schema_versions 记录「一个版本号只应用一次」，
// 不依赖 ALTER TABLE 语句自身可重复执行。
func centerBrokerProbeFieldsMigration() migration {
	return migration{
		Version:     33,
		Description: "add centers broker probe result fields",
		Statements: []string{`
ALTER TABLE centers
  ADD COLUMN last_broker_probe_at DATETIME(3) NULL,
  ADD COLUMN last_broker_probe_result JSON NULL`,
		},
	}
}
