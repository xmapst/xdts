package repo

// alertRuleIDNullableMigration 把 alerts.rule_id 改为可空。
//
// 有一类告警没有对应的 alert_rules 行：登录锁定（阈值写在代码里、不由用户
// 配置）、本地存储异常这类系统自身发出的告警，RuleID 只能是 uuidx.Nil。
// 而 uuidx.UUID.Value() 把 Nil 落成 NULL ⇒ 在 NOT NULL 列上直接
// `Error 1048 (23000): Column 'rule_id' cannot be null`，一条也插不进去。
//
// 那样 alert_rules 会混进用户从未配置、也不能删的行，GET /alert-rules 必须
// 再加一层过滤才能不把它们列出来——把一个存储约束问题搬到了每个读取方身上。
//
// 红线「已发布的契约只增不改语义」：v20 可能已经在环境里执行过，改写它对那些
// 环境完全不生效。这里沿用 `internal/manager/repo/ddl_pipeline_recovery_exhausted_nullable.go`
// （v41）的先例，用一条独立的 MODIFY 迁移。存量行的 rule_id 都非 NULL，
// 放宽约束不需要配套的数据修补语句。
func alertRuleIDNullableMigration() migration {
	return migration{
		Version:     46,
		Description: "modify alerts rule_id nullable",
		Statements: []string{
			"ALTER TABLE alerts MODIFY rule_id BINARY(16) NULL",
		},
	}
}
