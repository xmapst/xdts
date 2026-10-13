package repo

// settingUpdatedByFieldMigration 给 settings 表补 updated_by 列，对应
// internal/manager/model.Setting.UpdatedBy。
//
// 设置项是运行期可调的权威参数，改动必须能追溯到人。原 v24 建表没有这一列，
// 已发布的契约只增不改语义 ⇒ 这里新增可空列而不是改写 v24。
//
// 允许 NULL：v45 之前落地的行没有这个信息，迁移不能凭空编一个用户。
func settingUpdatedByFieldMigration() migration {
	return migration{
		Version:     45,
		Description: "add settings updated_by",
		Statements: []string{
			"ALTER TABLE settings ADD COLUMN updated_by BINARY(16) NULL",
		},
	}
}
