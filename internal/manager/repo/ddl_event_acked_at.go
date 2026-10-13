package repo

// eventAckedAtFieldMigration 增加可空的确认时间。
// acked_by 表示是否已确认；acked_at 缺失表示确认时刻未知。
func eventAckedAtFieldMigration() migration {
	return migration{
		Version:     43,
		Description: "add events acked_at field",
		Statements: []string{`
ALTER TABLE events
  ADD COLUMN acked_at DATETIME(3) NULL`,
		},
	}
}
