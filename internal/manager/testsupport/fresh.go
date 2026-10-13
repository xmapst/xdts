package testsupport

import (
	"fmt"
	"testing"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// FreshEmptyDB 新建一个当前测试独占、随机命名的临时 schema，不跑任何
// 迁移，测试结束后 DROP。给连"迁移已经跑过"这个前提都不能假设的测试用
// （迁移自身的正确性/幂等性测试、分区维护测试的自建 scratch 表），调用方
// 自己决定要不要迁移、迁几次。
func FreshEmptyDB(t *testing.T) *gorm.DB {
	t.Helper()
	managerMySQL.Check(t)

	cfg, err := parseTestDSN()
	if err != nil {
		managerMySQL.Unavailable(t, err.Error())
	}
	adminDB, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		managerMySQL.Unavailable(t, fmt.Sprintf("无法连接管理库测试实例: %v", err))
	}
	adminSQLDB, err := adminDB.DB()
	if err != nil {
		managerMySQL.Unavailable(t, fmt.Sprintf("无法获取底层 *sql.DB: %v", err))
	}
	if err := adminSQLDB.Ping(); err != nil {
		managerMySQL.Unavailable(t, fmt.Sprintf("管理库测试实例不可达: %v", err))
	}
	t.Cleanup(func() { _ = adminSQLDB.Close() })

	suffix, err := randomHex()
	if err != nil {
		t.Fatalf("%v", err)
	}
	schema := "xdts_test_" + suffix
	if err := adminDB.Exec("CREATE DATABASE `" + schema + "` CHARACTER SET utf8mb4").Error; err != nil {
		t.Fatalf("创建临时测试 schema %s 失败: %v", schema, err)
	}
	t.Cleanup(func() {
		if err := adminDB.Exec("DROP DATABASE IF EXISTS `" + schema + "`").Error; err != nil {
			t.Logf("清理临时测试 schema %s 失败（需要人工清理）: %v", schema, err)
		}
	})

	testCfg := *cfg
	testCfg.DBName = schema
	db, err := gorm.Open(mysql.Open(testCfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatalf("连接临时测试 schema %s 失败: %v", schema, err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("无法获取底层 *sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	return db
}

// FreshDB 新建测试独占 schema，执行 SetMigrator 注册的迁移，测试结束后删除。
func FreshDB(t *testing.T) *gorm.DB {
	t.Helper()
	db := FreshEmptyDB(t)
	if migrator == nil {
		t.Fatalf("testsupport: 未注册迁移函数，请在 TestMain 里调用 testsupport.SetMigrator")
	}
	if err := migrator(db); err != nil {
		t.Fatalf("迁移失败: %v", err)
	}
	return db
}
