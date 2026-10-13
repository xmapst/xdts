package main

import (
	"fmt"
	"log/slog"
	"strings"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/xmapst/xdts/internal/manager/config"
)

const managerDBIsolation = "REPEATABLEREAD"

func openManagerDB(cfg config.DatabaseConfig) (*gorm.DB, error) {
	db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Warn),
	})
	if err != nil {
		return nil, fmt.Errorf("gorm 连接失败: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("获取底层 *sql.DB 失败: %w", err)
	}
	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("管理库不可达: %w", err)
	}
	if err := assertManagerDBIsolation(db); err != nil {
		_ = sqlDB.Close()
		return nil, err
	}
	return db, nil
}

func assertManagerDBIsolation(db *gorm.DB) error {
	var session string
	if err := db.Raw("SELECT @@transaction_isolation").Scan(&session).Error; err != nil {
		return fmt.Errorf("无法读取管理库事务隔离级别: %w", err)
	}
	if normalizeIsolationLevel(session) == managerDBIsolation {
		slog.Info("manager db isolation level verified", slog.String("isolation", session))
		return nil
	}

	// 全局值只进错误信息，取不到就留空——它不参与判定，读失败不该改变结论。
	var global string
	if err := db.Raw("SELECT @@global.transaction_isolation").Scan(&global).Error; err != nil {
		global = "(读取失败)"
	}
	return fmt.Errorf(
		"管理库事务隔离级别必须是 REPEATABLE READ，当前会话级为 %q（全局级 %q）——"+
			"多处 SELECT ... FOR UPDATE 的互斥依赖 REPEATABLE READ 的间隙锁，"+
			"READ COMMITTED 下会静默退化为可以并发插入重复行（其中告警去重守的是账号锁定这项安全控制）。"+
			"请在 MySQL 服务端配置 transaction-isolation=REPEATABLE-READ（推荐），"+
			"或在 DSN 中指定 transaction_isolation=%%27REPEATABLE-READ%%27"+
			"（**值必须自带引号并 URL 编码**：裸写 transaction_isolation=REPEATABLE-READ 会得到 "+
			"Error 1064 语法错误，因为驱动把未识别参数原样拼进 SET 语句，字符串型系统变量的值需要引号）"+
			"，改完重启",
		session, global)
}

func normalizeIsolationLevel(v string) string {
	r := strings.ToUpper(strings.TrimSpace(v))
	r = strings.ReplaceAll(r, "-", "")
	r = strings.ReplaceAll(r, "_", "")
	r = strings.ReplaceAll(r, " ", "")
	return r
}
