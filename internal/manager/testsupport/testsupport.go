// Package testsupport manages disposable MySQL schemas for Manager tests.
// SharedDB resets one process-owned migrated schema; FreshEmptyDB and FreshDB
// create isolated schemas. Cleanup only touches schemas created by these fixtures.
//
// XDTS_MGR_MYSQL_DSN overrides the connection. Required environments fail when
// unavailable; optional local environments skip through the common testenv gate.
// Fixture reset uses DELETE so GTID-enabled foreign-key handling remains valid.
package testsupport

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/xmapst/xdts/internal/testenv"
)

// managerMySQL 定义管理库测试的环境要求，使用 testenv 统一处理必需环境和本地跳过。
var managerMySQL = testenv.ManagerMySQL

const defaultMySQLDSN = "root:root@tcp(127.0.0.1:33071)/xdts?charset=utf8mb4&parseTime=True&loc=UTC&timeout=2s&readTimeout=5s&writeTimeout=5s"

func testDSN() string {
	if v := os.Getenv("XDTS_MGR_MYSQL_DSN"); v != "" {
		return v
	}
	return defaultMySQLDSN
}

// parseTestDSN 解析测试 DSN，返回一个 DBName 已清空（可用于建/删 schema、
// 查询 information_schema）的配置副本。
func parseTestDSN() (*mysqldriver.Config, error) {
	cfg, err := mysqldriver.ParseDSN(testDSN())
	if err != nil {
		return nil, fmt.Errorf("解析测试 DSN 失败: %w", err)
	}
	adminCfg := *cfg
	adminCfg.DBName = ""
	return &adminCfg, nil
}

// randomHex 生成一个 8 位十六进制随机后缀，用于临时测试 schema 命名，
// 避免并发跑测试（多进程/多次 go test 调用）时互相踩踏。
func randomHex() (string, error) {
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("生成随机后缀失败: %w", err)
	}
	return hex.EncodeToString(b), nil
}
