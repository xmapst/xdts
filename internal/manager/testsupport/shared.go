package testsupport

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 另有一条形式上的冲突：仓规要求 message 必须是字面量、变量进字段，而这 5 条
// 全是带 `%s`/`%d` 的插值。改成 slog 就得把它们拆成"字面量 message + 字段"，
// 那是一次真正的改造，不是替换函数名——不要顺手做。
//
// 本条只有注释级保护。 没有任何结构能让"本文件可以用 log"这件事在被违反时
// 变红：把这 5 条改成 slog，编译过、测试绿，只是清扫失败的提示从此不再露面。
// 反方向（别的文件开始用 log）同样不会红。这是第 3 档，按规矩在原地写明。
const staleSharedSchemaAge = 30 * time.Minute

// sharedState 是 SharedDB 的一次性初始化结果，用 sync.Once 计算一次，
// 之后每次 SharedDB(t) 调用都把这个结果套用到调用方自己的 t 上（t.Skip/
// t.Fatalf），而不是只有触发 sync.Once.Do 的第一个测试能正确报告失败——
// t.Skip/t.Fatalf 内部靠 runtime.Goexit 实现，如果直接在 sync.Once.Do 的
// 闭包里对某个测试的 t 调用，只有第一个测试能被正确终止，后面的测试会
// 在没有可用 db 的情况下继续跑并崩溃在别的地方，报错信息跟真正原因对不
// 上。所以这里的初始化函数不接触任何 *testing.T，只把结果（成功用
// db/tables/baseline，失败用 unavailableReason 或 err）存起来，由每次
// SharedDB 调用各自处理。
type sharedState struct {
	db       *gorm.DB
	admin    *gorm.DB
	schema   string
	tables   []string // 业务表清单，从 information_schema 动态读取，不硬编码——迁移增删表时自动跟着变，不需要同步维护这里的列表
	baseline string   // 迁移完成后的结构指纹基线，见 structuralFingerprint

	unavailableReason string
	err               error
}

var (
	sharedOnce sync.Once
	shared     sharedState
)

// 这个顺序带一个已知的、可接受的边界：如果一个测试对 schema 做了结构性
// 变更且它是整个进程里最后一个调用 SharedDB 的测试，不会有下一次调用来
// 触发对它的指纹校验——违规不会被本次运行发现，只会在下一次运行的第一个
// 测试上意外报错（如果那次运行还用同一个已损坏的持久化 schema；本包默认
// 每次运行都新建 schema，所以实际后果通常只是"这次运行没抓到"，不是"状态
// 一直往后传"）。这是个记录在案的取舍，不是遗漏。
//
// 不要在会对 schema 做结构性变更的测试里用这个——用 FreshDB。
func SharedDB(t *testing.T) *gorm.DB {
	t.Helper()
	managerMySQL.Check(t)
	sharedOnce.Do(func() { shared = buildSharedState() })

	if shared.unavailableReason != "" {
		managerMySQL.Unavailable(t, shared.unavailableReason)
	}
	if shared.err != nil {
		t.Fatalf("共享测试 schema 不可用: %v", shared.err)
	}

	resetShared(t)
	return shared.db
}

// TeardownShared 删除本进程建的共享测试 schema（如果建过）。调用方在自己
// 包的 TestMain 里、m.Run() 之后调用：
//
//	func TestMain(m *testing.M) {
//		code := m.Run()
//		testsupport.TeardownShared()
//		os.Exit(code)
//	}
//
// 测试二进制被 kill（go test 超时强杀、ctrl-C）时这个函数不会被执行到，
// 那种情况交给下一次运行时的陈旧 schema 清扫处理——两层合起来才是完整
// 的清理策略：正常退出靠这个，异常退出靠下一轮的清扫，不靠人工发现
// 泄漏。
func TeardownShared() {
	if shared.db == nil || shared.admin == nil || shared.schema == "" {
		return
	}
	if err := shared.admin.Exec("DROP DATABASE IF EXISTS `" + shared.schema + "`").Error; err != nil {
		log.Printf("testsupport: 清理共享测试 schema %s 失败（需要人工清理，下一轮运行的陈旧 schema 清扫也会处理）: %v", shared.schema, err)
	}
}

func buildSharedState() sharedState {
	cfg, err := parseTestDSN()
	if err != nil {
		return sharedState{unavailableReason: err.Error()}
	}
	adminDB, err := gorm.Open(mysql.Open(cfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return sharedState{unavailableReason: fmt.Sprintf("无法连接管理库测试实例: %v", err)}
	}
	adminSQLDB, err := adminDB.DB()
	if err != nil {
		return sharedState{unavailableReason: fmt.Sprintf("无法获取底层 *sql.DB: %v", err)}
	}
	if err := adminSQLDB.Ping(); err != nil {
		return sharedState{unavailableReason: fmt.Sprintf("管理库测试实例不可达: %v", err)}
	}

	sweepStaleSharedSchemas(adminDB, staleSharedSchemaAge)

	suffix, err := randomHex()
	if err != nil {
		return sharedState{err: err}
	}

	schema := sharedSchemaName(os.Getpid(), suffix)
	if err := adminDB.Exec("CREATE DATABASE `" + schema + "` CHARACTER SET utf8mb4").Error; err != nil {
		return sharedState{err: fmt.Errorf("创建共享测试 schema %s 失败: %w", schema, err)}
	}

	testCfg := *cfg
	testCfg.DBName = schema
	db, err := gorm.Open(mysql.Open(testCfg.FormatDSN()), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		return sharedState{err: fmt.Errorf("连接共享测试 schema %s 失败: %w", schema, err)}
	}

	if migrator == nil {
		return sharedState{err: fmt.Errorf("未注册迁移函数，请在 TestMain 里调用 testsupport.SetMigrator")}
	}
	if err := migrator(db); err != nil {
		return sharedState{err: fmt.Errorf("共享测试 schema 初始迁移失败: %w", err)}
	}

	tables, err := businessTables(db, schema)
	if err != nil {
		return sharedState{err: fmt.Errorf("读取业务表清单失败: %w", err)}
	}
	baseline, err := structuralFingerprint(db, schema)
	if err != nil {
		return sharedState{err: fmt.Errorf("计算结构指纹基线失败: %w", err)}
	}

	return sharedState{db: db, admin: adminDB, schema: schema, tables: tables, baseline: baseline}
}

// resetShared 在调用方（当前测试）拿到共享 db 之前复位它：先 DELETE
// 全部业务表（不是 TRUNCATE，原因见包文档——DELETE 在测试场景的行数量级
// 下快两个数量级，且不受系统负载影响），再校验结构指纹，确认上一个用过
// 这个 schema 的测试没有留下结构性变更。DELETE 与 TRUNCATE 都不改变表
// 结构，所以先做哪一步不影响指纹校验的正确性，这里选先 DELETE 再校验。
//
// 注意这里校验的是"上一个测试"而不是"当前测试"的结构完整性——复位在
// 当前测试的业务代码跑之前执行，当前测试自己如果做了结构性变更，要等到
// 再下一个调用 SharedDB 的测试才会被发现（见 SharedDB 文档注释里记录的
// 已知边界）。
func resetShared(t *testing.T) {
	t.Helper()
	for _, tbl := range shared.tables {
		if err := shared.db.Exec("DELETE FROM `" + tbl + "`").Error; err != nil {
			t.Fatalf("复位共享测试 schema 失败（DELETE FROM %s）: %v", tbl, err)
		}
	}
	cur, err := structuralFingerprint(shared.db, shared.schema)
	if err != nil {
		t.Fatalf("复位后校验共享测试 schema 结构指纹失败: %v", err)
	}
	if cur != shared.baseline {
		t.Fatalf("共享测试 schema 的结构已被改变（结构指纹从 %s 变为 %s），大概率是上一个使用它的测试做了结构性变更（ADD/DROP/MODIFY COLUMN、ADD/DROP INDEX、整表 CREATE/DROP、ADD/DROP/REORGANIZE PARTITION）——这类测试必须改用 testsupport.FreshDB，不能用 SharedDB", shared.baseline, cur)
	}
}

// businessTables 返回 schema 下除 schema_versions / schema_migration_statements（迁移引擎记录
// 表，不能被清空，否则下一次 Migrate 会重新执行所有迁移）之外的全部表名，
// 按名称排序。从 information_schema 动态读取而不是硬编码列表：新迁移
// 增删表时这里自动跟着变，不需要额外维护、不会漂移。
func businessTables(db *gorm.DB, schema string) ([]string, error) {
	var tables []string
	if err := db.Raw(
		"SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? AND TABLE_NAME NOT IN ('schema_versions','schema_migration_statements') ORDER BY TABLE_NAME",
		schema,
	).Scan(&tables).Error; err != nil {
		return nil, fmt.Errorf("查询业务表清单失败: %w", err)
	}
	return tables, nil
}

// structuralFingerprint 计算 schema 的结构指纹：全部列（表名+列名+类型+
// 是否可空）与全部索引（表名+索引名+序号+列名）按固定顺序拼接后取
// sha256。
//
// 会改变它的（闭区间，就这些）：ADD/DROP/MODIFY COLUMN、ADD/DROP INDEX、
// 整表的 CREATE/DROP（连带改变列集合）、ADD/DROP/REORGANIZE PARTITION。
// 用于在共享 schema 上抓"本该只做数据读写、却顺手做了结构性变更"的测试。
//
// 不会改变它的：纯数据操作——DELETE、TRUNCATE TABLE、TRUNCATE PARTITION。
// 它们不改结构，不触发是正确行为。
//
// （最初报的是 2.1~5.7ms，那是"两臂各自取中位数再相减"算出来的，没控住轮内
// 漂移、系统性高估。配对测量把它修正到 ~1.7ms。方法本身比数字值得记：两个
// 分别测量的中位数相减，减不掉测量期间的漂移。）
func structuralFingerprint(db *gorm.DB, schema string) (string, error) {
	var colRows []string
	if err := db.Raw(`
		SELECT CONCAT(TABLE_NAME, '.', COLUMN_NAME, ':', COLUMN_TYPE, ':', IS_NULLABLE)
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, ORDINAL_POSITION`, schema).Scan(&colRows).Error; err != nil {
		return "", fmt.Errorf("读取列信息失败: %w", err)
	}
	var idxRows []string
	if err := db.Raw(`
		SELECT CONCAT(TABLE_NAME, '.', INDEX_NAME, '.', SEQ_IN_INDEX, ':', COLUMN_NAME)
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ?
		ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX`, schema).Scan(&idxRows).Error; err != nil {
		return "", fmt.Errorf("读取索引信息失败: %w", err)
	}
	// 取名字、序号和边界描述三样：只取名字会漏掉 REORGANIZE PARTITION
	// 把边界改了而名字没变的情形。多取这两列不花钱——成本全在"多发一次
	// 查询"这件事本身，与返回多少列无关：窄查法（只取表名+分区名）单独耗时
	// 与全量版的增量同量级，两者都是一次往返。
	var partRows []string
	if err := db.Raw(`
		SELECT CONCAT(TABLE_NAME, '.', PARTITION_NAME, ':', IFNULL(PARTITION_ORDINAL_POSITION, 0), ':', IFNULL(PARTITION_DESCRIPTION, ''))
		FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA = ? AND PARTITION_NAME IS NOT NULL
		ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION`, schema).Scan(&partRows).Error; err != nil {
		return "", fmt.Errorf("读取分区信息失败: %w", err)
	}

	h := sha256.New()
	for _, r := range colRows {
		h.Write([]byte(r))
		h.Write([]byte("\n"))
	}
	h.Write([]byte("--\n"))
	for _, r := range idxRows {
		h.Write([]byte(r))
		h.Write([]byte("\n"))
	}
	h.Write([]byte("--\n"))
	for _, r := range partRows {
		h.Write([]byte(r))
		h.Write([]byte("\n"))
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// sweepStaleSharedSchemas 清扫上一轮被强杀（go test 超时、ctrl-C 等）而
// 没能执行 TestMain 收尾的陈旧共享 schema。被杀的测试二进制不会来得及
// 调用 TeardownShared，如果不主动清扫，这些 schema 会一直堆积——上一轮
// 就因为这个原因误判过"DROP DATABASE 会卡住"（实际是 600s 超时杀掉了
// 整个二进制，t.Cleanup/TestMain 根本没机会跑，泄漏的 schema 是超时的
// 结果不是原因）。
//
// 主修复：陈旧判定完全放到 SQL 端用 NOW() 比较，不再把 Go 端
// time.Now()（宿主机时钟域）跟驱动解析出的 CREATE_TIME（服务器时钟域）
// 做跨时钟域比较——CREATE_TIME 与 NOW() 由同一个 MySQL 会话在同一个
// 时区语境下产出，天然一致，不受客户端 loc 参数影响（DSN 的 loc 也
// 顺手对齐成了 UTC，见 internal/manager/testsupport/testsupport.go，是第二层防护，不是唯一防线）。
//
// 清扫失败不阻断当前测试：宁可留给下一次清扫或人工处理，也不能因为
// 清扫本身出错就让所有测试都跑不起来。
func sweepStaleSharedSchemas(admin *gorm.DB, maxAge time.Duration) {
	var schemas []string
	// NOW(6)（微秒精度）而不是默认精度的 NOW()：CREATE_TIME 与不带精度
	// 参数的 NOW() 都只有整秒精度，建库和这条 sweep 查询如果落在同一秒
	// 内会打平（相等而不是严格小于），maxAge=0 这类边界值会因为这个平局
	// 判定不出"陈旧"——陈旧 schema 检查
	// DropsSchemaPastThreshold 最初就是被这个同秒平局绊住的（已用独立
	// 诊断程序验证根因，不是靠猜的）。生产场景 maxAge 固定是 30 分钟，
	// 这个平局窗口窄到不可能撞上，但既然 NOW(6) 免费、无副作用，直接把
	// 这类边界情况也消除掉。
	if err := admin.Raw(`
		SELECT TABLE_SCHEMA
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA LIKE 'xdts_shared_%'
		GROUP BY TABLE_SCHEMA
		HAVING MIN(CREATE_TIME) < NOW(6) - INTERVAL ? SECOND`,
		int64(maxAge/time.Second)).Scan(&schemas).Error; err != nil {
		log.Printf("testsupport: 扫描陈旧共享 schema 失败（不阻断本次测试）: %v", err)
		return
	}

	selfPID := os.Getpid()
	for _, schema := range schemas {
		if pid, ok := schemaOwnerPID(schema); ok {
			if pid == selfPID {
				// 当前调用时机（buildSharedState 里建自己的库之前）下
				// 理论上不会命中——纯粹是防御未来调用顺序变化。
				continue
			}
			if processAlive(pid) {
				log.Printf("testsupport: 共享 schema %s 按年龄判定已陈旧，但其所属进程 PID=%d 仍存活，纵深防御跳过（不删除）——如果这不是预期情况，说明年龄判定可能又出问题了，需要人工确认", schema, pid)
				continue
			}
		}
		if err := admin.Exec("DROP DATABASE IF EXISTS `" + schema + "`").Error; err != nil {
			log.Printf("testsupport: 清扫陈旧共享 schema %s 失败（需要人工清理）: %v", schema, err)
			continue
		}
		log.Printf("testsupport: 已清扫陈旧共享 schema %s（超过 %s 未清理，判定为被强杀的测试二进制遗留）", schema, maxAge)
	}
}

// sharedSchemaName includes the process owner so fixture cleanup cannot target
// a schema belonging to another test process.
func sharedSchemaName(pid int, suffix string) string {
	return fmt.Sprintf("xdts_shared_%d_%s", pid, suffix)
}

// schemaOwnerPID 从共享 schema 名（形如 xdts_shared_<pid>_<hex后缀>，见
// buildSharedState 里的命名）解析出建库进程的 PID。不符合这个命名模式
// 的返回 (0, false)，正常情况下不会出现不匹配，这里只是不 panic 地防御性
// 处理。
//
// 建名的一侧只有 sharedSchemaName 一处（生产路径与清扫测试的探针都经由
// 它），由 共享命名约束 钉住。
//
// 现在两侧共用 sharedSchemaName。但"共用"这件事本身也需要被钉住，否则下一
// 个人把探针改回自己拼字符串，什么都不会红——那条断言就是为此存在的。
func schemaOwnerPID(schema string) (int, bool) {
	const prefix = "xdts_shared_"
	rest, ok := strings.CutPrefix(schema, prefix)
	if !ok {
		return 0, false
	}
	pidStr, _, ok := strings.Cut(rest, "_")
	if !ok {
		return 0, false
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// processAlive 判断给定 PID 当前（同一台机器上）是否仍然存活，用于
// sweep 的纵深防御。os.FindProcess 在类 Unix 系统上总是成功，真正的
// 存活探测发生在 Signal(0)——它不会真的发送信号，只探测目标进程是否
// 存在。err == nil 对应"存活"；err 是 os.ErrProcessDone 对应"已死"——
// Go 标准库在 Unix 下会把底层 syscall.ESRCH 转换成这个哨兵错误再返回
// （见 Go 标准库 src/os/exec_unix.go 的 convertESRCH，实测确认过，不是凭文档假设：
// 最初直接判 errors.Is(err, syscall.ESRCH) 拿到的永远是 false，被
// 清理阈值检查 当场抓住）。
// 除此之外的错误（比如权限不足——理论上不该出现，本包所有测试进程都
// 在同一个用户下跑）无法证明进程已经不存在，保守当作存活处理，不删。
// 本包只在本机 Linux/WSL 开发环境下配合 docker-compose 的 MySQL 测试
// 实例使用，不需要跨平台。
func processAlive(pid int) bool {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return true
	}
	err = proc.Signal(syscall.Signal(0))
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrProcessDone) {
		return false
	}
	return true
}
