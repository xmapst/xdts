package repo

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// Default retention periods for partitioned observation tables.
const (
	DefaultLoginLockoutRetention = 30 * 24 * time.Hour

	// DefaultPartitionLookahead 指定预创建未来 7 天的分区。
	DefaultPartitionLookahead = 7

	// DefaultBatchDeleteSize/DefaultBatchDeleteInterval 是非分区表退化清理
	// 的批量大小与批次间隔（「不分区时退化为按批 DELETE ...
	// LIMIT，单批 ≤ 5000 行，带 sleep 间隔」，避免一次大事务长时间持锁）。
	DefaultBatchDeleteSize     = 5000
	DefaultBatchDeleteInterval = 100 * time.Millisecond
)

// 按天 RANGE COLUMNS(ts) 分区的两张表名，供迁移与分区维护
// 调用方引用，避免表名散落各处的字符串字面量。
const (
	TableLogs            = "logs"
	TablePipelineMetrics = "pipeline_metrics"
)

// Validate SQL identifiers before interpolating table names into partition DDL.
var sqlIdentifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validateSQLIdentifier 校验 table 是否符合 sqlIdentifierPattern，
// 不通过时返回一个说明是哪个表名不合法的错误。
func validateSQLIdentifier(table string) error {
	if !sqlIdentifierPattern.MatchString(table) {
		return fmt.Errorf("repo: 表名不是合法的 SQL 标识符: %q", table)
	}
	return nil
}

// partitionName 返回某一天分区的名字：p + YYYYMMDD。
func partitionName(day time.Time) string {
	return "p" + day.Format("20060102")
}

// partitionUpperBound 返回该天分区 VALUES LESS THAN 子句的边界（该天的
// 下一天零点），用字符串字面量嵌入 DDL——分区定义语句不支持占位符参数。
func partitionUpperBound(day time.Time) string {
	return day.AddDate(0, 0, 1).Format("2006-01-02 15:04:05")
}

// truncateToDay 去掉时分秒，保留调用方所在时区的日历日，作为分区边界的
// 计算基准。
func truncateToDay(t time.Time) time.Time {
	y, mo, d := t.Date()
	return time.Date(y, mo, d, 0, 0, 0, 0, t.Location())
}

// initialPartitionClause 生成建表语句里的初始分区定义：从 day0 起共
// count 天，每天一个分区。count 至少为 1（MySQL 分区表建表时必须至少带
// 一个分区定义）。
func initialPartitionClause(day0 time.Time, count int) string {
	if count < 1 {
		count = 1
	}
	day0 = truncateToDay(day0)
	var clause strings.Builder
	clause.WriteString("PARTITION BY RANGE COLUMNS (ts) (\n")
	for i := 0; i < count; i++ {
		day := day0.AddDate(0, 0, i)
		_, _ = fmt.Fprintf(&clause, "  PARTITION %s VALUES LESS THAN ('%s')", partitionName(day), partitionUpperBound(day))
		if i < count-1 {
			clause.WriteString(",\n")
		} else {
			clause.WriteString("\n")
		}
	}
	clause.WriteString(")")
	return clause.String()
}

// existingPartitionDays 读取 information_schema，返回某表当前已存在的分区
// 覆盖的日期集合（按分区名 pYYYYMMDD 解析，忽略解析失败的分区——理论上
// 不会出现，因为分区只由本包按同一命名规则创建）。
func existingPartitionDays(ctx context.Context, db *gorm.DB, table string) (map[string]bool, error) {
	var names []string
	err := db.WithContext(ctx).Raw(
		`SELECT PARTITION_NAME FROM information_schema.PARTITIONS
		 WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL`,
		table,
	).Scan(&names).Error
	if err != nil {
		return nil, fmt.Errorf("repo: 查询 %s 现有分区失败: %w", table, err)
	}
	days := make(map[string]bool, len(names))
	for _, n := range names {
		days[n] = true
	}
	return days, nil
}

// EnsureFuturePartitions 为分区表预创建从 now 起共 lookaheadDays 天的分区
// （含 now 当天），已存在的天数跳过，满足「可重复执行」。
// 分区上界也可能已经覆盖未使用相同名字的日期（时区日期差、时钟回退或
// 既有宽范围）；RANGE只能追加更大上界，不能按缺少名字反向追加过去的范围。
//
// now 由调用方（控制层的每日后台任务）传入，保证本函数在测试中可注入
// 固定时间。
func EnsureFuturePartitions(ctx context.Context, db *gorm.DB, table string, now time.Time, lookaheadDays int) error {
	if err := validateSQLIdentifier(table); err != nil {
		return err
	}
	if lookaheadDays < 1 {
		lookaheadDays = DefaultPartitionLookahead
	}
	existing, err := existingPartitionDays(ctx, db, table)
	if err != nil {
		return err
	}
	var last []string
	if err := db.WithContext(ctx).Raw(`SELECT PARTITION_DESCRIPTION FROM information_schema.PARTITIONS
		WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION DESC LIMIT 1`, table).Scan(&last).Error; err != nil {
		return fmt.Errorf("repo: 查询 %s 分区覆盖上界失败: %w", table, err)
	}
	upper := ""
	if len(last) != 0 {
		if last[0] == "MAXVALUE" {
			return nil
		}
		upper = strings.Trim(last[0], "'")
		if _, err := time.Parse("2006-01-02 15:04:05", upper); err != nil {
			return fmt.Errorf("repo: %s 分区上界不是受支持的日期范围", table)
		}
	}

	day0 := truncateToDay(now)
	for i := 0; i < lookaheadDays; i++ {
		day := day0.AddDate(0, 0, i)
		name := partitionName(day)
		boundary := partitionUpperBound(day)
		if existing[name] || boundary <= upper {
			continue
		}
		stmt := fmt.Sprintf(
			"ALTER TABLE %s ADD PARTITION (PARTITION %s VALUES LESS THAN ('%s'))",
			table, name, boundary,
		)
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("repo: 为 %s 添加分区 %s 失败: %w", table, name, err)
		}
		upper = boundary
	}
	return nil
}

// DropExpiredPartitions 删除分区上界早于 now-retention 的分区（`DROP
// PARTITION`，避免大事务删除）。逐个分区单独执行 ALTER，任一
// 分区失败不影响已经成功删除的分区。
func DropExpiredPartitions(ctx context.Context, db *gorm.DB, table string, now time.Time, retention time.Duration) error {
	if err := validateSQLIdentifier(table); err != nil {
		return err
	}
	if retention <= 0 {
		return fmt.Errorf("repo: 删除 %s 过期分区失败: retention 必须为正数，实际 %s", table, retention)
	}

	existing, err := existingPartitionDays(ctx, db, table)
	if err != nil {
		return err
	}

	cutoff := truncateToDay(now.Add(-retention))

	names := make([]string, 0, len(existing))
	for name := range existing {
		names = append(names, name)
	}
	sort.Strings(names) // 按分区名（即日期）升序，从最旧的开始删

	type candidate struct {
		name string
		day  time.Time
	}
	expired := make([]candidate, 0, len(names))
	for _, name := range names {
		day, err := time.ParseInLocation("20060102", name[1:], now.Location())
		if err != nil {
			return fmt.Errorf("repo: 无法解析 %s 的分区名 %q: %w", table, name, err)
		}
		// 分区 pYYYYMMDD 覆盖 [day, day+1) 的数据，只有其整体落在保留期之外
		// （即该分区最后一刻也早于 cutoff）才视为过期候选。
		if day.Before(cutoff) {
			expired = append(expired, candidate{name: name, day: day})
		}
	}

	// MySQL 不允许用 DROP PARTITION 删空最后一个分区（会报错 "Cannot
	// remove all partitions, use DROP TABLE instead"）。正常运维顺序是
	// 「先 EnsureFuturePartitions 再 DropExpiredPartitions」，届时必然还有
	// 未过期的当天 / 未来分区留存；但为了让本函数在单独调用、或维护任务
	// 长时间未运行、一次性追平的极端情况下也不出错，这里始终保留最新的
	// 一个分区，即使它同样早于 cutoff。
	if len(expired) == len(names) && len(names) > 0 {
		expired = expired[:len(expired)-1]
	}

	for _, c := range expired {
		stmt := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", table, c.name)
		if err := db.WithContext(ctx).Exec(stmt).Error; err != nil {
			return fmt.Errorf("repo: 删除 %s 的过期分区 %s 失败: %w", table, c.name, err)
		}
	}
	return nil
}

// DeleteInBatches 按批删除满足 whereSQL 条件的行。whereSQL 是不含 WHERE
// 关键字、不含 LIMIT 子句的条件表达式，whereArgs 是其中占位符对应的参数；
// batchSize/interval 传 <=0 时分别退回
// DefaultBatchDeleteSize/DefaultBatchDeleteInterval。
//
// whereSQL 不做同样的校验：它本身就是一段 SQL 条件表达式（可以含
// 括号、AND/OR、比较运算符等），不是单个标识符，没有一个通用规则能在
// 保留合法用法的同时挡住恶意拼接；它的安全性只能靠调用方约束——本包内
// 所有调用方传入的都是 Go 源码里的字符串字面量常量（不含任何外部输入
// 拼接），whereArgs 才是承载动态值的地方。这一点目前仍是文档约定，不是
// 运行期强制，留给未来如果出现「whereSQL 由非常量拼接」的调用方时再处理。
//
// ctx 被取消时立即停止并返回已删除的行数与 ctx.Err()，不吞掉取消信号——
// 长期运行的清理任务可能跨越多个批次，调用方需要能够中途叫停。
// 返回值是实际删除的总行数。
func DeleteInBatches(ctx context.Context, db *gorm.DB, table, whereSQL string, whereArgs []any, batchSize int, interval time.Duration) (int64, error) {
	if err := validateSQLIdentifier(table); err != nil {
		return 0, err
	}
	if batchSize <= 0 {
		batchSize = DefaultBatchDeleteSize
	}
	if interval <= 0 {
		interval = DefaultBatchDeleteInterval
	}

	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s LIMIT ?", table, whereSQL)
	args := make([]any, 0, len(whereArgs)+1)
	args = append(args, whereArgs...)
	args = append(args, batchSize)

	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		res := db.WithContext(ctx).Exec(stmt, args...)
		if res.Error != nil {
			return total, fmt.Errorf("repo: 按批删除 %s 失败: %w", table, res.Error)
		}
		total += res.RowsAffected
		if res.RowsAffected < int64(batchSize) {
			return total, nil
		}

		select {
		case <-ctx.Done():
			return total, ctx.Err()
		case <-time.After(interval):
		}
	}
}
