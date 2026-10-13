// Package task 实现 Manager 侧不属于 internal/manager/reclaim（两步删除
// 的异步资源回收）的其它周期性后台维护任务，包括分区预创建、保留期
// 清理和巡检派发，均由 cmd/manager/main.go 拉起。
//
// 验证本包有几个 Loop 类型、构造函数各叫什么；不验证它们是否都被拉起，
// 也不验证是否有第三个 Loop 定义在别处（本包当前只有这两个非测试文件，
// 新增文件时这条命令要跟着扩）。
//
// 验证两项确实都被启动；不验证启动顺序、ctx 生命周期与关停是否有序。
package task

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"gorm.io/gorm"

	"github.com/xmapst/xdts/internal/manager/repo"
)

// DefaultPartitionCheckInterval 是分区预创建巡检的执行周期。
// 要求"每天预创建未来 7 天分区"；lookahead 有 7 天缓冲，24 小时一次的
// 巡检足够及时，不需要更高频率。
const DefaultPartitionCheckInterval = 24 * time.Hour

// PartitionTables 是需要预创建分区的表（按天 RANGE 分区的两张
// 表）。复用 repo 包已定义的表名常量，不在这里重复字面量。
var PartitionTables = []string{repo.TableLogs, repo.TablePipelineMetrics}

// Options 配置 PartitionLoop。
type Options struct {
	DB          *gorm.DB
	PartitionMu *sync.Mutex
	// Tables 为空时使用 PartitionTables。
	Tables []string
	// LookaheadDays <=0 时使用 repo.DefaultPartitionLookahead。
	LookaheadDays int
	// Interval <=0 时使用 DefaultPartitionCheckInterval。
	Interval time.Duration
}

// PartitionLoop 是分区预创建后台任务：周期性对 Tables 里的每张表调用
// repo.EnsureFuturePartitions，续建未来 LookaheadDays 天的分区。
type PartitionLoop struct {
	db            *gorm.DB
	partitionMu   *sync.Mutex
	tables        []string
	lookaheadDays int
	interval      time.Duration
}

// New 构造分区预创建循环。
func New(opts Options) *PartitionLoop {
	tables := opts.Tables
	if len(tables) == 0 {
		tables = PartitionTables
	}
	lookahead := opts.LookaheadDays
	if lookahead <= 0 {
		lookahead = repo.DefaultPartitionLookahead
	}
	interval := opts.Interval
	if interval <= 0 {
		interval = DefaultPartitionCheckInterval
	}
	return &PartitionLoop{db: opts.DB, partitionMu: opts.PartitionMu, tables: tables, lookaheadDays: lookahead, interval: interval}
}

// RunOnce 对每张表各跑一次分区预创建，用 time.Now() 作为基准（控制层读取
// 当前时间）。返回值见 RunOnceAt。
func (l *PartitionLoop) RunOnce(ctx context.Context) error {
	return l.RunOnceAt(ctx, time.Now())
}

// 每张表独立处理，一张表失败（Error 级别日志，因为这直接对应"该表未来会
// 开始报 1526、静默丢数据"这个后果，不是可以降级处理的普通错误）不影响
// 其它表继续尝试——两张表的分区状态互不相干，没有理由因为一张表的 DDL
// 失败（比如权限、锁等待）就让另一张表也保持未续建。
func (l *PartitionLoop) RunOnceAt(ctx context.Context, now time.Time) error {
	if l.partitionMu != nil {
		l.partitionMu.Lock()
		defer l.partitionMu.Unlock()
	}
	var errs []error
	for _, table := range l.tables {
		if err := repo.EnsureFuturePartitions(ctx, l.db, table, now, l.lookaheadDays); err != nil {
			// 后果见文件头注释：窗口耗尽后写入会报 MySQL 1526、数据静默丢失。
			slog.LogAttrs(ctx, slog.LevelError, "partition pre-create failed",
				slog.String("table", table), slog.Any("error", err))
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// 常驻循环内部的每一轮不处理 RunOnce 的返回值——RunOnceAt 内部已经对每张
// 表的失败记过结构化日志，常驻循环这里没有一个"启动流程"可以对失败做
// 反应（不像 cmd/manager/main.go 里那次同步调用，后面紧跟着 ingest 启动，值得把两件
// 事关联起来记一条日志），后续每一轮巡检本身就是对上一轮失败的自动重试，
// 不需要额外动作。
func (l *PartitionLoop) Run(ctx context.Context) {
	_ = l.RunOnce(ctx)
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = l.RunOnce(ctx)
		}
	}
}
