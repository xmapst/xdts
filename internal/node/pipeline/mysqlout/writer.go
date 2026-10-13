package mysqlout

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	_ "github.com/go-sql-driver/mysql" // 注册 "mysql" driver，本包用 database/sql 直接写目标库
	"github.com/xmapst/xdts/internal/dblineage"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
	"github.com/xmapst/xdts/internal/schemamig"
	"google.golang.org/protobuf/proto"
)

// connectTimeout 是建立目标连接（含首次 Ping 与会话初始化）的整体超时。目标库
// 一般是本地/同机房实例，10 秒足够覆盖正常抖动，同时避免网络异常时 New 无限期
// 挂起（呼应 position 包 connResetTimeout 的同一考量，量级放大是因为这里除了
// 一次 Ping 还要跑完整的会话初始化语句序列）。
const connectTimeout = 10 * time.Second

// Writer持有主连接及按WritePolicy准备的独立工作连接，实现IBatchApplier。
// 调用方必须保证同一根Writer的ApplyBatch严格串行；调度器逐个交接工作连接，
// 单个工作单元始终只由一个goroutine拥有，本包不对业务状态加锁。
type Writer struct {
	opts Options

	db     *sql.DB
	conn   *sql.Conn
	broken bool
	closed bool

	// conflictSink 见 SetConflictSink。只在装配期（消费开始前）写一次，之后只读
	// ⇒ 不加锁（"只有确实被多个 goroutine 访问的可变数据才加锁"）。
	conflictSink       conflictSink
	conflictMasks      []conflictMask
	catchingUp         bool
	restoreForeignKeys bool
	transforms         *rule.TransformPlan
	workers            []*Writer // additional exclusive connections; primary is worker 0
	recovery           *tableRecovery
}

// 装配顺序决定的：internal/node/controller/loader.go 必须先 mysqlout.New 出 Writer，才能把它
// 填进 amqpin.Options.Applier 去构造 amqp_in 模块——而回调的接收者正是那个模块，
// 构造 Options 的时刻它还不存在。给 Options 也留一个同名字段等于同一件事两种设法，
// 改一处另一处不报错（红线"契约改全"）。
//
// 必须在该 Writer 开始被用于写入之前调用，也就是在 internal/node/controller/loader.go 把 amqp_in 模块交给
// xhive.AddDynamicModules 之前。那一刻消费者还不存在，本字段是单 goroutine 写；
// 之后它只被 handler goroutine 读。⇒ 不要在运行期改它。
//
// fn 为 nil 时检测照旧发生、产物被丢弃（行为与本功能落地前一致）。
//
// 交出去的是 *iproto.ConflictEventDetail，不是 Event：拼信封（EventId/Kind/Level/TsMs）
// 并 Cast 给 reporter 是 amqp_in 的事，与 ack/nack 完全留在 amqp_in 侧是同一条分工
// 本包是模块持有的写入器，不是 xhive
// 模块，没有 Cast。
func (w *Writer) SetConflictSink(fn func(*iproto.ConflictEventDetail)) {
	w.conflictSink = fn
}

// New 建立到目标库的连接并完成一次性会话初始化（NODE-OUT-01/09/12）。
func New(ctx context.Context, opts Options) (*Writer, error) {
	if err := iproto.ValidateWritePolicy(opts.WritePolicy); err != nil {
		return nil, err
	}
	if opts.WritePolicy != nil {
		opts.WritePolicy = proto.Clone(opts.WritePolicy).(*iproto.WritePolicy)
	}
	if err := opts.validate(); err != nil {
		return nil, err
	}
	if err := validateDDLPolicy(opts.DDLPolicy); err != nil {
		return nil, err
	}
	if err := iproto.ValidateConflictPolicy(opts.ConflictPolicy); err != nil {
		return nil, err
	}
	if opts.ConflictPolicy != nil {
		opts.ConflictPolicy = proto.Clone(opts.ConflictPolicy).(*iproto.ConflictPolicy)
	}
	if opts.DDLPolicy != nil {
		opts.DDLPolicy = proto.Clone(opts.DDLPolicy).(*iproto.DDLPolicy)
	}
	if opts.ShadowTablePolicy != nil {
		opts.ShadowTablePolicy = iproto.CloneShadowTablePolicy(opts.ShadowTablePolicy)
		if err := iproto.ValidateShadowTablePolicy(opts.ShadowTablePolicy); err != nil {
			return nil, err
		}
	}
	if opts.ForeignKeyChecks != nil {
		checks := *opts.ForeignKeyChecks
		opts.ForeignKeyChecks = &checks
	}
	opts.Mapping.Schemas = maps.Clone(opts.Mapping.Schemas)
	opts.Mapping.Tables = maps.Clone(opts.Mapping.Tables)
	columns := make(map[schemamig.SchemaTable]map[string]string, len(opts.Mapping.Columns))
	for table, cols := range opts.Mapping.Columns {
		columns[table] = maps.Clone(cols)
	}
	opts.Mapping.Columns = columns
	drops := make(map[schemamig.SchemaTable][]string, len(opts.Mapping.DropColumns))
	for table, cols := range opts.Mapping.DropColumns {
		drops[table] = slices.Clone(cols)
	}
	opts.Mapping.DropColumns = drops
	opts.SchemaOptions.StripTableOptions = slices.Clone(opts.SchemaOptions.StripTableOptions)
	opts.Transforms = cloneTransformRules(opts.Transforms)
	transforms, err := compileWriterTransforms(opts)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?multiStatements=true&interpolateParams=true", opts.User, opts.Password, opts.Host, opts.Port)
	// DSN 故意不带默认 database：RowChange/DDL 都自带 schema 字段，本包生成的
	// 每条语句都用 `schema`.`table` 全限定，不依赖连接的默认库。
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("mysqlout: 打开目标库连接失败: %w", err)
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("mysqlout: 获取目标库物理连接失败: %w", err)
	}
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, fmt.Errorf("mysqlout: 目标库不可达: %w", err)
	}

	w := &Writer{opts: opts, db: db, conn: conn, transforms: transforms}
	w.compileConflictMasks()
	if err := w.initSession(ctx, conn); err != nil {
		_ = conn.Close()
		_ = db.Close()
		return nil, err
	}
	for i := 1; i < iproto.ReplayDegree(opts.WritePolicy); i++ {
		childOpts := opts
		childOpts.WritePolicy = proto.Clone(opts.WritePolicy).(*iproto.WritePolicy)
		childOpts.WritePolicy.ParallelReplayDegree = 1
		worker, err := New(ctx, childOpts)
		if err != nil {
			_ = w.Close()
			return nil, err
		}
		w.workers = append(w.workers, worker)
	}
	return w, nil
}

// Close 释放目标库连接。
func (w *Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	var workerErr error
	for _, worker := range w.workers {
		workerErr = errors.Join(workerErr, worker.Close())
	}
	w.workers = nil
	var connErr error
	if w.conn != nil {
		connErr = w.conn.Close()
		w.conn = nil
	}
	var dbErr error
	if w.db != nil {
		dbErr = w.db.Close()
	}
	if connErr != nil && !errors.Is(connErr, sql.ErrConnDone) {
		return fmt.Errorf("mysqlout: 关闭连接失败: %w", connErr)
	}
	if dbErr != nil {
		return fmt.Errorf("mysqlout: 关闭连接池失败: %w", dbErr)
	}
	return workerErr
}

// initSession 在一条新连接上执行一次性会话初始化。必须在该连接执行任何
// ApplyBatch 相关语句之前调用且只调用一次（写会话初始化模板：
// "SET @xdts_apply=1 一次"），New 和断线恢复对每条新物理连接各调用一次。
func (w *Writer) initSession(ctx context.Context, conn *sql.Conn) error {
	if w.opts.DatabaseRoute.Persistent {
		if err := dblineage.New(conn).Validate(ctx); err != nil {
			return err
		}
	}
	if w.opts.ExpectedServerUUID != "" {
		var actual string
		if err := conn.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&actual); err != nil || actual != w.opts.ExpectedServerUUID {
			return fmt.Errorf("mysqlout: target database identity changed; probe and refresh configuration")
		}
	}
	// NODE-OUT-01：time_zone='+00:00'（与源端解码 binlog 时使用的约定一致，
	// proto/data.proto ColumnValue 注释"TIMESTAMP 在源端已按 UTC 规整"）、utf8mb4。
	stmts := []struct {
		query string
		args  []any
	}{
		{"SET SESSION time_zone = ?", []any{"+00:00"}},
		{"SET NAMES utf8mb4", nil},
		{"SET SESSION foreign_key_checks = ?", []any{w.opts.foreignKeyChecksValue()}},
		// NODE-OUT-12：所有同步写入会话设置 @xdts_apply=1，供触发器守卫判断。
		{"SET @xdts_apply = 1", nil},
		{"SET @xdts_pipeline_id = ?", []any{w.opts.PipelineID}},
	}
	if w.opts.SQLMode != "" {
		stmts = append(stmts, struct {
			query string
			args  []any
		}{"SET SESSION sql_mode = ?", []any{w.opts.SQLMode}})
	}

	for _, s := range stmts {
		if _, err := conn.ExecContext(ctx, s.query, s.args...); err != nil {
			return fmt.Errorf("mysqlout: 会话初始化失败(%s): %w", s.query, err)
		}
	}

	var sqlLogBin int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.sql_log_bin").Scan(&sqlLogBin); err != nil {
		return fmt.Errorf("mysqlout: 读取 sql_log_bin 失败: %w", err)
	}
	if sqlLogBin != 1 {
		return fmt.Errorf("mysqlout: 写入会话的 sql_log_bin=%d，不等于 1（NODE-OUT-09 禁止关闭；"+
			"拒绝在这条会话上写入，请检查目标库连接账号或初始化脚本是否意外关闭了 binlog 记录）",
			sqlLogBin)
	}

	return nil
}
