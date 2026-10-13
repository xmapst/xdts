package mysqlout

import (
	"errors"
	"strings"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/rule"
	"github.com/xmapst/xdts/internal/schemamig"
)

// ⚠️ 本文件是部分还原的，不是原件。
//
// s = s[:m.end()] + '...' if 'import (' not in s else s
//
// Python 的条件表达式绑定整个右侧，于是条件成立时整个文件被替换成"包声明 + import
// 块"，正文全部丢失。本仓零提交、无 HEAD，`.temp/` 的备份不含本文件，会话记录里也
// 没有本文件的完整读取 ⇒ 原始注释无法还原。
//
// 可确证的事实（从代码本身读出，不是从记忆）：New(ctx, Options) 返回 *Writer，
// 本类型是构造 Writer 的运行参数；全部字段由调用方显式注入，本包不读配置文件、
// 不依赖 controller 包。
type Options struct {
	WritePolicy *iproto.WritePolicy
	// DDLPolicy and mappings are copied during New and owned by this writer.
	DDLPolicy             *iproto.DDLPolicy
	ConflictPolicy        *iproto.ConflictPolicy
	Mapping               schemamig.Mapping
	SchemaOptions         schemamig.Options
	Transforms            []rule.TransformRule
	ShadowTablePolicy     *iproto.ShadowTablePolicy
	DDLSourceDependencies bool

	PipelineID         string
	ExpectedServerUUID string
	DatabaseRoute      iproto.DatabaseLineageRoute

	Host     string
	Port     uint16
	User     string
	Password string

	// SQLMode 对应 NODE-OUT-01"sql_mode（可配，默认与源端兼容）"。为空时本包
	// 不执行 SET SESSION sql_mode，沿用目标连接账号自身的默认值——"默认与源端
	// 兼容"这个具体值应该由调用方（预检查/Manager 下发的 PipelineSpec）算出后
	// 传进来，本包不猜测源端 sql_mode 是什么。
	SQLMode string

	// ForeignKeyChecks 对应 NODE-OUT-01"foreign_key_checks（可配，默认 0）"。
	// nil 时按需求默认值 0（关闭）；显式给 true 才会设为 1。
	ForeignKeyChecks *bool

	// 冲突回调**不在这里**：它由 Writer.SetConflictSink 设置，见该方法文档。
	// 理由是装配顺序——internal/node/controller/loader.go 必须先 New 出 Writer 才能构造 amqp_in
	// 模块（amqpin.Options.Applier 就是这个 Writer），而回调的接收者正是那个模块，
	// 构造 Options 的时刻它还不存在。**同一件事不给两种设法**（红线"契约改全"的
	// 同一形状：两处入口，改一处另一处不报错）。
}

// validate checks connection and session options before opening the target.
// It does not infer endpoint identity or change pipeline policy.
func (o Options) validate() error {
	var errs []error
	if err := o.DatabaseRoute.Validate(); err != nil {
		errs = append(errs, err)
	}
	if o.DatabaseRoute.Persistent && o.ExpectedServerUUID != o.DatabaseRoute.TargetServerUUID {
		errs = append(errs, errors.New("mysqlout: persistent lineage target identity disagreement"))
	}
	if strings.TrimSpace(o.Host) == "" {
		errs = append(errs, errors.New("mysqlout: Host 不能为空"))
	}

	if o.Port == 0 {
		errs = append(errs, errors.New("mysqlout: Port 不能为 0"))
	}
	if strings.TrimSpace(o.User) == "" {
		errs = append(errs, errors.New("mysqlout: User 不能为空"))
	}
	return errors.Join(errs...)
}

func (o Options) foreignKeyChecksValue() int {
	if o.ForeignKeyChecks != nil && *o.ForeignKeyChecks {
		return 1
	}
	return 0
}
