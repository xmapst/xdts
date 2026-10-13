// Package testenv declares external test dependencies.
// Optional unavailable services skip; explicitly required services fail. Invalid
// requirement flags are errors. Common dependency gates and AST skip-site checks
// ensure required CI coverage cannot silently shrink.
package testenv

import (
	"fmt"
	"os"
	"strings"
)

// ISkipFataler is the test capability subset required by environment gates.
// Unlike testing.TB, it can be implemented by branch-verifying test doubles.
type ISkipFataler interface {
	Helper()
	Skipf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// IReporter 是 RequireNoDirectSkips 用到的能力子集（它要报告多处问题，
// 用 Errorf 而不是一处就终止）。
type IReporter interface {
	Helper()
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// requiredTrueValues 是被认作「是」的取值。未设置或空串一律是「否」。
//
// **取值不在两个集合里时不是「否」，是错误**——见 Requirement.Check。
var requiredTrueValues = map[string]bool{
	"1": true, "true": true, "yes": true, "on": true,
}

// requiredFalseValues 是显式的「否」。允许显式关闭，是为了让 CI 能在某个 job
// 里明确写出"这一个 job 不要求该服务"，而不是靠 unset 表达。
var requiredFalseValues = map[string]bool{
	"0": true, "false": true, "no": true, "off": true, "": true,
}

// Requirement 是「本环境必须提供某个外部服务」这件事本身。零值不可用，
// 用 New 构造。
type Requirement struct {
	// envVar names the published requirement flag; update all consumers if adding a contract.
	envVar string
	// service 是人类可读的服务名，出现在跳过/失败信息里。
	service string
	// endpointEnvVar 是**覆盖连接目标用的环境变量名**，可以为空。
	//
	// 这里刻意只存**变量名**，不存默认连接串：失败信息里打一个形如
	// `<用户>:<口令>@tcp(<地址>)/<库>` 的默认 DSN 会把口令字面量写进日志，撞
	// 「口令、令牌、密钥不进日志」。本地开发默认值的实害很低，
	// 但要改的理由不是这一处的实害——是这个写法会被抄：本包是三族都会当模板
	// 读的地方，把三份含口令的默认值集中进来，才是真正的问题。
	//
	// 实际连接目标不会因此丢失：它本来就出现在调用方传进 Unavailable 的
	// reason 里（例如"无法连接管理库测试实例: dial tcp 127.0.0.1:59999:
	// connect: connection refused"）。
	endpointEnvVar string
}

// New 构造一个 Requirement。endpointEnvVar 可以为空（该服务没有单独的连接
// 目标覆盖变量时）。
func New(envVar, service, endpointEnvVar string) Requirement {
	return Requirement{envVar: envVar, service: service, endpointEnvVar: endpointEnvVar}
}

// EnvVar 返回声明该要求的环境变量名，供调用方在自己的文档与错误信息里引用，
// 不必再写一遍字面量。
func (r Requirement) EnvVar() string { return r.envVar }

// Service 返回人类可读的服务名。
func (r Requirement) Service() string { return r.service }

// parseRequired 解析环境变量取值。**无法识别的取值返回错误，不是返回 false**
// ——理由见 Check。
//
// 做成不读环境变量的纯函数，是为了让它能在**没有任何外部服务**的环境里被
// 穷举测试：这套开关自己也需要一个会红的检查，否则它就是它要修的那个缺陷的
// 下一个实例。
func parseRequired(envVar, raw string) (bool, error) {
	v := strings.ToLower(strings.TrimSpace(raw))
	if requiredTrueValues[v] {
		return true, nil
	}
	if requiredFalseValues[v] {
		return false, nil
	}
	return false, fmt.Errorf("testenv: 环境变量 %s 取值 %q 无法识别。\n"+
		"合法取值：1/true/yes/on（要求必须有该服务，不可达即失败）、"+
		"0/false/no/off/空（默认，不可达则跳过）。\n"+
		"这里刻意不把无法识别的取值当成\"否\"：拼错一个字母就会静默关掉"+
		"\"必须有\"的要求，而关掉之后的绿色和真正通过的绿色一模一样",
		envVar, raw)
}

// Required 报告调用环境是否声明了「必须有该服务」。取值合法性由 Check 在入口
// 处保证；取值非法时这里返回 false，但那种取值已经在入口处 Fatal 过了，
// 走不到这里。
func (r Requirement) Required() bool {
	required, err := parseRequired(r.envVar, os.Getenv(r.envVar))
	return err == nil && required
}

// Check 校验环境变量取值本身合法，取值非法即 Fatal。
//
// **每个入口都要在最开头调用它，不是只在"服务不可用"那条路径上调用。**
// 这个区别是本方法存在的全部理由：如果只在不可用路径上解析，
// `XDTS_..._REQUIRED=ture`（拼错）在服务可达时不会有任何表现，CI 一路绿灯，
// 而那个 job 其实根本没有在要求该服务——**这正是"关掉的守护看起来和通过的
// 守护一样"在开关上的复现**。取值拼错必须立刻响，且与服务是否可达无关。
func (r Requirement) Check(t ISkipFataler) {
	t.Helper()
	if _, err := parseRequired(r.envVar, os.Getenv(r.envVar)); err != nil {
		t.Fatalf("%v", err)
	}
}

// Unavailable 是「该服务不可用」的唯一出口：默认 Skip，在声明了对应环境变量
// 的环境里 Fatal。
//
// reason 是不带"跳过："前缀的原因描述——前缀由本方法按当前模式加，因为在
// required 模式下它不是一次跳过。reason 里应当包含实际的连接目标与底层错误。
//
// 失败信息刻意写明这是环境缺失、不是被测代码的缺陷：这条失败会出现在一个
// "所有代码都是好的"仓库里，读到它的人第一反应不该是去读代码。
func (r Requirement) Unavailable(t ISkipFataler, reason string) {
	t.Helper()
	if !r.Required() {
		t.Skipf("跳过：%s", reason)
		return
	}
	endpointHint := ""
	if r.endpointEnvVar != "" {
		endpointHint = fmt.Sprintf("（连接目标可用 %s 覆盖；实际连接目标见上面的原因文本）", r.endpointEnvVar)
	}
	t.Fatalf("%s不可用：%s\n"+
		"\n"+
		"这是**环境缺失，不是被测代码的缺陷**——本环境通过 %s 声明了必须有可用的%s，"+
		"因此这里失败而不是跳过。请先确认它已启动%s，不要从这条失败去找代码问题。\n"+
		"\n"+
		"不设置 %s 时这里会跳过，本地没有该服务的开发者不受影响。",
		r.service, reason, r.envVar, r.service, endpointHint, r.envVar)
}

// ManagerMySQL 是 Manager 的管理库（独立部署的 MySQL 8，C-18）。
var ManagerMySQL = New(
	"XDTS_MGR_MYSQL_REQUIRED",
	"管理库",
	"XDTS_MGR_MYSQL_DSN",
)

// Broker 是消息 Broker（RabbitMQ，AMQP）。
//
// **不给 endpointEnvVar（留空），这是刻意的。** 本族的测试把 broker 地址写成
// 包内 const（`testBrokerURL` / `testAMQPBrokerURL`，共 5 处文件），**没有任何
// 环境变量覆盖**。生产配置里确实有一个 `XDTS_AMQP_URL`，但那些测试不读它——
// 把它填进来，失败信息就会告诉开发者"连接目标可用 XDTS_AMQP_URL 覆盖"，而设了
// 它什么都不会变。**一条假的排查指引比没有指引更糟**：它会让人先去怀疑自己设
// 错了，而不是去启动服务。真实连接目标由调用方传进 Unavailable 的 reason 携带。
//
// 将来若某族把 const 改成可被环境变量覆盖，在这里补上那个变量名即可——
// Unavailable 对空值已经是省略提示，不需要改机制。
var Broker = New(
	"XDTS_AMQP_REQUIRED",
	"消息 Broker（AMQP）",
	"",
)

// BizMySQL 是被同步的业务库（源库 127.0.0.1:33072 / 目标库 127.0.0.1:33073），
// 不是 Manager 的管理库——后者是 ManagerMySQL，两者不可互换。
//
// 现有连接常量（`testMySQLDSN` = 源库、`testTargetDSN` = 目标库，共 5 处文件）
// 也都是硬编码 const，今天没有覆盖变量可填。
var BizMySQL = New(
	"XDTS_BIZ_MYSQL_REQUIRED",
	"业务库",
	"",
)

// declared 是本包声明过的全部 Requirement。
//
// 存在它的理由有两个，都不是"为了好看"：
//
//  1. **让"只给变量名、不给连接串"这条约束可被断言**（见
//     要求声明的凭据检查）。检查的是**数据**而不是
//     源码文本——对源码做子串扫描会被"解释这条约束的注释"本身触发，那种断言
//     会从第一天起就红、然后被人加白名单绕过去。
//  2. B / C 两族声明进来时自动进入同一套检查，不需要谁记得去加。
var declared = []Requirement{
	ManagerMySQL,
	Broker,
	BizMySQL,
}

// Declared 返回本包声明过的全部 Requirement 的副本。
func Declared() []Requirement {
	out := make([]Requirement, len(declared))
	copy(out, declared)
	return out
}
