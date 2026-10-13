// Package redact 提供从自由文本里遮盖 DSN/URL 风格凭据的工具函数。
//
// 提到这个独立包、而不是让其中一个包从另一个包导出：http 的生产依赖已经
// 包含 dispatch（pipelineHandler 持有 dispatch.IPipelineDispatcher），
// 反过来从 dispatch 导出给 http 用技术上不会成环，但会让 dispatch 的
// 公开 API 里混进一个与"pipeline 下发编排"无关的文本工具函数——两个消费
// 方都在同一个人的地盘（internal/manager/http、internal/manager/dispatch）
// 内，新开一个两者都不依赖、只做这一件事的叶子包是成本最低的选择，
// 不需要因此改动包边界的归属。
package redact

import "regexp"

// credentialInURLPattern/credentialInMySQLDSNPattern 匹配"URL 风格
// scheme://user:pass@..."与 go-sql-driver/mysql 风格
// "user:pass@tcp(...)"/"user:pass@unix(...)"这两种具体、有明确形状的
// 凭据外泄方式——与 internal/manager/config/config.go 的
// redactDSN/redactAMQPURL 是同一类风险的同一种应对（那两个函数要求整个
// 字符串就是一份能被 mysql.ParseDSN/url.Parse 解析的 DSN；本包的输入是
// 一段自由文本，凭据可能只是其中一个子串，因此换成正则匹配，不直接复用
// 那两个函数）。
var (
	credentialInURLPattern       = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*://)[^\s/@]+:[^\s/@]*@`)
	credentialInMySQLDSNPattern  = regexp.MustCompile(`[^\s/@:]+:[^\s/@]*@(tcp|unix)\(`)
	redactedURLCredential        = "${1}***:***@"
	redactedMySQLDSNCredentialFn = func(scheme string) string { return "***:***@" + scheme + "(" }
)

// PotentialCredentials 对 s 做上面两个正则描述的脱敏替换，返回替换后的
// 文本。s 本身不是一份完整的 DSN/URL，可能只是一段包含它的自由文本
// （比如一条错误信息），因此不能用 mysql.ParseDSN/url.Parse 整串解析。
func PotentialCredentials(s string) string {
	s = credentialInURLPattern.ReplaceAllString(s, redactedURLCredential)
	s = credentialInMySQLDSNPattern.ReplaceAllStringFunc(s, func(match string) string {
		scheme := "tcp"
		if len(match) >= 5 && match[len(match)-5:len(match)-1] == "unix" {
			scheme = "unix"
		}
		return redactedMySQLDSNCredentialFn(scheme)
	})
	return s
}
