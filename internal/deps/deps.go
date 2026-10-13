// Package deps anchors predeclared runtime dependencies for module maintenance.
// These imports do not assemble services. Avoid go-mysql/canal: its parser-driver
// contract is incompatible with the pinned TiDB parser.
package deps

import (
	// Node 端：Actor 框架、本地状态库、日志、文件锁
	_ "github.com/gofrs/flock"
	_ "github.com/xmapst/xdoc"
	_ "github.com/xmapst/xhive"
	_ "github.com/xmapst/xslog"

	// 源端 binlog 读取与位点。
	_ "github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-mysql-org/go-mysql/replication"

	// 结构迁移使用 TiDB parser 处理表与视图，使用 bytebase/parser/mysql 处理存储程序。
	_ "github.com/bytebase/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/parser"
	_ "github.com/pingcap/tidb/pkg/parser/ast"

	// Manager 认证：LDAP / OIDC
	_ "github.com/coreos/go-oidc/v3/oidc"
	_ "github.com/go-ldap/ldap/v3"
	_ "golang.org/x/oauth2"

	// 规则表达式（行过滤 / 清洗转换，NODE-TF-*）
	_ "github.com/expr-lang/expr"

	// 日志轮转
	_ "github.com/DeRuina/timberjack"
	_ "gopkg.in/natefinch/lumberjack.v2"

	// 协议和数据面使用的 UUID 实现。
	_ "github.com/google/uuid"
)
