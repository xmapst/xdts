// internal/schemamig/parse_antlr.go 提供存储过程 / 函数 / 触发器 / 事件改写共用的
// bytebase/parser/mysql（ANTLR4）封装（ADR-08 分层解析器的存储程序分支）。
package schemamig

import (
	"fmt"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
)

type routineErrorListener struct {
	*antlr.DefaultErrorListener
	errs []string
}

func newRoutineErrorListener() *routineErrorListener {
	return &routineErrorListener{DefaultErrorListener: &antlr.DefaultErrorListener{}}
}

func (l *routineErrorListener) SyntaxError(_ antlr.Recognizer, _ any, line, column int, msg string, _ antlr.RecognitionException) {
	l.errs = append(l.errs, fmt.Sprintf("%d:%d %s", line, column, msg))
}

// parsedRoutine 打包一次成功解析的存储程序：语法树、token 流（改写时
// TokenStreamRewriter 要用）。
type parsedRoutine struct {
	tree   mysql.IScriptContext
	stream *antlr.CommonTokenStream
}

// 解析失败（词法或语法错误）时返回非 nil error，调用方必须整体阻断——不
// 存在"部分解析成功，凑合改写"这种路径。
func parseRoutine(ddl string) (*parsedRoutine, error) {
	text := strings.TrimRight(ddl, " \t\r\n;") + "\n;"
	input := antlr.NewInputStream(text)
	lexer := mysql.NewMySQLLexer(input)
	stream := antlr.NewCommonTokenStream(lexer, 0)
	p := mysql.NewMySQLParser(stream)
	p.BuildParseTrees = true

	lexErrs := newRoutineErrorListener()
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(lexErrs)

	parseErrs := newRoutineErrorListener()
	p.RemoveErrorListeners()
	p.AddErrorListener(parseErrs)

	tree := p.Script()

	if len(lexErrs.errs) > 0 || len(parseErrs.errs) > 0 {
		return nil, fmt.Errorf("解析失败: 词法错误=%v 语法错误=%v", lexErrs.errs, parseErrs.errs)
	}
	return &parsedRoutine{tree: tree, stream: stream}, nil
}
