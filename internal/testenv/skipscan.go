package testenv

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SkipCallSite 是一处对 t.Skip/t.Skipf/t.SkipNow 的**调用**。
type SkipCallSite struct {
	File string // 目录内的文件名，不含路径
	Line int
	Text string // 该行去掉首尾空白后的源码
}

// String 便于直接放进错误信息。
func (s SkipCallSite) String() string { return fmt.Sprintf("%s:%d: %s", s.File, s.Line, s.Text) }

// ScanSkipCallSites 扫描 dir 下（不递归）全部 .go 文件里对
// t.Skip/t.Skipf/t.SkipNow 的调用，按文件名、行号排序返回。
//
// 这条断言按包生效，所以 B / C 两族收敛时每个被改的包都要带一条自己
// 的。把扫描实现放在这里导出，是为了让那些包各写三行断言，而不是各自抄
// 一份六十行的 AST 代码——抄出来的必然互相漂移。
//
// 注释里提到 t.Skip 的地方很多（本包与 testsupport 的文档注释都写了"默认
// 跳过"，并且举了 `t.Skipf("跳过：...")` 这样的例子）。正则扫会把它们全部
// 算成调用点，于是这条断言会从第一天起就是红的、然后被人加白名单绕过去，
// 而一条从来没绿过的断言等于没有断言。AST 天然只看代码。
//
// 匹配的是"方法名为 Skip/Skipf/SkipNow 的选择器调用"，不要求接收者叫 t
// ——否则把变量改名成 tb 就绕过了检查，这条断言同样白写。代价是可能匹配到
// 某个恰好也叫 Skip 的无关方法；真出现时在调用方的白名单里说明理由即可，
// 比漏检安全。
func ScanSkipCallSites(dir string) ([]SkipCallSite, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("testenv: 读目录 %s 失败: %w", dir, err)
	}

	fset := token.NewFileSet()
	var hits []SkipCallSite
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		f, err := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if err != nil {
			return nil, fmt.Errorf("testenv: 解析 %s 失败: %w", path, err)
		}
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("testenv: 读 %s 失败: %w", path, err)
		}
		lines := strings.Split(string(src), "\n")

		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			switch sel.Sel.Name {
			case "Skip", "Skipf", "SkipNow":
			default:
				return true
			}
			pos := fset.Position(sel.Sel.Pos())
			text := ""
			if pos.Line-1 < len(lines) {
				text = strings.TrimSpace(lines[pos.Line-1])
			}
			hits = append(hits, SkipCallSite{File: e.Name(), Line: pos.Line, Text: text})
			return true
		})
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].File != hits[j].File {
			return hits[i].File < hits[j].File
		}
		return hits[i].Line < hits[j].Line
	})
	return hits, nil
}

// RequireNoDirectSkips 断言 dir 下没有绕过 Requirement.Unavailable 的裸
// t.Skip 调用。
//
// 把标准说明放在这里而不是让每个包自己写，是为了让"为什么不许裸 Skip"这句
// 解释只有一份：抄出去的解释会随包漂移，而漂移之后每个包的判据实际就不一样了。
func RequireNoDirectSkips(t IReporter, dir string, allowed ...string) {
	t.Helper()
	sites, err := ScanSkipCallSites(dir)
	if err != nil {
		t.Fatalf("%v", err)
		return
	}
	allow := make(map[string]bool, len(allowed))
	for _, a := range allowed {
		allow[a] = true
	}
	for _, s := range sites {
		key := fmt.Sprintf("%s:%d", s.File, s.Line)
		if allow[key] {
			delete(allow, key)
			continue
		}
		t.Errorf("%s 直接调用了 t.Skip，绕过了 testenv 的 REQUIRED 开关。\n"+
			"本包里\"外部服务不可用\"必须走对应 Requirement 的 Unavailable 方法，"+
			"否则声明了 REQUIRED 的环境里这一处仍然会静默跳过——而 skip 是绿的。\n"+
			"如果这一处跳过的原因**不是**缺了一个可以启动的服务（并发条件、运行时"+
			"时序、窗口内没等到事件等，见 testenv 包文档 D 类），把 %q 加进本断言的"+
			"白名单并写明理由。", s, key)
	}
	for key := range allow {
		t.Errorf("testenv: allowed skip %q no longer exists; update its explicit exemption", key)
	}
}
