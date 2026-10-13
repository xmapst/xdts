package rule

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"regexp/syntax"
	"slices"
	"sort"
	"strings"
	"unicode"
)

// ErrOverlapComplexity means the configured language could not be proven within
// the validation budget. Callers must reject it rather than assume disjointness.
var ErrOverlapComplexity = errors.New("rule: object scope intersection exceeds validation budget")

// FindTableOverlap returns a schema/table selected by both source plans, including
// INCLUDE unions and EXCLUDE precedence. It considers future objects as well as
// present ones. MySQL schema/table identifiers are nonempty BMP strings of at most
// 64 characters, without NUL or a trailing space.
func FindTableOverlap(a, b *Plan) (TableRef, bool, error) {
	if a == nil || b == nil || a.side != SideSource || b.side != SideSource {
		return TableRef{}, false, errors.New("rule: overlap requires two source plans")
	}
	if len(a.objects)+len(b.objects) > 64 {
		return TableRef{}, false, ErrOverlapComplexity
	}
	objects := append(append([]compiledObjectRule(nil), a.objects...), b.objects...)
	schemas, err := scopeAcceptVectors(objects, true)
	if err != nil {
		return TableRef{}, false, err
	}
	tables, err := scopeAcceptVectors(objects, false)
	if err != nil {
		return TableRef{}, false, err
	}
	checks := 0
	for _, s := range schemas {
		for _, t := range tables {
			checks++
			if checks > 1_000_000 {
				return TableRef{}, false, ErrOverlapComplexity
			}
			if scopeSelected(a.objects, s.matches, t.matches, 0) && scopeSelected(b.objects, s.matches, t.matches, len(a.objects)) {
				return TableRef{Schema: s.witness, Table: t.witness}, true, nil
			}
		}
	}
	return TableRef{}, false, nil
}

func scopeSelected(objects []compiledObjectRule, schemas, tables []bool, offset int) bool {
	hasInclude, included := false, false
	for i, obj := range objects {
		if obj.action == FilterActionInclude {
			hasInclude = true
		}
		if !schemas[offset+i] || !tables[offset+i] {
			continue
		}
		if obj.action == FilterActionExclude {
			return false
		}
		included = true
	}
	return !hasInclude || included
}

type scopeVector struct {
	matches []bool
	witness string
}
type scopeAutomaton struct {
	programs []*syntax.Prog
	alphabet []rune
}
type scopeState struct {
	pcs      [][]uint32
	previous rune
	witness  string
	length   int
}

func scopeAcceptVectors(objects []compiledObjectRule, schema bool) ([]scopeVector, error) {
	machine := scopeAutomaton{}
	programSize := 0
	for _, obj := range objects {
		m := obj.table
		if schema {
			m = obj.schema
		}
		expression := "(?s:.*)"
		switch match := m.(type) {
		case nil:
		case exactMatcher:
			expression = regexp.QuoteMeta(match.pattern)
			if !match.caseSensitive {
				expression = "(?i:" + expression + ")"
			}
		case *regexp.Regexp:
			expression = match.String()
		default:
			return nil, fmt.Errorf("rule: unsupported scope matcher %T", m)
		}
		parsed, err := syntax.Parse(expression, syntax.Perl)
		if err != nil {
			return nil, err
		}
		if parsed.MaxCap() > 100 || len(expression) > 16_384 {
			return nil, ErrOverlapComplexity
		}
		program, err := syntax.Compile(parsed.Simplify())
		if err != nil {
			return nil, err
		}
		if len(program.Inst) > 4096 {
			return nil, ErrOverlapComplexity
		}
		programSize += len(program.Inst)
		if programSize > 16384 {
			return nil, ErrOverlapComplexity
		}
		machine.programs = append(machine.programs, program)
	}
	machine.alphabet = scopeAlphabet(machine.programs)
	if len(machine.alphabet) > 4096 {
		return nil, ErrOverlapComplexity
	}
	start := scopeState{previous: -1, pcs: make([][]uint32, len(machine.programs))}
	for i, p := range machine.programs {
		start.pcs[i] = []uint32{uint32(p.Start)}
	}
	queue := []scopeState{start}
	visited := map[string]bool{scopeStateKey(start): true}
	accepted := map[string]bool{}
	var result []scopeVector
	for index := 0; index < len(queue); index++ {
		state := queue[index]
		if state.length > 0 && !strings.HasSuffix(state.witness, " ") {
			vector := scopeVector{witness: state.witness, matches: make([]bool, len(machine.programs))}
			key := make([]byte, len(machine.programs))
			for i, p := range machine.programs {
				_, vector.matches[i] = scopeClosure(p, state.pcs[i], state.previous, -1)
				if vector.matches[i] {
					key[i] = 1
				}
			}
			if !accepted[string(key)] {
				accepted[string(key)] = true
				result = append(result, vector)
			}
		}
		if state.length == 64 {
			continue
		}
		for _, r := range machine.alphabet {
			next := scopeState{previous: scopePrevious(r), witness: state.witness + string(r), length: state.length + 1, pcs: make([][]uint32, len(machine.programs))}
			for i, p := range machine.programs {
				ready, _ := scopeClosure(p, state.pcs[i], state.previous, r)
				out := map[uint32]bool{}
				for _, pc := range ready {
					inst := &p.Inst[pc]
					if inst.MatchRune(r) {
						out[inst.Out] = true
					}
				}
				for pc := range out {
					next.pcs[i] = append(next.pcs[i], pc)
				}
				slices.Sort(next.pcs[i])
			}
			key := scopeStateKey(next)
			if visited[key] {
				continue
			}
			visited[key] = true
			queue = append(queue, next)
			if len(queue) > 20_000 || len(queue)*max(1, len(machine.alphabet))*max(1, programSize) > 5_000_000 {
				return nil, ErrOverlapComplexity
			}
		}
	}
	return result, nil
}

// Empty-width assertions are evaluated with the actual previous/next category;
// dropping them would misclassify word boundaries, multiline anchors and EOF.
func scopeClosure(p *syntax.Prog, pcs []uint32, previous, next rune) ([]uint32, bool) {
	stack := append([]uint32(nil), pcs...)
	seen := make([]bool, len(p.Inst))
	var ready []uint32
	matched := false
	context := syntax.EmptyOpContext(previous, next)
	for len(stack) > 0 {
		pc := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[pc] {
			continue
		}
		seen[pc] = true
		inst := &p.Inst[pc]
		switch inst.Op {
		case syntax.InstAlt, syntax.InstAltMatch:
			stack = append(stack, inst.Out, inst.Arg)
		case syntax.InstCapture, syntax.InstNop:
			stack = append(stack, inst.Out)
		case syntax.InstEmptyWidth:
			if syntax.EmptyOp(inst.Arg)&context == syntax.EmptyOp(inst.Arg) {
				stack = append(stack, inst.Out)
			}
		case syntax.InstMatch:
			matched = true
		case syntax.InstRune, syntax.InstRune1, syntax.InstRuneAny, syntax.InstRuneAnyNotNL:
			ready = append(ready, pc)
		}
	}
	return ready, matched
}

func scopePrevious(r rune) rune {
	if r == '\n' || r == ' ' {
		return r
	}
	if syntax.IsWordChar(r) {
		return 'a'
	}
	return '!'
}
func scopeStateKey(s scopeState) string {
	buffer := make([]byte, 0, 4+len(s.pcs)*8)
	buffer = binary.LittleEndian.AppendUint32(buffer, uint32(s.previous))
	for _, pcs := range s.pcs {
		buffer = binary.LittleEndian.AppendUint32(buffer, uint32(len(pcs)))
		for _, pc := range pcs {
			buffer = binary.LittleEndian.AppendUint32(buffer, pc)
		}
	}
	return string(buffer)
}

// Rune interval boundaries partition the BMP into characters with identical
// transition behavior for all programs. Unicode folding adds every orbit member.
func scopeAlphabet(programs []*syntax.Prog) []rune {
	cuts := []int{1, 10, 11, 32, 33, 46, 47, 48, 58, 65, 91, 92, 93, 95, 96, 97, 123, 0xD800, 0xE000, 0x10000}
	for _, p := range programs {
		for _, inst := range p.Inst {
			switch inst.Op {
			case syntax.InstRune, syntax.InstRune1:
				if len(inst.Rune) == 1 {
					r := inst.Rune[0]
					cuts = append(cuts, int(r), int(r)+1)
					if syntax.Flags(inst.Arg)&syntax.FoldCase != 0 {
						for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
							cuts = append(cuts, int(f), int(f)+1)
						}
					}
				} else {
					for i := 0; i+1 < len(inst.Rune); i += 2 {
						cuts = append(cuts, int(inst.Rune[i]), int(inst.Rune[i+1])+1)
					}
				}
			}
		}
	}
	sort.Ints(cuts)
	var alphabet []rune
	for i, n := range cuts {
		if i > 0 && n == cuts[i-1] || n < 1 || n >= 0x10000 || n >= 0xD800 && n < 0xE000 {
			continue
		}
		r := rune(n)
		end := 0x10000
		for j := i + 1; j < len(cuts); j++ {
			if cuts[j] > n {
				end = cuts[j]
				break
			}
		}
		// Pick a readable representative strictly inside this interval; skipping
		// forbidden intervals must never merge two different transition classes.
		for _, nice := range []rune{'a', 't', '0', '_'} {
			if int(nice) >= n && int(nice) < end {
				r = nice
				break
			}
		}
		alphabet = append(alphabet, r)
	}
	// Readable witnesses first, without changing the partition itself.
	sort.SliceStable(alphabet, func(i, j int) bool {
		readable := func(r rune) int {
			if r == 'a' {
				return 0
			}
			if r >= 'a' && r <= 'z' {
				return 1
			}
			if r >= 'A' && r <= 'Z' {
				return 2
			}
			if r >= '0' && r <= '9' {
				return 3
			}
			if r == '_' {
				return 4
			}
			return 5
		}
		return readable(alphabet[i]) < readable(alphabet[j])
	})
	return alphabet
}
