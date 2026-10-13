package schemamig

import (
	"fmt"
	"sort"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/format"
	tidbmysql "github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// Difference describes an actual target mismatch. SQL string values are kept
// distinct: normalization never changes the contents of a quoted literal.
type Difference struct {
	Path     string `json:"path"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
}

// CompareDefinitions checks a table's write-relevant columns and keys. Extra
// target columns must accept omitted values and cannot add unique constraints.
// Other objects require the same parsed, normalized definition.
func CompareDefinitions(kind ObjectKind, expected, actual string) ([]Difference, error) {
	return CompareDefinitionsInSchema(kind, "", expected, actual)
}

func CompareDefinitionsInSchema(kind ObjectKind, schema, expected, actual string) ([]Difference, error) {
	if kind == ObjectTable {
		return compareTables(expected, actual)
	}
	a, err := normalizeDefinition(kind, schema, expected)
	if err != nil {
		return nil, err
	}
	b, err := normalizeDefinition(kind, schema, actual)
	if err != nil {
		return nil, err
	}
	if a == b {
		return nil, nil
	}
	return []Difference{{Path: "definition", Expected: a, Actual: b}}, nil
}

func normalizeDefinition(kind ObjectKind, schema, ddl string) (string, error) {
	if kind == ObjectView {
		n, err := parseOneTiDBStmt(stripUTF8MB3Markers(ddl))
		if err != nil {
			return "", err
		}
		v, ok := n.(*ast.CreateViewStmt)
		if !ok {
			return "", ErrDDLKindMismatch
		}
		v.OrReplace = false
		if schema != "" {
			if v.ViewName.Schema.O == "" {
				v.ViewName.Schema = ast.NewCIStr(schema)
			}
			v.Select.Accept(&tableNameRewriter{mapping: Mapping{}, caseSensitive: true, defaultSchema: schema})
			binding := &viewComparisonBindings{relations: map[string]string{}, ambiguous: map[string]bool{}, aliases: map[string]bool{}}
			v.Select.Accept(binding)
			v.Select.Accept(&viewComparisonColumns{bindings: binding})
		}
		out, err := restoreMySQLView(v, ddl)
		return restoreUTF8MB3Markers(out), err
	}
	pr, err := parseRoutine(ddl)
	if err != nil {
		return "", err
	}
	if len(pr.tree.AllQuery()) != 1 {
		return "", ErrDDLKindMismatch
	}
	if schema != "" {
		rw := antlr.NewTokenStreamRewriter(pr.stream)
		antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(Mapping{}, Options{SourceDefaultSchema: schema, SourceCaseSensitive: true}, rw), pr.tree)
		q := pr.tree.AllQuery()[0]
		bound := rewrittenTextRange(rw, q.GetStart().GetTokenIndex(), q.GetStop().GetTokenIndex()-1)
		pr, err = parseRoutine(bound)
		if err != nil {
			return "", err
		}
	}
	pr.stream.Fill()
	var b strings.Builder
	for _, tok := range pr.stream.GetAllTokens() {
		if tok.GetChannel() != antlr.TokenDefaultChannel || tok.GetTokenType() == antlr.TokenEOF {
			continue
		}
		s := tok.GetText()
		if s == ";" && tok.GetTokenIndex() == pr.tree.AllQuery()[0].GetStop().GetTokenIndex() {
			continue
		}
		if len(s) == 0 {
			continue
		}
		if tok.GetTokenType() == mysql.MySQLLexerIDENTIFIER || tok.GetTokenType() == mysql.MySQLLexerBACK_TICK_QUOTED_ID {
			s = "ID:" + unquoteMySQLIdent(s)
		}
		if tok.GetTokenType() != mysql.MySQLLexerIDENTIFIER && tok.GetTokenType() != mysql.MySQLLexerBACK_TICK_QUOTED_ID && tok.GetTokenType() != mysql.MySQLLexerDOUBLE_QUOTED_TEXT && tok.GetTokenType() != mysql.MySQLLexerSINGLE_QUOTED_TEXT && tok.GetTokenType() != mysql.MySQLLexerNCHAR_TEXT {
			s = strings.ToUpper(s)
		}
		fmt.Fprintf(&b, "%d:%s|", len(s), s)
	}
	return b.String(), nil
}

// SHOW CREATE omits the current database from qualified column references.
// Bind only an unambiguous real table name; aliases retain their own scope.
type viewComparisonBindings struct {
	relations map[string]string
	ambiguous map[string]bool
	aliases   map[string]bool
}

func (v *viewComparisonBindings) Enter(n ast.Node) (ast.Node, bool) {
	switch node := n.(type) {
	case *ast.TableSource:
		if node.AsName.O != "" {
			v.aliases[node.AsName.O] = true
		}
	case *ast.TableName:
		if prior, ok := v.relations[node.Name.O]; ok && prior != node.Schema.O {
			v.ambiguous[node.Name.O] = true
		}
		v.relations[node.Name.O] = node.Schema.O
	}
	return n, false
}
func (v *viewComparisonBindings) Leave(n ast.Node) (ast.Node, bool) { return n, true }

type viewComparisonColumns struct{ bindings *viewComparisonBindings }

func (v *viewComparisonColumns) Enter(n ast.Node) (ast.Node, bool) {
	if c, ok := n.(*ast.ColumnName); ok && c.Schema.O == "" && c.Table.O != "" && !v.bindings.aliases[c.Table.O] && !v.bindings.ambiguous[c.Table.O] {
		if schema, ok := v.bindings.relations[c.Table.O]; ok {
			c.Schema = ast.NewCIStr(schema)
		}
	}
	return n, false
}
func (v *viewComparisonColumns) Leave(n ast.Node) (ast.Node, bool) { return n, true }

type comparedColumn struct {
	signature string
	omittable bool
}
type comparedTable struct {
	columns map[string]comparedColumn
	keys    map[string]string
	options map[string]string
}

func comparisonTable(ddl string) (comparedTable, error) {
	marked, inv, ok := stripInvisibleColumnMarkers(stripUTF8MB3Markers(ddl))
	if !ok {
		return comparedTable{}, fmt.Errorf("cannot locate invisible columns")
	}
	n, err := parseOneTiDBStmt(marked)
	if err != nil {
		return comparedTable{}, err
	}
	ct, ok := n.(*ast.CreateTableStmt)
	if !ok || ct.Select != nil || ct.ReferTable != nil || ct.TemporaryKeyword != ast.TemporaryNone {
		return comparedTable{}, ErrDDLKindMismatch
	}
	out := comparedTable{columns: map[string]comparedColumn{}, keys: map[string]string{}, options: map[string]string{}}
	tableCharset, tableCollation := "", ""
	for _, opt := range ct.Options {
		if opt.Tp == ast.TableOptionCharset {
			tableCharset = restoreUTF8MB3Markers(opt.StrValue)
		}
		if opt.Tp == ast.TableOptionCollate {
			tableCollation = restoreUTF8MB3Markers(opt.StrValue)
		}
		switch opt.Tp {
		case ast.TableOptionCharset, ast.TableOptionCollate:
			text, err := restoreTiDB(opt)
			if err != nil {
				return out, err
			}
			out.options[fmt.Sprint(opt.Tp)] = restoreUTF8MB3Markers(text)
		}
	}
	primary := map[string]bool{}
	inlineUnique := map[string]bool{}
	for _, c := range ct.Constraints {
		if c.Tp == ast.ConstraintPrimaryKey {
			for _, key := range c.Keys {
				if key.Column != nil {
					primary[foldKey(key.Column.Name.O)] = true
				}
			}
		}
	}
	for _, c := range ct.Cols {
		for _, o := range c.Options {
			if o.Tp == ast.ColumnOptionPrimaryKey {
				primary[foldKey(c.Name.Name.O)] = true
			}
			if o.Tp == ast.ColumnOptionUniqKey {
				inlineUnique[foldKey(c.Name.Name.O)] = true
			}
		}
	}
	invisible := map[string]bool{}
	for _, m := range inv {
		invisible[foldKey(m.column)] = true
	}
	for _, c := range ct.Cols {
		if c.Tp == nil {
			return out, fmt.Errorf("missing column type")
		}
		name := foldKey(c.Name.Name.O)
		if _, exists := out.columns[name]; exists {
			return out, fmt.Errorf("duplicate column")
		}
		var typ strings.Builder
		columnType := c.Tp.Clone()
		switch columnType.GetType() {
		case tidbmysql.TypeTiny, tidbmysql.TypeShort, tidbmysql.TypeInt24, tidbmysql.TypeLong, tidbmysql.TypeLonglong:
			if !tidbmysql.HasZerofillFlag(columnType.GetFlag()) {
				columnType.SetFlen(types.UnspecifiedLength)
			}
		}
		if err := columnType.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &typ)); err != nil {
			return out, err
		}
		nullable := !primary[name]
		defaultValue := ""
		hasDefault := false
		auto := false
		generated := false
		extras := []string{}
		columnCollation := ""
		for _, o := range c.Options {
			switch o.Tp {
			case ast.ColumnOptionNotNull:
				nullable = false
			case ast.ColumnOptionNull:
				if !primary[name] {
					nullable = true
				}
			case ast.ColumnOptionDefaultValue:
				hasDefault = true
				defaultValue, err = restoreTiDB(o.Expr)
				if err != nil {
					return out, err
				}
			case ast.ColumnOptionAutoIncrement:
				auto = true
			case ast.ColumnOptionGenerated:
				generated = true
				s, err := restoreTiDB(o)
				if err != nil {
					return out, err
				}
				extras = append(extras, s)
			case ast.ColumnOptionPrimaryKey, ast.ColumnOptionUniqKey, ast.ColumnOptionComment:
			case ast.ColumnOptionCollate:
				columnCollation = restoreUTF8MB3Markers(o.StrValue)
			default:
				s, err := restoreTiDB(o)
				if err != nil {
					return out, err
				}
				extras = append(extras, s)
			}
		}
		if columnCollation == "" && tableCollation != "" && !tidbmysql.HasBinaryFlag(columnType.GetFlag()) {
			switch columnType.GetType() {
			case tidbmysql.TypeVarchar, tidbmysql.TypeVarString, tidbmysql.TypeString, tidbmysql.TypeEnum, tidbmysql.TypeSet, tidbmysql.TypeTinyBlob, tidbmysql.TypeMediumBlob, tidbmysql.TypeLongBlob, tidbmysql.TypeBlob:
				charset := restoreUTF8MB3Markers(columnType.GetCharset())
				if charset != "binary" && (charset == "" || strings.EqualFold(charset, tableCharset)) {
					columnCollation = tableCollation
				}
			}
		}
		if columnCollation != "" {
			extras = append(extras, "COLLATE "+strings.ToLower(columnCollation))
		}
		if nullable && !hasDefault {
			defaultValue = "NULL"
		}
		sort.Strings(extras)
		out.columns[name] = comparedColumn{signature: fmt.Sprintf("%s;nullable=%t;default=%s;auto=%t;invisible=%t;%s", restoreUTF8MB3Markers(typ.String()), nullable, defaultValue, auto, invisible[name], strings.Join(extras, ";")), omittable: nullable || hasDefault || auto || generated}
	}
	keySig := func(c *ast.Constraint) (string, error) {
		parts := []string{}
		for _, k := range c.Keys {
			if k.Column != nil {
				k.Column.Name = ast.NewCIStr(foldKey(k.Column.Name.O))
				k.Column.Schema = ast.NewCIStr("")
				k.Column.Table = ast.NewCIStr("")
			}
			s, err := restoreTiDB(k)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, ","), nil
	}
	for _, c := range ct.Constraints {
		switch c.Tp {
		case ast.ConstraintPrimaryKey, ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
			s, err := keySig(c)
			if err != nil {
				return out, err
			}
			prefix := "unique:"
			if c.Tp == ast.ConstraintPrimaryKey {
				prefix = "primary:"
			}
			out.keys[prefix+s] = s
		case ast.ConstraintCheck, ast.ConstraintForeignKey:
			s, err := restoreTiDB(c)
			if err != nil {
				return out, err
			}
			out.keys["constraint:"+s] = s
		}
	}
	for name := range inlineUnique {
		out.keys["unique:"+quoteMySQLIdent(name)] = quoteMySQLIdent(name)
	}
	// Inline PRIMARY KEY and table-level PRIMARY KEY have the same meaning.
	tablePrimary := false
	for key := range out.keys {
		if strings.HasPrefix(key, "primary:") {
			tablePrimary = true
		}
	}
	if !tablePrimary && len(primary) != 0 {
		names := make([]string, 0, len(primary))
		for name := range primary {
			names = append(names, name)
		}
		sort.Strings(names)
		quoted := make([]string, len(names))
		for i, name := range names {
			quoted[i] = quoteMySQLIdent(name)
		}
		s := strings.Join(quoted, ",")
		out.keys["primary:"+s] = s
	}
	return out, nil
}

func compareTables(expected, actual string) ([]Difference, error) {
	a, err := comparisonTable(expected)
	if err != nil {
		return nil, err
	}
	b, err := comparisonTable(actual)
	if err != nil {
		return nil, err
	}
	diff := []Difference{}
	for key, value := range a.options {
		if value != b.options[key] {
			diff = append(diff, Difference{Path: "table_options." + key, Expected: value, Actual: b.options[key]})
		}
	}
	for name, col := range a.columns {
		other, ok := b.columns[name]
		if !ok {
			diff = append(diff, Difference{Path: "columns." + name, Expected: col.signature, Actual: "missing"})
		} else if col.signature != other.signature {
			diff = append(diff, Difference{Path: "columns." + name, Expected: col.signature, Actual: other.signature})
		}
	}
	for name, col := range b.columns {
		if _, ok := a.columns[name]; !ok && !col.omittable {
			diff = append(diff, Difference{Path: "columns." + name, Expected: "omittable target column", Actual: col.signature})
		}
	}
	for key, value := range a.keys {
		if _, ok := b.keys[key]; !ok {
			diff = append(diff, Difference{Path: "keys." + key, Expected: value, Actual: "missing"})
		}
	}
	for key, value := range b.keys {
		if _, ok := a.keys[key]; !ok {
			diff = append(diff, Difference{Path: "keys." + key, Expected: "absent", Actual: value})
		}
	}
	sort.Slice(diff, func(i, j int) bool { return diff[i].Path < diff[j].Path })
	return diff, nil
}

// ViewDependencies enumerates relation references from SELECT, excluding CTE
// aliases. Callers decide whether each actual dependency is in migration scope.
func ViewDependencies(schema, ddl string) ([]SchemaTable, error) {
	n, err := parseOneTiDBStmt(stripUTF8MB3Markers(ddl))
	if err != nil {
		return nil, err
	}
	v, ok := n.(*ast.CreateViewStmt)
	if !ok {
		return nil, ErrDDLKindMismatch
	}
	refs := &scopedViewReferences{schema: schema, refs: map[SchemaTable]bool{}, ctes: map[string]bool{}}
	v.Select.Accept(refs)
	out := make([]SchemaTable, 0, len(refs.refs))
	for ref := range refs.refs {
		out = append(out, ref)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Schema != out[j].Schema {
			return out[i].Schema < out[j].Schema
		}
		return out[i].Table < out[j].Table
	})
	return out, nil
}

type viewReferences struct {
	schema string
	refs   map[SchemaTable]bool
	ctes   map[string]bool
}

func (v *viewReferences) Enter(n ast.Node) (ast.Node, bool) {
	switch x := n.(type) {
	case *ast.CommonTableExpression:
		v.ctes[foldKey(x.Name.O)] = true
	case *ast.TableName:
		schema := x.Schema.O
		if schema == "" {
			if v.ctes[foldKey(x.Name.O)] {
				return n, true
			}
			schema = v.schema
		}
		v.refs[SchemaTable{Schema: schema, Table: x.Name.O}] = true
	}
	return n, false
}
func (v *viewReferences) Leave(n ast.Node) (ast.Node, bool) { return n, true }

// CompareDatabaseDefinitions ignores IF NOT EXISTS and the already matched
// database identifier, while retaining character set and collation options.
func CompareDatabaseDefinitions(expected, actual string) ([]Difference, error) {
	normalized := func(ddl string) (string, error) {
		n, err := parseOneTiDBStmt(stripUTF8MB3Markers(ddl))
		if err != nil {
			return "", err
		}
		db, ok := n.(*ast.CreateDatabaseStmt)
		if !ok {
			return "", ErrDDLKindMismatch
		}
		db.IfNotExists = false
		db.Name = ast.NewCIStr("_")
		options := db.Options[:0]
		for _, opt := range db.Options {
			if opt.Tp == ast.DatabaseOptionEncryption {
				continue
			}
			options = append(options, opt)
		}
		db.Options = options
		out, err := restoreTiDB(db)
		return restoreUTF8MB3Markers(out), err
	}
	a, err := normalized(expected)
	if err != nil {
		return nil, err
	}
	b, err := normalized(actual)
	if err != nil {
		return nil, err
	}
	if a == b {
		return nil, nil
	}
	return []Difference{{Path: "database.options", Expected: a, Actual: b}}, nil
}
