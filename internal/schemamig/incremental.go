package schemamig

import (
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// IncrementalRequest carries one binlog Query event and its source default database.
type IncrementalRequest struct {
	Schema             string
	DDL                string
	Mapping            Mapping
	Options            Options
	SourceDependencies map[SchemaTable]TableDependencies
}

// IncrementalResult includes policy facts obtained from the parsed statement.
// Affected tables in the wire message are observational and are never trusted here.
type IncrementalResult struct {
	Result
	Schema            string
	Drop              bool
	Truncate          bool
	UsesDefaultSchema bool
	// Noop means all DDL effects were excluded by column projection. There
	// is no executable SQL; callers must not manufacture an empty GTID commit.
	Noop bool
}

func RewriteIncremental(req IncrementalRequest) (IncrementalResult, error) {
	if req.DDL == "" {
		return IncrementalResult{}, ErrEmptyDDL
	}
	req.Options.SourceDefaultSchema = req.Schema
	out := IncrementalResult{Schema: req.Mapping.schemaTarget(req.Schema, req.Options.SourceCaseSensitive), UsesDefaultSchema: true}
	marked, invisible, markersOK := stripInvisibleColumnMarkers(stripUTF8MB3Markers(req.DDL))
	stmt, err := parseOneTiDBStmt(marked)
	// Index visibility is supported directly by the parser. The CREATE-column
	// fidelity scanner must not remove an ALTER INDEX visibility keyword.
	if original, parseErr := parseOneTiDBStmt(stripUTF8MB3Markers(req.DDL)); parseErr == nil {
		if _, ok := original.(*ast.AlterTableStmt); ok && hasIndexVisibility(original.(*ast.AlterTableStmt)) {
			stmt, err, invisible, markersOK = original, nil, nil, true
		}
	}
	// TiDB accepts CREATE PROCEDURE, but the ANTLR pipeline owns stored
	// program bodies and their local SQL scopes, as it does for snapshot DDL.
	if _, ok := stmt.(*ast.ProcedureInfo); ok {
		err = ErrDDLKindMismatch
	}
	if err != nil {
		if definition, ok := alterViewDefinition(req.DDL); ok {
			out.Result, err = rewriteAlterView(req, definition)
			return out, err
		}
		// The routine parser owns compound bodies; do not identify them with SQL text prefixes.
		pr, parseErr := parseRoutine(req.DDL)
		if parseErr != nil || len(pr.tree.AllQuery()) != 1 {
			out.Result = blocked("增量 DDL 不能解析为一条完整语句", req.DDL)
			return out, nil
		}
		kind := ObjectUnknown
		switch {
		case findCreateProcedure(pr.tree) != nil:
			kind = ObjectProcedure
		case findCreateFunction(pr.tree) != nil:
			kind = ObjectFunction
		case findCreateTrigger(pr.tree) != nil:
			kind = ObjectTrigger
		case findCreateEvent(pr.tree) != nil:
			kind = ObjectEvent
		}
		if kind != ObjectUnknown {
			out.Result, err = Rewrite(Request{Kind: kind, DDL: req.DDL, Mapping: req.Mapping, Options: req.Options})
			return out, err
		}
		if alter := findRoutineAlter(pr.tree); alter.count == 1 {
			out.Result, err = rewriteRoutineAlter(req, pr, alter)
			return out, err
		}
		finder := &routineDropFinder{BaseMySQLParserListener: &mysql.BaseMySQLParserListener{}}
		antlr.ParseTreeWalkerDefault.Walk(finder, pr.tree)
		if finder.count != 1 {
			out.Result = blocked("该增量 DDL 暂无法安全改写", req.DDL)
			return out, nil
		}
		rw := antlr.NewTokenStreamRewriter(pr.stream)
		antlr.ParseTreeWalkerDefault.Walk(newQualifiedNameRewriter(req.Mapping, req.Options, rw), pr.tree)
		q := pr.tree.AllQuery()[0]
		out.Result = Result{DDL: rewrittenTextRange(rw, q.GetStart().GetTokenIndex(), q.GetStop().GetTokenIndex()-1)}
		out.Drop = true
		return out, nil
	}
	// CREATE keeps the original text for the established fidelity pipeline.
	switch s := stmt.(type) {
	case *ast.AnalyzeTableStmt:
		out.Noop = true // Source statistics are local and have no target DDL effect.
		return out, nil
	case *ast.CreateTableStmt:
		if s.TemporaryKeyword != ast.TemporaryNone || s.Select != nil {
			out.Result = blocked("临时表或 CREATE TABLE SELECT 不能保真为单个增量 DDL", req.DDL)
			return out, nil
		}
		out.Result, err = Rewrite(Request{Kind: ObjectTable, DefaultSchema: req.Schema, DDL: req.DDL, Mapping: req.Mapping, Options: req.Options})
		return out, err
	case *ast.CreateViewStmt:
		out.Result, err = Rewrite(Request{Kind: ObjectView, DefaultSchema: req.Schema, DDL: req.DDL, Mapping: req.Mapping, Options: req.Options})
		return out, err
	}
	if !markersOK || len(invisible) != 0 {
		out.Result = blocked("此增量 DDL 的 INVISIBLE 属性暂不能安全保留", req.DDL)
		return out, nil
	}
	// Explicitly bind unqualified DDL table names to the event's default database.
	// Apply mapping only after the CREATE pipelines have consumed source names.
	binder := &incrementalNames{schema: req.Schema}
	if _, ok := stmt.Accept(binder); !ok {
		return out, fmt.Errorf("schemamig: DDL name binding failed")
	}
	switch s := stmt.(type) {
	case *ast.DropProcedureStmt:
		out.Drop = true
		binder.Enter(s.ProcedureName)
		(&incrementalNames{schema: req.Schema, mapping: &req.Mapping, sensitive: req.Options.SourceCaseSensitive}).Enter(s.ProcedureName)
	case *ast.CreateDatabaseStmt:
		out.UsesDefaultSchema = false
		s.Name = ast.NewCIStr(req.Mapping.schemaTarget(s.Name.O, req.Options.SourceCaseSensitive))
	case *ast.AlterDatabaseStmt:
		out.UsesDefaultSchema = false
		if s.AlterDefaultDatabase {
			s.Name = ast.NewCIStr(req.Schema)
			s.AlterDefaultDatabase = false
		}
		s.Name = ast.NewCIStr(req.Mapping.schemaTarget(s.Name.O, req.Options.SourceCaseSensitive))
	case *ast.DropDatabaseStmt:
		out.UsesDefaultSchema = false
		out.Drop = true
		s.Name = ast.NewCIStr(req.Mapping.schemaTarget(s.Name.O, req.Options.SourceCaseSensitive))
	case *ast.AlterTableStmt:
		req.Mapping = columnPolicyMapping(req.Mapping, s, s.Table.Schema.O, s.Table.Name.O, req.Options.SourceCaseSensitive)
		// Dropping a column is destructive too; the same explicit allow_drop applies.
		for _, spec := range s.Specs {
			if spec.Constraint != nil && referencesExcludedForeignColumn(spec.Constraint, req.Mapping, s.Table.Schema.O, req.Options.SourceCaseSensitive) {
				out.Result = blocked("外键引用被过滤的目标列，不能安全投影 ALTER", req.DDL)
				return out, nil
			}
			for _, constraint := range spec.NewConstraints {
				if referencesExcludedForeignColumn(constraint, req.Mapping, s.Table.Schema.O, req.Options.SourceCaseSensitive) {
					out.Result = blocked("外键引用被过滤的目标列，不能安全投影 ALTER", req.DDL)
					return out, nil
				}
			}
			if spec.Tp == ast.AlterTableDropColumn || spec.Tp == ast.AlterTableDropPartition {
				out.Drop = true
			}
			if spec.Tp == ast.AlterTableTruncatePartition {
				out.Truncate = true
			}
		}
		drops := req.Mapping.dropColumnSet(s.Table.Schema.O, s.Table.Name.O, req.Options.SourceCaseSensitive)
		filtered := len(drops) != 0 || req.Mapping.ColumnFilter != nil && req.Mapping.ColumnFilter.FiltersTable(s.Table.Schema.O, s.Table.Name.O)
		if filtered {
			if drops == nil {
				drops = map[string]bool{}
			}
			original := req.originalDependencies(s.Table.Schema.O, s.Table.Name.O)
			if original != nil {
				for _, column := range original.Columns {
					if req.Mapping.ColumnFilter != nil && !req.Mapping.ColumnFilter.Allow(s.Table.Schema.O, s.Table.Name.O, column) {
						drops[foldKey(column)] = true
					}
				}
			}
			if reason := projectAlterDependencies(s, original, drops); reason != "" {
				out.Result = blocked(reason, req.DDL)
				return out, nil
			}
			if reason := projectAlterColumns(s, drops); reason != "" {
				out.Result = blocked(reason, req.DDL)
				return out, nil
			}
			if len(s.Specs) == 0 {
				out.Noop = true
				return out, nil
			}
		}
		for _, spec := range s.Specs {
			spec.Accept(&definitionColumns{mapping: req.Mapping, schema: s.Table.Schema.O, table: s.Table.Name.O, sensitive: req.Options.SourceCaseSensitive})
		}
	case *ast.CreateIndexStmt:
		req.Mapping = columnPolicyMapping(req.Mapping, s, s.Table.Schema.O, s.Table.Name.O, req.Options.SourceCaseSensitive)
		if drops := req.Mapping.dropColumnSet(s.Table.Schema.O, s.Table.Name.O, req.Options.SourceCaseSensitive); len(drops) != 0 && constraintReferencesDropped(&ast.Constraint{Keys: s.IndexPartSpecifications}, drops) {
			out.Noop = true
			return out, nil
		}
		s.Accept(&definitionColumns{mapping: req.Mapping, schema: s.Table.Schema.O, table: s.Table.Name.O, sensitive: req.Options.SourceCaseSensitive})
	case *ast.DropTableStmt:
		if s.TemporaryKeyword != ast.TemporaryNone {
			out.Result = blocked("临时表 DDL 不参与增量同步", req.DDL)
			return out, nil
		}
		out.Drop = true
	case *ast.RenameTableStmt:
		if req.Mapping.ColumnFilter != nil {
			for _, pair := range s.TableToTables {
				// Shadow-table tools rename physical helper tables as one atomic
				// cutover. Their column policy belongs to the logical table, so the
				// normal filtered-rename safety block does not apply when both ends
				// resolve through the immutable alias map.
				oldShadow, newShadow := false, false
				if req.Mapping.TableAliases != nil {
					_, _, oldShadow = req.Mapping.TableAliases.ResolveTable(pair.OldTable.Schema.O, pair.OldTable.Name.O)
					_, _, newShadow = req.Mapping.TableAliases.ResolveTable(pair.NewTable.Schema.O, pair.NewTable.Name.O)
				}
				if (!oldShadow && !newShadow) && (req.Mapping.ColumnFilter.FiltersTable(pair.OldTable.Schema.O, pair.OldTable.Name.O) || req.Mapping.ColumnFilter.FiltersTable(pair.NewTable.Schema.O, pair.NewTable.Name.O)) {
					out.Result = blocked("表重命名涉及列过滤范围，需重新初始化目标结构", req.DDL)
					return out, nil
				}
			}
		}
	case *ast.DropIndexStmt:
		drops := req.Mapping.dropColumnSet(s.Table.Schema.O, s.Table.Name.O, req.Options.SourceCaseSensitive)
		if len(drops) > 0 || req.Mapping.ColumnFilter != nil && req.Mapping.ColumnFilter.FiltersTable(s.Table.Schema.O, s.Table.Name.O) {
			if drops == nil {
				drops = map[string]bool{}
			}
			original := req.originalDependencies(s.Table.Schema.O, s.Table.Name.O)
			if original == nil {
				out.Result = blocked("删除索引需原表依赖定义才能安全投影列过滤", req.DDL)
				return out, nil
			}
			for _, column := range original.Columns {
				if req.Mapping.ColumnFilter != nil && !req.Mapping.ColumnFilter.Allow(s.Table.Schema.O, s.Table.Name.O, column) {
					drops[foldKey(column)] = true
				}
			}
			index := original.object("index", s.IndexName)
			if index < 0 {
				out.Result = blocked("删除索引缺少原表索引身份", req.DDL)
				return out, nil
			}
			if dependencyExcluded(original.Objects[index], drops) {
				out.Noop = true
				return out, nil
			}
		}
	case *ast.TruncateTableStmt:
		out.Truncate = true
	default:
		out.Result = blocked(fmt.Sprintf("该增量语句类型不能作为结构变更执行（%T）", stmt), req.DDL)
		return out, nil
	}
	mapper := &incrementalNames{schema: req.Schema, mapping: &req.Mapping, sensitive: req.Options.SourceCaseSensitive}
	stmt.Accept(mapper)
	out.DDL, err = restoreTiDB(stmt)
	out.DDL = restoreUTF8MB3Markers(out.DDL)
	if err != nil {
		out.Result = blocked(err.Error(), req.DDL)
	}
	return out, nil
}

func hasIndexVisibility(stmt *ast.AlterTableStmt) bool {
	for _, spec := range stmt.Specs {
		if spec.Tp == ast.AlterTableIndexInvisible {
			return true
		}
	}
	return false
}

func (req IncrementalRequest) originalDependencies(schema, table string) *TableDependencies {
	for ref, value := range req.SourceDependencies {
		if value.Complete && foldEqual(ref.Schema, schema, req.Options.SourceCaseSensitive) && foldEqual(ref.Table, table, req.Options.SourceCaseSensitive) {
			copy := value.Clone()
			return &copy
		}
	}
	return nil
}

type incrementalNames struct {
	schema    string
	mapping   *Mapping
	sensitive bool
}

func (v *incrementalNames) Enter(n ast.Node) (ast.Node, bool) {
	if t, ok := n.(*ast.TableName); ok {
		schema := t.Schema.O
		if schema == "" {
			schema = v.schema
		}
		name := t.Name.O
		if v.mapping != nil {
			name = v.mapping.tableTarget(schema, name, v.sensitive)
			schema = v.mapping.schemaTarget(schema, v.sensitive)
		}
		t.Schema = ast.NewCIStr(schema)
		t.Name = ast.NewCIStr(name)
	}
	return n, false
}
func (v *incrementalNames) Leave(n ast.Node) (ast.Node, bool) { return n, true }

type routineDropFinder struct {
	*mysql.BaseMySQLParserListener
	count int
}

func (f *routineDropFinder) EnterDropProcedure(*mysql.DropProcedureContext) { f.count++ }
func (f *routineDropFinder) EnterDropFunction(*mysql.DropFunctionContext)   { f.count++ }
func (f *routineDropFinder) EnterDropTrigger(*mysql.DropTriggerContext)     { f.count++ }
func (f *routineDropFinder) EnterDropEvent(*mysql.DropEventContext)         { f.count++ }
