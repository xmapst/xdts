package task

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/proto"
)

type migrationPrecheckEvidence struct {
	PlanDigest           string `json:"plan_digest,omitempty"`
	Objects              int    `json:"objects"`
	CapabilitiesVerified bool   `json:"capabilities_verified"`
	DefinersVerified     bool   `json:"definers_verified"`
	TablesVerified       bool   `json:"tables_verified"`
	Code                 string `json:"code,omitempty"`
}

func (l *InspectionLoop) assignedDefinitions(ctx context.Context, ep model.Endpoint, node model.Node, args *iproto.DefsProbeArgs) (*iproto.DefsResult, error) {
	d, ok := l.dispatcherSnapshot().(iDefinitionsDispatcher)
	if !ok || args == nil || len(args.GetSchemas()) == 0 {
		return nil, ErrProbeUnavailable
	}
	currentEp, currentNode, err := l.loadManualProbeTargets(ctx, ep.ID, node.ID)
	if err != nil {
		return nil, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	r, err := d.DispatchDefinitions(queryCtx, *currentEp, *currentNode, uuidx.New().String(), proto.Clone(args).(*iproto.DefsProbeArgs))
	if err != nil || !r.GetSuccess() || r.GetDefs() == nil || proto.Size(r.GetDefs()) > 5<<20 || len(r.GetDefs().GetEntries()) > 4000 {
		return nil, ErrDefinitionsFailed
	}
	result := r.GetDefs()
	if result.GetKeyParams()["version"] != ep.Version || result.GetKeyParams()["server_uuid"] != ep.ServerUUID {
		return nil, ErrDefinitionsFailed
	}
	lower, err := mysqlx.ParseLowerCaseTableNames(result.GetKeyParams()["lower_case_table_names"])
	if err != nil || lower.String() != ep.LowerCaseTableNames {
		return nil, ErrDefinitionsFailed
	}
	allowed := map[string]bool{}
	key := func(s string) string {
		if lower != mysqlx.LowerCaseTableNamesSensitive {
			return strings.ToLower(s)
		}
		return s
	}
	for _, schema := range args.GetSchemas() {
		allowed[key(schema)] = true
	}
	for _, entry := range result.GetEntries() {
		if entry == nil || !allowed[key(entry.GetSchema())] {
			return nil, ErrDefinitionsFailed
		}
	}
	return proto.Clone(result).(*iproto.DefsResult), nil
}

// The persisted SQL is the reviewed fallback authority. Never invent a fallback
// using the executing account when no rewrite was explicitly confirmed.
func confirmedPlanFallback(plan schemaplan.Plan) (string, error) {
	fallback := ""
	for _, s := range plan.Steps {
		if s.Kind == model.ObjectTypeTable || s.Kind == model.ObjectTypeSchema || s.Kind == model.ObjectTypeForeignKey {
			continue
		}
		kind, err := schemaplan.ProtocolKind(s.Kind)
		if err != nil {
			return "", err
		}
		objectKind := map[iproto.SchemaObjectKind]schemamig.ObjectKind{
			iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW:      schemamig.ObjectView,
			iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_PROCEDURE: schemamig.ObjectProcedure,
			iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_FUNCTION:  schemamig.ObjectFunction,
			iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_TRIGGER:   schemamig.ObjectTrigger,
			iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_EVENT:     schemamig.ObjectEvent,
		}[kind]
		src, err := schemamig.DefinitionDefiner(objectKind, s.SourceDDL)
		if err != nil {
			return "", err
		}
		dst, err := schemamig.DefinitionDefiner(objectKind, s.DDL)
		if err != nil || dst != nil && dst.CurrentUser {
			return "", errors.New("confirmed definer is not explicit")
		}
		if dst != nil && (src == nil || src.CurrentUser || src.Key() != dst.Key()) {
			if fallback != "" && fallback != dst.SQL() {
				return "", errors.New("ambiguous confirmed fallback")
			}
			fallback = dst.SQL()
		}
	}
	return fallback, nil
}

func sameConfirmedMigration(confirmed, fresh schemaplan.Plan) bool {
	if confirmed.Blocked || fresh.Blocked || len(confirmed.Steps) != len(fresh.Steps) {
		return false
	}
	for i, a := range confirmed.Steps {
		b := fresh.Steps[i]
		if a.Seq != b.Seq || a.Kind != b.Kind || a.SourceSchema != b.SourceSchema || a.SourceName != b.SourceName ||
			a.Schema != b.Schema || a.Name != b.Name || a.SourceDDL != b.SourceDDL || a.DDL != b.DDL ||
			a.AfterSnapshot != b.AfterSnapshot || a.TableName != b.TableName || a.ViewPlaceholder != b.ViewPlaceholder ||
			a.ViewReplacementDDL != b.ViewReplacementDDL || a.ViewPlaceholderDDL != b.ViewPlaceholderDDL ||
			a.ViewReplacementSeq != b.ViewReplacementSeq || a.ViewPlaceholderSeq != b.ViewPlaceholderSeq ||
			(a.Action != b.Action && (a.Action != model.MigrationActionCreate || b.Action != model.MigrationActionSkip)) {
			return false
		}
	}
	return true
}

func (l *InspectionLoop) precheckMigration(ctx context.Context, scope *pipelinePrecheckScope) *migrationPrecheckEvidence {
	e := &migrationPrecheckEvidence{Code: "confirmed_plan_required"}
	if scope.plan == nil {
		return e
	}
	e.PlanDigest = scope.plan.Record.Digest
	schemas := map[string]bool{}
	for _, step := range scope.plan.Plan.Steps {
		schemas[step.SourceSchema] = true
	}
	for _, table := range scope.source.GetTables() {
		schemas[table.GetSchema()] = true
	}
	var sourceSchemas []string
	for schema := range schemas {
		sourceSchemas = append(sourceSchemas, schema)
	}
	sort.Strings(sourceSchemas)
	sourceResult, err := l.assignedDefinitions(ctx, scope.sourceEndpoint, scope.sourceNode, &iproto.DefsProbeArgs{Schemas: sourceSchemas})
	if err != nil {
		e.Code = "source_objects_unverified"
		return e
	}
	all, err := schemaplan.ProbeDefinitions(sourceResult)
	if err != nil {
		e.Code = "source_objects_invalid"
		return e
	}
	source, err := schemaplan.SelectDefinitions(all, scope.rules.Source.ObjectFilters, scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE")
	if err != nil || len(source) == 0 {
		e.Code = "source_scope_invalid"
		return e
	}
	fallback, err := confirmedPlanFallback(scope.plan.Plan)
	if err != nil {
		e.Code = "confirmed_definer_invalid"
		return e
	}
	accounts, err := schemaplan.DefinitionAccounts(source, fallback)
	if err != nil {
		e.Code = "definer_scope_invalid"
		return e
	}
	targetResult, err := l.assignedDefinitions(ctx, scope.targetEndpoint, scope.targetNode, &iproto.DefsProbeArgs{Schemas: schemaplan.TargetSchemas(source, scope.mapping, scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE"), AllowMissingSchemas: true, Accounts: accounts})
	if err != nil {
		e.Code = "target_objects_unverified"
		return e
	}
	verified, err := schemaplan.VerifiedDefinerAccounts(targetResult, accounts)
	if err != nil || len(verified) != len(accounts) {
		e.Code = "definer_accounts_incomplete"
		return e
	}
	// Unknown must never be silently treated as absence and rewritten.
	for _, account := range targetResult.GetAccounts() {
		if !account.GetVerified() {
			e.Code = "definer_accounts_unverified"
			return e
		}
	}
	if fallback != "" {
		account, err := schemamig.ParseDefinerAccount(fallback)
		if err != nil || !verified[account.Key()] {
			e.Code = "confirmed_fallback_missing"
			return e
		}
	}
	target, err := schemaplan.ProbeDefinitions(targetResult)
	if err != nil {
		e.Code = "target_objects_invalid"
		return e
	}
	version, err := mysqlx.ParseVersion(targetResult.GetKeyParams()["version"])
	if err != nil || !mysqlx.SupportedVersion(version) {
		e.Code = "target_version_unverified"
		return e
	}
	caps := mysqlx.DetectCaps(version)
	fresh, err := schemaplan.Build(schemaplan.Request{Source: source, Target: target, Mapping: scope.mapping, TargetCaseSensitive: scope.targetEndpoint.LowerCaseTableNames == "SENSITIVE", DefinerAccounts: verified,
		Options: schemamig.Options{SourceCaseSensitive: scope.sourceEndpoint.LowerCaseTableNames == "SENSITIVE", TargetInvisibleColumn: caps.InvisibleColumn, TargetVectorType: caps.VectorType, DefinerFallback: fallback}})
	if err != nil || !sameConfirmedMigration(scope.plan.Plan, fresh) {
		e.Code = "confirmed_objects_changed"
		return e
	}
	e.Objects = len(fresh.Steps)
	for _, step := range fresh.Steps {
		if err := schemamig.ValidateDDLCharacterSets(step.DDL, targetResult.GetCharacterSets(), targetResult.GetCollations()); err != nil {
			e.Code = "target_charset_unsupported"
			return e
		}
	}
	e.CapabilitiesVerified = true
	fresh.ApplyEnvironmentChecks(targetResult)
	fresh.ApplyDefinerExecuteChecks(targetResult)
	if fresh.Blocked {
		e.Code = "target_execution_conditions_unverified"
		return e
	}
	e.DefinersVerified, e.TablesVerified, e.Code = true, true, ""
	return e
}
