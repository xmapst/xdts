package task

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"unicode/utf8"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
	"google.golang.org/protobuf/encoding/protojson"
)

var ErrPipelinePrecheckInput = errors.New("pipeline precheck configuration invalid")

type pipelinePrecheckScope struct {
	source, target                 *iproto.PrecheckProbeArgs
	rules                          *pipelinespec.StrategyRulesDoc
	mapping                        schemamig.Mapping
	sourceEndpoint, targetEndpoint model.Endpoint
	sourceNode, targetNode         model.Node
	required                       []repo.PrecheckRequirement
	plan                           *repo.StoredSchemaPlan
	migration                      *migrationPrecheckEvidence
}

func precheckName(name string) bool {
	return name != "" && utf8.ValidString(name) && utf8.RuneCountInString(name) <= 64 && !strings.ContainsRune(name, 0)
}

func precheckCatalogTables(catalog *iproto.CatalogResult, filters []*iproto.ObjectFilterRule, sensitive bool) ([]*iproto.PrecheckTable, error) {
	if catalog == nil || len(catalog.GetTables()) > 1000 {
		return nil, ErrCatalogTooLarge
	}
	definitions := []schemaplan.Definition{}
	seen := map[string]bool{}
	for _, t := range catalog.GetTables() {
		if t == nil || !precheckName(t.GetSchema()) || !precheckName(t.GetTable()) {
			return nil, ErrPipelinePrecheckInput
		}
		switch strings.ToLower(t.GetSchema()) {
		case "mysql", "sys", "information_schema", "performance_schema":
			return nil, ErrPipelinePrecheckInput
		}
		key := t.GetSchema() + "\x00" + t.GetTable()
		if !sensitive {
			key = strings.ToLower(key)
		}
		if seen[key] {
			return nil, ErrPipelinePrecheckInput
		}
		seen[key] = true
		definitions = append(definitions, schemaplan.Definition{Schema: t.GetSchema(), Name: t.GetTable(), Kind: model.ObjectTypeTable})
	}
	selected, err := schemaplan.SelectDefinitions(definitions, filters, sensitive)
	if err != nil {
		return nil, ErrPipelinePrecheckInput
	}
	if len(selected) == 0 {
		return nil, ErrPipelinePrecheckInput
	}
	out := make([]*iproto.PrecheckTable, 0, len(selected))
	for _, t := range selected {
		out = append(out, &iproto.PrecheckTable{Schema: t.Schema, Table: t.Name})
	}
	return out, nil
}

func (s *pipelinePrecheckScope) setTables(tables []*iproto.PrecheckTable) error {
	sensitive := s.sourceEndpoint.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()
	conflict := s.rules.Target.ConflictPolicy
	s.source.Tables, s.target.Tables = nil, nil
	seen := map[string]bool{}
	for _, table := range tables {
		column := ""
		if conflict.GetMode() == iproto.ConflictMode_CONFLICT_MODE_LWW {
			for _, c := range conflict.GetLwwTables() {
				matches := c.GetSchema() == table.GetSchema() && c.GetTable() == table.GetTable()
				if !sensitive {
					matches = strings.EqualFold(c.GetSchema(), table.GetSchema()) && strings.EqualFold(c.GetTable(), table.GetTable())
				}
				if matches {
					if column != "" || !precheckName(c.GetTimestampColumn()) {
						return ErrPipelinePrecheckInput
					}
					column = c.GetTimestampColumn()
				}
			}
		}
		schema, name, columns := s.mapping.TargetNames(table.GetSchema(), table.GetTable(), []string{column}, sensitive)
		if !precheckName(schema) || !precheckName(name) {
			return ErrPipelinePrecheckInput
		}
		key := schema + "\x00" + name
		if s.targetEndpoint.LowerCaseTableNames != mysqlx.LowerCaseTableNamesSensitive.String() {
			key = strings.ToLower(key)
		}
		if seen[key] {
			return ErrPipelinePrecheckInput
		}
		seen[key] = true
		s.source.Tables = append(s.source.Tables, &iproto.PrecheckTable{Schema: table.GetSchema(), Table: table.GetTable(), LwwTimestampColumn: column})
		s.target.Tables = append(s.target.Tables, &iproto.PrecheckTable{Schema: schema, Table: name, LwwTimestampColumn: columns[0]})
	}
	return nil
}

func precheckNodeRequirements(side string, args *iproto.PrecheckProbeArgs, lww bool) []repo.PrecheckRequirement {
	items := []repo.PrecheckRequirement{}
	add := func(code string, warning bool) {
		level := model.PrecheckLevelBlocking
		if warning {
			level = model.PrecheckLevelWarning
		}
		items = append(items, repo.PrecheckRequirement{Item: side + "/" + code, Level: level})
	}
	for _, c := range []string{"PRE-01", "PRE-02", "PRE-03", "PRE-04"} {
		add(c, false)
	}
	if args.GetPersistentLineageV1() {
		add("PRE-35", false)
	}
	if side == "source" {
		if args.GetSnapshotReplica() != nil {
			add("PRE-34", false)
		}
		add("PRE-05", false)
		add("PRE-06", false)
		if args.GetSnapshot() {
			add("PRE-07", false)
			add("PRE-21", true)
		} else {
			add("PRE-18", args.GetKeylessIncrementalWarning())
		}
	} else {
		add("PRE-08", false)
		add("PRE-12", false)
		add("PRE-16", false)
		if args.GetSnapshot() {
			add("PRE-19", true)
			add("PRE-25", false)
		}
		if args.GetCreatesFunctions() {
			add("PRE-15", false)
		}
	}
	if lww {
		add("PRE-17", false)
	}
	return items
}

func (s *pipelinePrecheckScope) requirements() []repo.PrecheckRequirement {
	lww := len(s.rules.Target.ConflictPolicy.GetLwwTables()) > 0 && s.rules.Target.ConflictPolicy.GetMode() == iproto.ConflictMode_CONFLICT_MODE_LWW
	required := append(precheckNodeRequirements("source", s.source, lww), precheckNodeRequirements("target", s.target, lww)...)
	for _, code := range []string{"PRE-09", "PRE-10", "PRE-11", "PRE-12"} {
		required = append(required, repo.PrecheckRequirement{Item: "manager/" + code, Level: model.PrecheckLevelBlocking})
	}
	for _, code := range []string{"PRE-20", "PRE-24"} {
		required = append(required, repo.PrecheckRequirement{Item: "manager/" + code, Level: model.PrecheckLevelWarning})
	}
	if lww {
		required = append(required, repo.PrecheckRequirement{Item: "manager/PRE-22", Level: model.PrecheckLevelWarning})
	}
	if s.rules.Target.ConflictPolicy.GetMode() == iproto.ConflictMode_CONFLICT_MODE_LWW {
		required = append(required, repo.PrecheckRequirement{Item: "manager/PRE-26", Level: model.PrecheckLevelWarning})
	}
	if s.source.GetSnapshot() {
		required = append(required, repo.PrecheckRequirement{Item: "manager/PRE-23", Level: model.PrecheckLevelWarning})
	}
	if s.source.GetSchemaMigration() {
		for _, code := range []string{"PRE-13", "PRE-14"} {
			required = append(required, repo.PrecheckRequirement{Item: "manager/" + code, Level: model.PrecheckLevelBlocking})
		}
	}
	return required
}

func (l *InspectionLoop) loadPrecheckScope(ctx context.Context, p model.Pipeline) (*pipelinePrecheckScope, error) {
	se, sn, err := l.loadManualProbeTargets(ctx, p.SourceEndpointID, p.SourceNodeID)
	if err != nil {
		return nil, err
	}
	te, tn, err := l.loadManualProbeTargets(ctx, p.TargetEndpointID, p.TargetNodeID)
	if err != nil {
		return nil, err
	}
	version, err := repo.GetStrategyVersion(ctx, l.db, p.StrategyID, p.StrategyVersion)
	if err != nil {
		return nil, err
	}
	rules, err := pipelinespec.ParseStrategyRules(version.Rules)
	if err != nil || pipelinespec.ValidateObjectFilterRules(rules) != nil || pipelinespec.ValidateColumnFilterRules(rules) != nil || pipelinespec.ValidateRowFilterRules(rules) != nil || pipelinespec.ValidateTransformRules(rules) != nil || pipelinespec.ValidateMappingRules(rules) != nil || pipelinespec.ValidateDDLPolicyRules(rules) != nil {
		return nil, ErrPipelinePrecheckInput
	}
	if rules.Source == nil {
		rules.Source = &pipelinespec.StrategySourceRules{}
	}
	if rules.Target == nil {
		rules.Target = &pipelinespec.StrategyTargetRules{}
	}
	replicaEndpoint, err := pipelinespec.ResolveSnapshotReplicaEndpoint(ctx, l.db, &p, se, rules.Source.Snapshot)
	if err != nil {
		return nil, ErrPipelinePrecheckInput
	}
	if replicaEndpoint != nil && (replicaEndpoint.GetTls().GetEnabled() || len(replicaEndpoint.GetParams()) != 0) {
		return nil, ErrPipelinePrecheckInput
	}
	if p.BinlogServerID == nil || *p.BinlogServerID == 0 || !p.InitMode.Valid() || p.ReinitPending {
		return nil, ErrPipelinePrecheckInput
	}
	for _, ep := range []*model.Endpoint{se, te} {
		knownCase := ep.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String() || ep.LowerCaseTableNames == mysqlx.LowerCaseTableNamesLowercaseStored.String() || ep.LowerCaseTableNames == mysqlx.LowerCaseTableNamesLowercaseCompare.String()
		if _, err := mysqlx.ParseVersion(ep.Version); err != nil || !knownCase {
			return nil, ErrCatalogNotProbed
		}
		// These fields are not yet consumed by the shared business DSN builder.
		// Reject configured transport options before opening a plaintext connection.
		for _, blob := range []model.JSON{ep.TLS, ep.Params} {
			if len(blob) > 0 && string(blob) != "null" && string(blob) != "{}" {
				return nil, ErrPipelinePrecheckInput
			}
		}
	}
	mapping, err := schemaplan.MappingFromProtocol(rules.Target.Mappings)
	if err != nil {
		return nil, ErrPipelinePrecheckInput
	}
	if len(rules.Source.ColumnFilters) != 0 {
		mapping.ColumnFilter, err = iproto.CompileColumnFilters(rules.Source.ColumnFilters, se.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String())
		if err != nil {
			return nil, ErrPipelinePrecheckInput
		}
	}
	snapshot := p.InitMode == model.InitModeFullAndIncremental || p.InitMode == model.InitModeFullOnly
	var resumeChunkLimit uint64
	if snapshot && p.SnapshotUUID.IsZero() {
		return nil, ErrPipelinePrecheckInput
	}
	if snapshot {
		run, err := repo.GetPipelineSnapshotRun(ctx, l.db, p.ID, p.RunID)
		if err != nil && !errors.Is(err, repo.ErrSnapshotNotFound) {
			return nil, err
		}
		if err == nil {
			if run.SnapshotUUID != p.SnapshotUUID {
				return nil, ErrPipelinePrecheckInput
			}
			// Normal START resumes this run's saved checkpoints. A converged
			// snapshot is not initialized again; PRE-25's empty-history guard
			// and initial-full capacity checks would reject its expected GTIDs.
			snapshot = run.Status != model.SnapshotStatusConverged
			if snapshot && p.LastCmdSeq > 0 && p.DesiredState == model.DesiredStateStopped && p.SourceActualState == model.ActualStateStopped && p.TargetActualState == model.ActualStateStopped {
				capable := true
				for _, node := range []*model.Node{sn, tn} {
					var capabilities []string
					capable = capable && json.Unmarshal(node.Capabilities, &capabilities) == nil && slices.Contains(capabilities, iproto.CapabilitySnapshotResumeProof)
				}
				if capable {
					if run.TotalChunks > 0 && run.TotalChunks <= 1<<63-2 {
						resumeChunkLimit = run.TotalChunks
					}
					bundle, err := repo.GetPipelineStateBundle(ctx, l.db, &p, model.SideSource)
					if err != nil {
						return nil, err
					}
					if bundle != nil {
						resumeChunkLimit, err = iproto.SnapshotStateChunkLimit(bundle)
						if err != nil {
							return nil, err
						}
					}
				}
			}
		}
	}
	migration := !p.SchemaUUID.IsZero() || p.InitMode == model.InitModeSchemaOnly
	ddl := rules.Source.OperationFilter == nil || rules.Source.OperationFilter.GetDdl()
	ddl = ddl && rules.Target.DDLPolicy.GetMode() != iproto.DDLMode_DDL_MODE_IGNORE
	source := &iproto.PrecheckProbeArgs{Snapshot: snapshot, SnapshotReplica: replicaEndpoint, SchemaMigration: migration, SyncDdl: ddl, BinlogServerId: *p.BinlogServerID, StartKind: iproto.InitPositionKind_INIT_POSITION_KIND_LATEST}
	if snapshot {
		source.SnapshotRowsPerSecond = rules.Source.Snapshot.GetRateLimit().GetRowsPerSec()
		source.SnapshotBytesPerSecond = rules.Source.Snapshot.GetRateLimit().GetBytesPerSec()
	}
	target := &iproto.PrecheckProbeArgs{Snapshot: snapshot, SchemaMigration: migration, SyncDdl: ddl}
	source.PersistentLineageV1 = pipelinespec.SupportsPersistentLineage(sn) && pipelinespec.SupportsPersistentLineage(tn) && se.ServerUUID != "" && te.ServerUUID != ""
	target.PersistentLineageV1 = source.PersistentLineageV1
	lww := rules.Target.ConflictPolicy.GetMode() == iproto.ConflictMode_CONFLICT_MODE_LWW && len(rules.Target.ConflictPolicy.GetLwwTables()) > 0
	source.CheckLww, target.CheckLww = lww, lww
	if snapshot {
		target.SnapshotUuid = p.SnapshotUUID.String()
		target.SnapshotResumeChunkLimitV1 = resumeChunkLimit
	}
	var confirmedPlan *repo.StoredSchemaPlan
	if migration {
		plan, err := repo.GetSchemaPlan(ctx, l.db, p.ID, p.RunID)
		if err == nil && plan.Record.ConfirmedAt != nil && !plan.Plan.Blocked && !p.SchemaUUID.IsZero() && plan.Record.ConfigVersion < p.ConfigVersion {
			confirmedPlan = plan
			target.AllowMissingTargetTables = true
			for _, step := range plan.Plan.Steps {
				target.CreatesFunctions = target.CreatesFunctions || step.Kind == model.ObjectTypeFunction && step.Action == model.MigrationActionCreate
			}
		}
	}
	if p.PositionRevision > 0 || p.InitMode == model.InitModeIncrementalOnlyGTIDSet || p.InitMode == model.InitModeIncrementalOnlyBinlogPos || p.InitMode == model.InitModeIncrementalOnlyTimestamp {
		var pos iproto.StartPosition
		if len(p.StartPosition) == 0 || protojson.Unmarshal(p.StartPosition, &pos) != nil {
			return nil, ErrPipelinePrecheckInput
		}
		source.StartKind = iproto.InitPositionKind_INIT_POSITION_KIND_GTID_SET
		source.StartGtidSet = pos.GetGtidSet()
		if p.InitMode == model.InitModeIncrementalOnlyBinlogPos && p.PositionRevision == 0 {
			source.StartKind = iproto.InitPositionKind_INIT_POSITION_KIND_BINLOG_POS
		}
		if p.InitMode == model.InitModeIncrementalOnlyTimestamp {
			if err := repo.CheckTimestampStart(ctx, l.db, &p); err != nil {
				return nil, err
			}
		}
		if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, source.StartGtidSet); err != nil || len(source.StartGtidSet) > 1<<20 {
			return nil, ErrPipelinePrecheckInput
		}
	}
	scope := &pipelinePrecheckScope{source: source, target: target, rules: rules, mapping: mapping, sourceEndpoint: *se, targetEndpoint: *te, sourceNode: *sn, targetNode: *tn}
	scope.plan = confirmedPlan
	scope.required = scope.requirements()
	return scope, nil
}

func nodePrecheckItems(side string, required []repo.PrecheckRequirement, result *iproto.PrecheckResult, probeErr error) []model.PipelinePrecheck {
	expected := map[string]model.PrecheckLevel{}
	for _, item := range required {
		if after, ok := strings.CutPrefix(item.Item, side+"/"); ok {
			expected[after] = item.Level
		}
	}
	valid := probeErr == nil && result != nil && len(result.GetItems()) == len(expected)
	seen := map[string]*iproto.PrecheckItem{}
	total := 0
	for _, item := range result.GetItems() {
		level, ok := expected[item.GetCode()]
		severity := iproto.PrecheckSeverity_PRECHECK_SEVERITY_BLOCKING
		if level == model.PrecheckLevelWarning {
			severity = iproto.PrecheckSeverity_PRECHECK_SEVERITY_WARNING
		}
		total += len(item.GetDetailJson()) + len(item.GetMessage())
		if item == nil || !ok || seen[item.GetCode()] != nil || item.GetSeverity() != severity || total > 4<<20 || len(item.GetDetailJson()) > 0 && !json.Valid(item.GetDetailJson()) {
			valid = false
		}
		seen[item.GetCode()] = item
	}
	out := []model.PipelinePrecheck{}
	for _, r := range required {
		if !strings.HasPrefix(r.Item, side+"/") {
			continue
		}
		status := model.PrecheckStatusFail
		detail := model.JSON(`{"message":"节点未提供完整且可信的本侧检查结果","code":"node_result_unverified"}`)
		if valid {
			item := seen[strings.TrimPrefix(r.Item, side+"/")]
			if item.GetPassed() {
				status = model.PrecheckStatusPass
			}
			blob, _ := json.Marshal(struct {
				Message  string          `json:"message"`
				Evidence json.RawMessage `json:"evidence,omitempty"`
			}{item.GetMessage(), item.GetDetailJson()})
			detail = model.JSON(blob)
		}
		out = append(out, model.PipelinePrecheck{Item: r.Item, Level: r.Level, Status: status, Detail: detail})
	}
	return out
}

func (l *InspectionLoop) assignedPrecheckCatalog(ctx context.Context, ep model.Endpoint, node model.Node) (*iproto.CatalogResult, error) {
	d := l.dispatcherSnapshot()
	if d == nil {
		return nil, ErrProbeUnavailable
	}
	result, err := d.DispatchCatalog(ctx, ep, node, uuidx.New().String())
	if err != nil || !result.GetSuccess() || result.GetCatalog() == nil {
		return nil, ErrCatalogFailed
	}
	return result.GetCatalog(), nil
}
