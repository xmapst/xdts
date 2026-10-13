package task

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/xmapst/xdts/internal/datacheck"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/model"
	"github.com/xmapst/xdts/internal/manager/pipelinespec"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/internal/manager/schemaplan"
	"github.com/xmapst/xdts/internal/mysqlx"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xdts/pkg/uuidx"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrDataCheckNodeUnsupported       = errors.New("执行节点需要升级并声明 data_check_v1 能力")
	ErrDataCheckConnectionUnsupported = errors.New("数据校验暂不支持端点的 TLS 或自定义连接参数")
)

type iDataCheckDispatcher interface {
	DispatchDataCheck(context.Context, model.Endpoint, model.Node, string, uuidx.UUID, string, datacheck.LocalTable, uint64) (datacheck.DigestResult, error)
}

func (d *RPCProbeDispatcher) DispatchDataCheck(ctx context.Context, ep model.Endpoint, node model.Node, center string, pipeline uuidx.UUID, taskID string, table datacheck.LocalTable, sample uint64) (datacheck.DigestResult, error) {
	conn := probeEndpointConn(ep)
	// Refuse options that the current Node probe connection cannot execute.
	if len(ep.TLS) != 0 && string(ep.TLS) != "null" && string(ep.TLS) != "{}" || len(ep.Params) != 0 && string(ep.Params) != "null" && string(ep.Params) != "{}" {
		return datacheck.DigestResult{}, ErrDataCheckConnectionUnsupported
	}
	result, err := d.callProbe(ctx, &iproto.ProbeRequest{TaskId: taskID, Type: iproto.ProbeType_PROBE_TYPE_DATA_CHECK, PipelineId: pipeline.String(), CenterCode: center, Endpoint: conn, DataCheck: &iproto.DataCheckProbeArgs{Schema: table.Schema, Table: table.Table, Columns: table.Columns, SampleModulo: sample}}, node, taskID)
	if err != nil {
		return datacheck.DigestResult{}, errors.New("校验请求未能完成可信往返，请检查节点连接")
	}
	if !result.Success {
		return datacheck.DigestResult{}, errors.New(result.GetError().GetMessage())
	}
	var digest datacheck.DigestResult
	if len(result.GetDataCheck().GetResultJson()) > 4<<20 || json.Unmarshal(result.GetDataCheck().GetResultJson(), &digest) != nil || digest.Validate(len(table.Columns)) != nil || digest.SampleModulo != sample {
		return digest, errors.New("校验节点返回了不合法摘要")
	}
	return digest, nil
}

func requireDataCheckNode(ep *model.Endpoint, node *model.Node) error {
	if node.CenterID != ep.CenterID || node.Status != model.NodeStatusOnline {
		return ErrProbeNodeOffline
	}
	var capabilities []string
	if json.Unmarshal(node.Capabilities, &capabilities) != nil || iproto.ValidateNodeCapabilities(capabilities) != nil || !slices.Contains(capabilities, iproto.CapabilityDataCheck) {
		return ErrDataCheckNodeUnsupported
	}
	return nil
}

func validateDataCheckScope(p *model.Pipeline, source model.Endpoint, doc *pipelinespec.StrategyRulesDoc, config datacheck.Config) error {
	sensitive := source.LowerCaseTableNames == mysqlx.LowerCaseTableNamesSensitive.String()
	if doc.Source == nil {
		doc.Source = &pipelinespec.StrategySourceRules{}
	}
	if doc.Target == nil {
		doc.Target = &pipelinespec.StrategyTargetRules{}
	}
	var mapping schemamig.Mapping
	if doc.Target.Mappings != nil {
		var err error
		mapping, err = schemaplan.MappingFromProtocol(doc.Target.Mappings)
		if err != nil {
			return err
		}
	}
	for _, table := range config.Tables {
		selected, err := schemaplan.SelectDefinitions([]schemaplan.Definition{{Schema: table.SourceSchema, Name: table.SourceTable, Kind: model.ObjectTypeTable}}, doc.Source.ObjectFilters, sensitive)
		if err != nil || len(selected) != 1 {
			return datacheck.ErrConfig
		}
		names := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			names[i] = col.Source
		}
		schema, name, columns := mapping.TargetNames(table.SourceSchema, table.SourceTable, names, sensitive)
		if schema != table.TargetSchema || name != table.TargetTable {
			return datacheck.ErrConfig
		}
		for i, col := range table.Columns {
			if col.Target != columns[i] {
				return datacheck.ErrConfig
			}
		}
	}
	return nil
}

func (l *InspectionLoop) SubmitDataCheck(ctx context.Context, pipeline, user uuidx.UUID, config datacheck.Config, sample uint64) (*model.DataCheckRun, error) {
	if l == nil || l.db == nil {
		return nil, ErrProbeUnavailable
	}
	if _, ok := l.dispatcherSnapshot().(iDataCheckDispatcher); !ok {
		return nil, ErrProbeUnavailable
	}
	if config.Validate() != nil || len(config.Tables) > 20 {
		return nil, datacheck.ErrConfig
	}
	var run *model.DataCheckRun
	err := l.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var locked model.Pipeline
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("id=?", pipeline).First(&locked).Error; err != nil {
			return err
		}
		p, err := repo.GetPipeline(ctx, tx, pipeline)
		if err != nil {
			return err
		}
		for _, role := range []struct{ ep, node uuidx.UUID }{{p.SourceEndpointID, p.SourceNodeID}, {p.TargetEndpointID, p.TargetNodeID}} {
			ep, err := repo.GetEndpoint(ctx, tx, role.ep)
			if err != nil {
				return err
			}
			node, err := repo.GetNode(ctx, tx, role.node)
			if err != nil {
				return err
			}
			if err := requireDataCheckNode(ep, node); err != nil {
				return err
			}
		}
		version, err := repo.GetStrategyVersion(ctx, tx, p.StrategyID, p.StrategyVersion)
		if err != nil {
			return err
		}
		doc, err := pipelinespec.ParseStrategyRules(version.Rules)
		if err != nil {
			return err
		}
		source, err := repo.GetEndpoint(ctx, tx, p.SourceEndpointID)
		if err != nil {
			return err
		}
		if err := validateDataCheckScope(p, *source, doc, config); err != nil {
			return err
		}
		run, err = repo.CreateDataCheck(ctx, tx, pipeline, user, config, sample)
		return err
	})
	return run, err
}

func (l *InspectionLoop) runDataChecks(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if _, ok := l.dispatcherSnapshot().(iDataCheckDispatcher); ok {
			run, err := repo.ClaimDataCheck(ctx, l.db)
			if err != nil && ctx.Err() == nil {
				slog.Warn("data check claim failed", slog.Any("error", err))
			}
			if run != nil {
				l.executeDataCheck(ctx, *run)
				continue
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *InspectionLoop) executeDataCheck(parent context.Context, run model.DataCheckRun) {
	ctx, cancel := context.WithDeadline(parent, run.ExpiresAt)
	defer cancel()
	done := make(chan struct{})
	defer close(done)
	// Cancellation state is durable. This watcher only signals the job owner.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				stored, err := repo.GetDataCheck(ctx, l.db, run.PipelineID, run.ID)
				if err != nil || stored.Status != "running" {
					cancel()
					return
				}
			}
		}
	}()
	results := []datacheck.DigestComparison{}
	finish := func(status, message string) {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := repo.UpdateDataCheck(cleanupCtx, l.db, run.ID, results, status, message); err != nil {
			slog.Warn("data check result persistence failed", slog.String("task_id", run.ID.String()), slog.Any("error", err))
		}
	}
	if err := l.collectDataCheck(ctx, run, &results); err != nil {
		status := "failed"
		if parent.Err() != nil {
			status = "interrupted"
		}
		finish(status, err.Error())
		return
	}
	status := "equal"
	for _, table := range results {
		if !table.Equal {
			status = "different"
			break
		}
	}
	finish(status, "")
}

func (l *InspectionLoop) collectDataCheck(ctx context.Context, run model.DataCheckRun, results *[]datacheck.DigestComparison) error {
	p, err := repo.GetPipeline(ctx, l.db, run.PipelineID)
	if err != nil {
		return errors.New("校验线路已不可用")
	}
	if p.ConfigVersion != run.ConfigVersion {
		return errors.New("校验开始前规则版本已变化，请重新发起")
	}
	dispatcher, ok := l.dispatcherSnapshot().(iDataCheckDispatcher)
	if !ok {
		return ErrProbeUnavailable
	}
	var config datacheck.Config
	if json.Unmarshal(run.Config, &config) != nil {
		return datacheck.ErrConfig
	}
	type roleInput struct {
		ep     model.Endpoint
		node   model.Node
		center string
	}
	inputs := make([]roleInput, 2)
	for i, role := range []struct{ ep, node uuidx.UUID }{{p.SourceEndpointID, p.SourceNodeID}, {p.TargetEndpointID, p.TargetNodeID}} {
		ep, err := repo.GetEndpoint(ctx, l.db, role.ep)
		if err != nil {
			return errors.New("校验端点已不可用")
		}
		node, err := repo.GetNode(ctx, l.db, role.node)
		if err != nil {
			return errors.New("校验节点已不可用")
		}
		if err := requireDataCheckNode(ep, node); err != nil {
			return err
		}
		center, err := repo.GetCenter(ctx, l.db, ep.CenterID)
		if err != nil {
			return err
		}
		inputs[i] = roleInput{ep: *ep, node: *node, center: center.Code}
	}
	for index, table := range config.Tables {
		current, err := repo.GetPipeline(ctx, l.db, run.PipelineID)
		if err != nil || current.ConfigVersion != run.ConfigVersion {
			return errors.New("校验期间线路规则或分配发生变化，请重新发起")
		}
		names := [][]string{make([]string, len(table.Columns)), make([]string, len(table.Columns))}
		for i, col := range table.Columns {
			names[0][i] = col.Source
			names[1][i] = col.Target
		}
		locals := []datacheck.LocalTable{{Schema: table.SourceSchema, Table: table.SourceTable, Columns: names[0]}, {Schema: table.TargetSchema, Table: table.TargetTable, Columns: names[1]}}
		type reply struct {
			side   int
			result datacheck.DigestResult
			err    error
		}
		replies := make(chan reply, 2)
		for side := range 2 {
			input, local := inputs[side], locals[side]
			go func() {
				result, err := dispatcher.DispatchDataCheck(ctx, input.ep, input.node, input.center, run.PipelineID, uuidx.New().String(), local, run.SampleModulo)
				replies <- reply{side: side, result: result, err: err}
			}()
		}
		var digests [2]datacheck.DigestResult
		for range 2 {
			select {
			case <-ctx.Done():
				return errors.New("校验已取消或超过任务时间限制")
			case response := <-replies:
				if response.err != nil {
					return fmt.Errorf("第 %d 张表：%w", index+1, response.err)
				}
				digests[response.side] = response.result
			}
		}
		comparison, err := datacheck.CompareDigests(table, digests[0], digests[1])
		if err != nil {
			return fmt.Errorf("第 %d 张表：%w", index+1, err)
		}
		*results = append(*results, comparison)
		if err := repo.UpdateDataCheck(ctx, l.db, run.ID, *results, "running", ""); err != nil {
			return errors.New("校验进度保存失败")
		}
	}
	current, err := repo.GetPipeline(ctx, l.db, run.PipelineID)
	if err != nil || current.ConfigVersion != run.ConfigVersion {
		return errors.New("校验期间线路规则或分配发生变化，请重新发起")
	}
	return nil
}
