package snapshot

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/node/store"
)

// Planner is owned by one source workflow, outside the actor event loop. A
// pending proposal is retained across failed critical writes: the commit may
// have happened even if the caller timed out before receiving its acknowledgement.
type Planner struct {
	store            IStore
	pipelineID       string
	runID            string
	proposal         *Plan
	committed        bool
	reinitDigest     string
	extensions       uint64
	initialTotal     uint64
	pendingExtension *PlanExtension
	operationTimeout time.Duration
}

func (p *Planner) load(ctx context.Context) (bool, error) {
	var loaded Plan
	err := p.store.Get(ctx, "snapshot_chunks", p.documentID(), &loaded)
	if errors.Is(err, xdoc.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("snapshot: load chunk plan failed: %w", err)
	}
	if err := p.validate(loaded); err != nil {
		return false, err
	}
	p.proposal, p.committed = &loaded, true
	p.initialTotal, p.extensions = loaded.Total, 0
	if err := p.loadExtensions(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func NewPlanner(storage IStore, pipelineID, runID string) (*Planner, error) {
	if storage == nil || pipelineID == "" || runID == "" {
		return nil, fmt.Errorf("snapshot: store, pipeline and run required")
	}
	return &Planner{store: storage, pipelineID: pipelineID, runID: runID, operationTimeout: DefaultOperationTimeout}, nil
}

// Plan returns only a committed immutable plan. A restart reads this plan before
// requesting any boundaries, so rows changing in the source cannot renumber
// chunks already published using the synthetic GTID.
func (p *Planner) Plan(ctx context.Context, tables []Table, chunkRows uint64, reader IBoundaryReader) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	if chunkRows == 0 || len(tables) == 0 {
		return Plan{}, fmt.Errorf("snapshot: tables and chunk rows required")
	}
	if p.proposal == nil {
		var loaded Plan
		err := p.store.Get(ctx, "snapshot_chunks", p.documentID(), &loaded)
		switch {
		case err == nil:
			if err := p.validate(loaded); err != nil {
				return Plan{}, err
			}
			p.proposal, p.committed = &loaded, true
			p.initialTotal, p.extensions = loaded.Total, 0
			if err := p.loadExtensions(ctx); err != nil {
				return Plan{}, err
			}
		case errors.Is(err, xdoc.ErrNotFound):
			if err := validateTables(tables); err != nil {
				return Plan{}, err
			}
			proposal := Plan{Version: planVersion, PipelineID: p.pipelineID, RunID: p.runID, ChunkRows: chunkRows, ReinitDigest: p.reinitDigest}
			for _, table := range tables {
				planned, err := PlanTable(ctx, table, chunkRows, reader)
				if err != nil {
					return Plan{}, err
				}
				if planned.ChunkCount > maxChunkNumber-proposal.Total {
					return Plan{}, fmt.Errorf("snapshot: global chunk number exceeds MySQL GTID range")
				}
				planned.FirstChunk = proposal.Total + 1
				proposal.Total += planned.ChunkCount
				proposal.Tables = append(proposal.Tables, planned)
			}
			p.proposal = &proposal
			p.initialTotal = proposal.Total
		default:
			return Plan{}, fmt.Errorf("snapshot: load chunk plan failed: %w", err)
		}
	}
	if p.committed {
		// A previous load may have adopted a valid prefix before encountering
		// a corrupt later extension. Retrying must not silently use that prefix.
		if err := p.loadExtensions(ctx); err != nil {
			return Plan{}, err
		}
	}
	if err := matchInputs(*p.proposal, tables, chunkRows); err != nil {
		return Plan{}, err
	}
	if !p.committed {
		err := p.store.PutCritical(ctx, store.Op{
			Collection: "snapshot_chunks", ID: p.documentID(), Doc: p.proposal.clone(), Critical: true, InsertOnly: true,
		})
		if errors.Is(err, store.ErrAlreadyExists) {
			// An earlier timed-out write or replacement workflow won creation.
			// Adopt only the validated committed document, never overwrite it.
			var stored Plan
			if err := p.store.Get(ctx, "snapshot_chunks", p.documentID(), &stored); err != nil {
				return Plan{}, fmt.Errorf("snapshot: read existing chunk plan failed: %w", err)
			}
			if err := p.validate(stored); err != nil {
				return Plan{}, err
			}
			if err := matchInputs(stored, tables, chunkRows); err != nil {
				return Plan{}, err
			}
			p.proposal = &stored
			p.initialTotal, p.extensions = stored.Total, 0
			if err := p.loadExtensions(ctx); err != nil {
				return Plan{}, err
			}
		} else if err != nil {
			return Plan{}, fmt.Errorf("snapshot: commit chunk plan failed: %w", err)
		}
		p.committed = true
	}
	return p.proposal.clone(), nil
}

func (p *Planner) documentID() string {
	// Length prefix makes the two arbitrary identifiers unambiguous.
	return fmt.Sprintf("%d:%s:%s", len(p.pipelineID), p.pipelineID, p.runID)
}

func validateTables(tables []Table) error {
	seen := make(map[[2]string]bool)
	for _, table := range tables {
		if _, err := ChooseKey(table); err != nil {
			return err
		}
		name := [2]string{table.Schema, table.Name}
		if seen[name] {
			return fmt.Errorf("snapshot: duplicate table in plan")
		}
		seen[name] = true
	}
	return nil
}

func matchInputs(plan Plan, tables []Table, chunkRows uint64) error {
	if len(plan.Tables) > len(tables) || plan.ChunkRows != chunkRows {
		return fmt.Errorf("snapshot: plan inputs changed within run")
	}
	if err := validateTables(tables); err != nil {
		return err
	}
	byName := make(map[[2]string]Table, len(tables))
	for _, table := range tables {
		byName[[2]string{table.Schema, table.Name}] = table
	}
	for _, old := range plan.Tables {
		table, ok := byName[[2]string{old.Schema, old.Table}]
		if !ok {
			return fmt.Errorf("snapshot: planned table disappeared within run")
		}
		key, _ := ChooseKey(table)
		if old.Baseline != nil {
			if old.Baseline.Schema != old.Schema || old.Baseline.Name != old.Table {
				return fmt.Errorf("snapshot: stored baseline identity mismatch")
			}
			if _, err := snapshotProjection(*old.Baseline, table, old.Key); err != nil {
				return err
			}
		}
		if old.Schema != table.Schema || old.Table != table.Name || !reflect.DeepEqual(old.Key, key) {
			return fmt.Errorf("snapshot: table positioning key changed within run")
		}
	}
	return nil
}

func tablesForPlan(plan Plan, tables []Table) ([]Table, error) {
	byName := make(map[[2]string]Table, len(tables))
	for _, table := range tables {
		byName[[2]string{table.Schema, table.Name}] = table
	}
	ordered := make([]Table, 0, len(plan.Tables))
	for _, planned := range plan.Tables {
		table, ok := byName[[2]string{planned.Schema, planned.Table}]
		if !ok {
			return nil, fmt.Errorf("snapshot: planned table disappeared within run")
		}
		ordered = append(ordered, table)
	}
	return ordered, nil
}

func (p *Planner) validate(plan Plan) error {
	if plan.ReinitDigest != p.reinitDigest {
		return fmt.Errorf("snapshot: stored confirmed source identity changed")
	}
	if plan.Version != planVersion || plan.PipelineID != p.pipelineID || plan.RunID != p.runID ||
		plan.ChunkRows == 0 || plan.Total == 0 || plan.Total > maxChunkNumber || len(plan.Tables) == 0 {
		return fmt.Errorf("snapshot: invalid stored chunk plan identity")
	}
	var total uint64
	seen := make(map[[2]string]bool)
	for _, table := range plan.Tables {
		name := [2]string{table.Schema, table.Table}
		if !validIdentifier(table.Schema) || !validIdentifier(table.Table) || seen[name] ||
			table.FirstChunk != total+1 || table.ChunkCount == 0 || table.ChunkCount > maxChunkNumber-total {
			return fmt.Errorf("snapshot: invalid stored chunk numbering")
		}
		seen[name] = true
		if table.Baseline != nil {
			key, err := ChooseKey(*table.Baseline)
			if err != nil || table.Baseline.Schema != table.Schema || table.Baseline.Name != table.Table || !reflect.DeepEqual(key, table.Key) {
				return fmt.Errorf("snapshot: invalid stored baseline metadata")
			}
		}
		metadata := Table{Schema: table.Schema, Name: table.Table, Columns: table.Key.Columns,
			PrimaryKey: Index{Name: table.Key.Name}}
		for _, col := range table.Key.Columns {
			metadata.PrimaryKey.Columns = append(metadata.PrimaryKey.Columns, col.Name)
		}
		if _, err := ChooseKey(metadata); err != nil {
			return fmt.Errorf("snapshot: invalid stored positioning key")
		}
		switch table.Strategy {
		case StrategyInteger:
			if len(table.Key.Columns) != 1 || integerBits(table.Key.Columns[0].DataType) == 0 || len(table.Boundaries) != 0 {
				return fmt.Errorf("snapshot: invalid stored integer strategy")
			}
			if table.Minimum == "" && table.Maximum == "" && table.Step == "" && table.ChunkCount == 1 {
				break
			}
			lo, errLo := parseInteger(table.Minimum, table.Key.Columns[0])
			hi, errHi := parseInteger(table.Maximum, table.Key.Columns[0])
			step, ok := new(big.Int).SetString(table.Step, 10)
			if errLo != nil || errHi != nil || !ok || step.Sign() <= 0 || step.String() != table.Step || lo.Cmp(hi) > 0 {
				return fmt.Errorf("snapshot: invalid stored integer bounds")
			}
			width := new(big.Int).Sub(hi, lo)
			width.Add(width, big.NewInt(1))
			count := ceilDiv(width, step)
			if !count.IsUint64() || count.Uint64() != table.ChunkCount {
				return fmt.Errorf("snapshot: stored integer count disagrees with bounds")
			}
		case StrategyKeyset:
			if table.Minimum != "" || table.Maximum != "" || table.Step != "" || uint64(len(table.Boundaries))+1 != table.ChunkCount {
				return fmt.Errorf("snapshot: invalid stored keyset count")
			}
			var previous Boundary
			for _, bound := range table.Boundaries {
				if !validBoundary(bound, table.Key) || sameBoundary(previous, bound) {
					return fmt.Errorf("snapshot: invalid stored keyset boundary")
				}
				previous = bound
			}
		default:
			return fmt.Errorf("snapshot: invalid stored strategy")
		}
		total += table.ChunkCount
	}
	if total != plan.Total {
		return fmt.Errorf("snapshot: stored total disagrees with table plans")
	}
	return nil
}
