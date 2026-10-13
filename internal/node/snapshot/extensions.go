package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/node/store"
)

// Extend keeps a proposal until its insert is acknowledged. Before querying
// fresh bounds it adopts any previously committed prefix, including a write
// whose acknowledgement was lost. Existing plans never change identity.
func (p *Planner) Extend(ctx context.Context, tables []Table, chunkRows uint64, reader IBoundaryReader) (Plan, bool, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, false, err
	}
	if p.proposal == nil || !p.committed {
		return Plan{}, false, fmt.Errorf("snapshot: commit initial plan before extending")
	}
	before := p.proposal.Total
	if err := p.loadExtensions(ctx); err != nil {
		return Plan{}, false, err
	}
	if err := matchInputs(*p.proposal, tables, chunkRows); err != nil {
		return Plan{}, false, err
	}
	if p.proposal.Total != before {
		p.pendingExtension = nil
		return p.proposal.clone(), true, nil
	}
	if p.reinitDigest != "" {
		return p.proposal.clone(), false, nil
	}
	if p.pendingExtension == nil {
		existing := make(map[[2]string]bool, len(p.proposal.Tables))
		for _, table := range p.proposal.Tables {
			existing[[2]string{table.Schema, table.Table}] = true
		}
		ext := PlanExtension{Version: planVersion, PipelineID: p.pipelineID, RunID: p.runID, Index: p.extensions + 1, Total: before}
		for _, table := range tables {
			if existing[[2]string{table.Schema, table.Name}] {
				continue
			}
			planned, err := PlanTable(ctx, table, chunkRows, reader)
			if err != nil {
				return Plan{}, false, err
			}
			if planned.ChunkCount > maxChunkNumber-ext.Total {
				return Plan{}, false, fmt.Errorf("snapshot: global chunk number exceeds MySQL GTID range")
			}
			planned.FirstChunk = ext.Total + 1
			ext.Total += planned.ChunkCount
			ext.Tables = append(ext.Tables, planned)
		}
		if len(ext.Tables) == 0 {
			return p.proposal.clone(), false, nil
		}
		if _, err := p.applyExtension(ext, *p.proposal); err != nil {
			return Plan{}, false, err
		}
		p.pendingExtension = &ext
	}
	// A pending proposal must still match the source identity on retry. Its
	// immutable boundaries remain authoritative even if row estimates change.
	candidate, err := p.applyExtension(*p.pendingExtension, *p.proposal)
	if err != nil {
		return Plan{}, false, err
	}
	if err := matchInputs(candidate, tables, chunkRows); err != nil {
		return Plan{}, false, err
	}
	ext := *p.pendingExtension
	ext.Tables = cloneTablePlans(ext.Tables)
	writeCtx, cancel := context.WithTimeout(ctx, p.operationTimeout)
	err = p.store.PutCritical(writeCtx, store.Op{Collection: "snapshot_chunks", ID: p.extensionID(ext.Index), Doc: ext, Critical: true, InsertOnly: true})
	cancel()
	if err != nil && !errors.Is(err, store.ErrAlreadyExists) {
		return Plan{}, false, fmt.Errorf("snapshot: commit chunk plan extension failed: %w", err)
	}
	// Read back even on successful insert; adopt the durable winner only.
	if err := p.loadExtensions(ctx); err != nil {
		return Plan{}, false, err
	}
	if p.extensions < ext.Index {
		return Plan{}, false, fmt.Errorf("snapshot: committed extension missing after acknowledgement")
	}
	if err := matchInputs(*p.proposal, tables, chunkRows); err != nil {
		return Plan{}, false, err
	}
	p.pendingExtension = nil
	return p.proposal.clone(), true, nil
}

func cloneTablePlans(in []TablePlan) []TablePlan {
	out := make([]TablePlan, len(in))
	for i := range in {
		out[i] = in[i].clone()
	}
	return out
}

func (p *Planner) extensionID(index uint64) string {
	return fmt.Sprintf("%s.extension.%d", p.documentID(), index)
}

func (p *Planner) loadExtensions(ctx context.Context) error {
	for {
		index := p.extensions + 1
		var ext PlanExtension
		readCtx, cancel := context.WithTimeout(ctx, p.operationTimeout)
		err := p.store.Get(readCtx, "snapshot_chunks", p.extensionID(index), &ext)
		cancel()
		if errors.Is(err, xdoc.ErrNotFound) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("snapshot: load chunk plan extension failed: %w", err)
		}
		candidate, err := p.applyExtension(ext, *p.proposal)
		if err != nil {
			return err
		}
		p.proposal, p.extensions = &candidate, index
	}
}

func (p *Planner) applyExtension(ext PlanExtension, current Plan) (Plan, error) {
	if ext.Version != planVersion || ext.PipelineID != p.pipelineID || ext.RunID != p.runID || ext.Index != p.extensions+1 || len(ext.Tables) == 0 || ext.Total <= current.Total || ext.Total > maxChunkNumber || p.reinitDigest != "" {
		return Plan{}, fmt.Errorf("snapshot: invalid stored chunk plan extension")
	}
	candidate := current.clone()
	candidate.Tables = append(candidate.Tables, cloneTablePlans(ext.Tables)...)
	candidate.Total = ext.Total
	if err := p.validate(candidate); err != nil {
		return Plan{}, err
	}
	return candidate, nil
}
