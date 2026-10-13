package schemamigrator

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/xmapst/xdoc"
	"google.golang.org/protobuf/proto"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/store"
)

type IStore interface {
	Get(context.Context, string, string, any) error
	PutCritical(context.Context, store.Op) error
}
type IApplier interface {
	ApplySchemaStep(context.Context, string, *iproto.SchemaMigrationStep) error
	Close() error
}
type Options struct {
	PipelineID       string
	RunID            uint64
	ConfigVersion    uint64
	Plan             *iproto.SchemaMigrationPlan
	AfterSnapshot    bool
	Store            IStore
	Applier          IApplier
	OperationTimeout time.Duration
	Report           func(uint64, error)
}
type planRecord struct {
	Version    int    `bson:"version"`
	PipelineID string `bson:"pipeline_id"`
	RunID      uint64 `bson:"run_id"`
	UUID       string `bson:"uuid"`
	Digest     string `bson:"digest"`
	Plan       []byte `bson:"plan"`
}
type stepRecord struct {
	Version    int    `bson:"version"`
	PipelineID string `bson:"pipeline_id"`
	RunID      uint64 `bson:"run_id"`
	UUID       string `bson:"uuid"`
	Digest     string `bson:"digest"`
	Seq        uint64 `bson:"seq"`
}

// Runner owns one background workflow. No Writer or plan memory is shared with
// incremental writes. The original plan is fixed before the first SQL executes.
type Runner struct {
	opts   Options
	record planRecord
}

func NewRunner(opts Options) (*Runner, error) {
	if opts.Plan == nil || opts.Store == nil || opts.Applier == nil || opts.PipelineID == "" || opts.OperationTimeout <= 0 || len(opts.Plan.GetSteps()) == 0 || len(opts.Plan.GetSteps()) > 4000 {
		return nil, fmt.Errorf("schemamigrator: invalid execution options")
	}
	id, err := uuid.Parse(opts.Plan.GetSchemaUuid())
	if err != nil || id == uuid.Nil() || id.String() != opts.Plan.GetSchemaUuid() {
		return nil, fmt.Errorf("schemamigrator: invalid structure UUID")
	}
	opts.Plan = proto.Clone(opts.Plan).(*iproto.SchemaMigrationPlan)
	for i, step := range opts.Plan.GetSteps() {
		if step.GetSeq() != uint64(i+1) || step.GetDdl() == "" || step.GetSchema() == "" || step.GetObjectName() == "" || (step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE && step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_SKIP) {
			return nil, fmt.Errorf("schemamigrator: invalid migration step")
		}
		if step.GetViewPlaceholder() {
			seq := step.GetViewReplacementSeq()
			if step.GetKind() != iproto.SchemaObjectKind_SCHEMA_OBJECT_KIND_VIEW || seq <= step.GetSeq() || seq > uint64(len(opts.Plan.GetSteps())) {
				return nil, fmt.Errorf("schemamigrator: invalid view replacement sequence")
			}
			final := opts.Plan.GetSteps()[seq-1]
			if step.GetAction() != iproto.SchemaMigrationAction_SCHEMA_MIGRATION_ACTION_CREATE || final.GetAction() != step.GetAction() || final.GetKind() != step.GetKind() || final.GetSchema() != step.GetSchema() || final.GetObjectName() != step.GetObjectName() || final.GetDdl() != step.GetViewReplacementDdl() || final.GetAfterSnapshot() != step.GetAfterSnapshot() || final.GetViewPlaceholder() || final.GetViewPlaceholderSeq() != step.GetSeq() || final.GetViewPlaceholderDdl() != step.GetDdl() {
				return nil, fmt.Errorf("schemamigrator: view replacement does not match immutable plan")
			}
		} else if step.GetViewReplacementSeq() != 0 || step.GetViewReplacementDdl() != "" {
			return nil, fmt.Errorf("schemamigrator: successor metadata without placeholder")
		}
		if step.GetViewPlaceholderSeq() != 0 || step.GetViewPlaceholderDdl() != "" {
			seq := step.GetViewPlaceholderSeq()
			if seq == 0 || seq >= step.GetSeq() || step.GetViewPlaceholder() {
				return nil, fmt.Errorf("schemamigrator: invalid view predecessor sequence")
			}
			prior := opts.Plan.GetSteps()[seq-1]
			if !prior.GetViewPlaceholder() || prior.GetViewReplacementSeq() != step.GetSeq() || prior.GetDdl() != step.GetViewPlaceholderDdl() {
				return nil, fmt.Errorf("schemamigrator: view predecessor does not match immutable plan")
			}
		}
	}
	blob, err := proto.MarshalOptions{Deterministic: true}.Marshal(opts.Plan)
	if err != nil {
		return nil, err
	}
	hash := sha256.Sum256(blob)
	return &Runner{opts: opts, record: planRecord{Version: 1, PipelineID: opts.PipelineID, RunID: opts.RunID, UUID: id.String(), Digest: hex.EncodeToString(hash[:]), Plan: blob}}, nil
}
func (r *Runner) recordID(suffix string) string {
	return fmt.Sprintf("%d:%s:%d.%s", len(r.opts.PipelineID), r.opts.PipelineID, r.opts.RunID, suffix)
}
func (r *Runner) Run(ctx context.Context) error {
	opctx, cancel := context.WithTimeout(ctx, r.opts.OperationTimeout)
	var prior planRecord
	err := r.opts.Store.Get(opctx, "schema_migrations", r.recordID("plan"), &prior)
	cancel()
	if errors.Is(err, xdoc.ErrNotFound) {
		opctx, cancel = context.WithTimeout(ctx, r.opts.OperationTimeout)
		err = r.opts.Store.PutCritical(opctx, store.Op{Collection: "schema_migrations", ID: r.recordID("plan"), Doc: r.record, InsertOnly: true})
		cancel()
		if errors.Is(err, store.ErrAlreadyExists) {
			return r.Run(ctx)
		}
	} else if err == nil && (prior.Version != 1 || prior.PipelineID != r.record.PipelineID || prior.RunID != r.record.RunID || prior.UUID != r.record.UUID || prior.Digest != r.record.Digest) {
		return fmt.Errorf("schemamigrator: stored plan identity changed")
	}
	if err != nil {
		return err
	}
	phaseID := r.recordID(fmt.Sprintf("phase.%t", r.opts.AfterSnapshot))
	var phase stepRecord
	opctx, cancel = context.WithTimeout(ctx, r.opts.OperationTimeout)
	err = r.opts.Store.Get(opctx, "schema_migrations", phaseID, &phase)
	cancel()
	if err == nil {
		if phase.Version != 1 || phase.PipelineID != r.record.PipelineID || phase.RunID != r.record.RunID || phase.UUID != r.record.UUID || phase.Digest != r.record.Digest {
			return fmt.Errorf("schemamigrator: stored phase identity changed")
		}
		if r.opts.Report != nil {
			for _, step := range r.opts.Plan.GetSteps() {
				if step.GetAfterSnapshot() == r.opts.AfterSnapshot {
					r.opts.Report(step.GetSeq(), nil)
				}
			}
		}
		return nil
	}
	if !errors.Is(err, xdoc.ErrNotFound) {
		return err
	}
	for _, step := range r.opts.Plan.GetSteps() {
		if step.GetAfterSnapshot() != r.opts.AfterSnapshot {
			continue
		}
		opctx, cancel = context.WithTimeout(ctx, r.opts.OperationTimeout)
		var saved stepRecord
		readErr := r.opts.Store.Get(opctx, "schema_migrations", r.recordID(fmt.Sprintf("step.%d", step.GetSeq())), &saved)
		if readErr != nil && !errors.Is(readErr, xdoc.ErrNotFound) {
			cancel()
			return readErr
		}
		if readErr == nil && (saved.Version != 1 || saved.PipelineID != r.record.PipelineID || saved.RunID != r.record.RunID || saved.UUID != r.record.UUID || saved.Digest != r.record.Digest || saved.Seq != step.GetSeq()) {
			cancel()
			return fmt.Errorf("schemamigrator: stored step identity changed")
		}
		// Recheck real MySQL even for locally completed steps. GTID idempotence is
		// the authority when a crash happens after SQL and before local persistence.
		err = r.opts.Applier.ApplySchemaStep(opctx, r.record.UUID, proto.Clone(step).(*iproto.SchemaMigrationStep))
		if err == nil && errors.Is(readErr, xdoc.ErrNotFound) {
			record := stepRecord{Version: 1, PipelineID: r.record.PipelineID, RunID: r.record.RunID, UUID: r.record.UUID, Digest: r.record.Digest, Seq: step.GetSeq()}
			err = r.opts.Store.PutCritical(opctx, store.Op{Collection: "schema_migrations", ID: r.recordID(fmt.Sprintf("step.%d", step.GetSeq())), Doc: record, InsertOnly: true})
		}
		cancel()
		if r.opts.Report != nil {
			r.opts.Report(step.GetSeq(), err)
		}
		if err != nil {
			return err
		}
	}
	opctx, cancel = context.WithTimeout(ctx, r.opts.OperationTimeout)
	defer cancel()
	return r.opts.Store.PutCritical(opctx, store.Op{Collection: "schema_migrations", ID: phaseID, Doc: stepRecord{Version: 1, PipelineID: r.record.PipelineID, RunID: r.record.RunID, UUID: r.record.UUID, Digest: r.record.Digest}, InsertOnly: true})
}
