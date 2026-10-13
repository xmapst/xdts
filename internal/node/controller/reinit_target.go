package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/mysqlout"
	"github.com/xmapst/xdts/internal/node/store"
	"google.golang.org/protobuf/proto"
)

type reinitPreparation struct {
	PipelineID string `bson:"pipeline_id"`
	RunID      uint64 `bson:"run_id"`
	Digest     string `bson:"digest"`
	PlanHash   string `bson:"plan_hash"`
	Complete   bool   `bson:"complete"`
}

func (l *prodLoader) prepareReinitTarget(ctx context.Context, item *iproto.PipelineConfigItem, opts mysqlout.Options) error {
	plan := item.GetTarget().GetReinitPlan()
	if plan == nil {
		return nil
	}
	identity := item.GetCommon().GetReinitIdentity()
	if l.nc.Store == nil || !validReinitIdentity(identity, item.GetCommon().GetRunId()) || identity.GetDigest() != plan.GetDigest() {
		return fmt.Errorf("controller: invalid target preparation identity")
	}
	blob, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(blob)
	record := reinitPreparation{PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(), Digest: identity.GetDigest(), PlanHash: hex.EncodeToString(hash[:])}
	key := fmt.Sprintf("%d:%s:%d.reinit", len(record.PipelineID), record.PipelineID, record.RunID)
	opctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var prior reinitPreparation
	err = l.nc.Store.Get(opctx, "schema_migrations", key, &prior)
	if err == nil {
		if prior.PipelineID != record.PipelineID || prior.RunID != record.RunID || prior.Digest != record.Digest || prior.PlanHash != record.PlanHash {
			return fmt.Errorf("controller: stored target preparation changed")
		}
		if prior.Complete {
			return nil
		}
	} else if errors.Is(err, xdoc.ErrNotFound) {
		if err := l.nc.Store.PutCritical(opctx, store.Op{Collection: "schema_migrations", ID: key, Doc: record, InsertOnly: true}); err != nil {
			return err
		}
	} else {
		return err
	}
	opts.WritePolicy = nil
	writer, err := mysqlout.New(opctx, opts)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(writer.Close)
	if err := writer.ApplyReinitPlan(opctx, proto.Clone(plan).(*iproto.ReinitTargetPlan), item.GetTarget().GetSchemaPlan()); err != nil {
		return err
	}
	record.Complete = true
	return l.nc.Store.PutCritical(opctx, store.Op{Collection: "schema_migrations", ID: key, Doc: record})
}
