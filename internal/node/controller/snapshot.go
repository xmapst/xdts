package controller

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/pipeline/amqpout"
	"github.com/xmapst/xdts/internal/node/pipeline/binlogin"
	"github.com/xmapst/xdts/internal/node/pipeline/snapreader"
	"github.com/xmapst/xdts/internal/node/reporter"
	"github.com/xmapst/xdts/internal/node/snapshot"
	"github.com/xmapst/xdts/internal/schemamig"
	"github.com/xmapst/xhive"
)

func needsSnapshot(item *iproto.PipelineConfigItem) bool {
	mode := item.GetSource().GetInitMode()
	return mode == iproto.InitMode_INIT_MODE_SNAPSHOT_AND_INCREMENTAL || mode == iproto.InitMode_INIT_MODE_SNAPSHOT_ONLY
}

// loadSnapshotSource is called from the loader's background workflow. SQL pool
// ownership transfers to snap_reader only once all modules were added; every
// startup failure rolls back modules and closes the pool.
func (l *prodLoader) loadSnapshotSource(ctx context.Context, item *iproto.PipelineConfigItem,
	binOpts binlogin.Options, outOpts amqpout.Options) error {
	src := item.GetSource()
	if identity := item.GetCommon().GetReinitIdentity(); identity != nil {
		if !validReinitIdentity(identity, item.GetCommon().GetRunId()) || src.GetReinitPlan() == nil || src.GetReinitPlan().GetDigest() != identity.GetDigest() {
			return fmt.Errorf("controller: confirmed source scope missing or changed")
		}
	}
	params := src.GetSnapshot()
	if params == nil {
		return fmt.Errorf("controller: snapshot parameters required")
	}
	if err := iproto.ValidateSnapshotReplicaPolicy(params); err != nil {
		return err
	}
	readEndpoint := src.GetEndpoint()
	readUUID := ""
	if params.GetReadFromReplica() {
		if src.GetSnapshotEndpoint() == nil {
			return fmt.Errorf("controller: replica snapshot requires a separate replica endpoint")
		}
		readEndpoint = src.GetSnapshotEndpoint()
		readUUID = readEndpoint.GetServerUuid()
	} else if src.GetSnapshotEndpoint() != nil {
		return fmt.Errorf("controller: replica credentials without replica snapshot mode")
	}
	opts := snapshot.ReaderOptions{Budget: l.nc.Quota.Budget(item.GetPipelineId()), Concurrency: params.GetReadConcurrency(), MaxRows: uint64(params.GetMaxTxnRows()),
		MaxBytes: uint64(params.GetMaxTxnBytes()), RowsPerSec: params.GetRateLimit().GetRowsPerSec(), BytesPerSec: params.GetRateLimit().GetBytesPerSec(),
		TableFilter: binOpts.TableFilter, ColumnFilter: binOpts.ColumnFilter, RowFilter: binOpts.RowFilter, Transforms: binOpts.Transforms, RefreshSchema: true}
	if opts.Concurrency == 0 {
		opts.Concurrency = 2
	}
	if opts.MaxRows == 0 {
		opts.MaxRows = 10000
	}
	if opts.MaxBytes == 0 {
		opts.MaxBytes = 16 << 20
	}
	chunkRows := uint64(params.GetChunkRows())
	if chunkRows == 0 {
		chunkRows = 2000
	}
	if chunkRows > opts.MaxRows {
		return fmt.Errorf("controller: snapshot chunk rows exceeds transaction row limit")
	}
	db, err := openSnapshotDB(readEndpoint)
	if err != nil {
		return err
	}
	transferred := false
	defer func() {
		if !transferred {
			_ = db.Close()
		}
	}()
	// Runtime tickets enforce the requested concurrency. Keep the pool ceiling
	// high enough to grow without closing any existing physical connection.
	db.SetMaxOpenConns(128)
	db.SetMaxIdleConns(128)
	boundaries, _ := snapshot.NewSQLBoundaryReader(db, 30*time.Second)
	if params.GetReadFromReplica() {
		if err := boundaries.SetReplicaPolicy(snapshot.ReplicaReadPolicy{SourceUUID: src.GetEndpoint().GetServerUuid(), ReplicaUUID: readUUID, CaseSensitive: binOpts.SourceCaseSensitive}); err != nil {
			return err
		}
	}
	boundaries.SetTableFilter(binOpts.TableFilter)
	ddlGuard := snapshot.NewDDLGuard()
	if item.GetCommon().GetSnapshotTableRecoveryV1() {
		ddlGuard.EnableRecovery()
		opts.Isolated = ddlGuard.Isolated
		opts.WaitSchema = func(ctx context.Context) error {
			high, err := boundaries.GTIDSet(ctx)
			if err != nil {
				return err
			}
			wait, cancel := context.WithTimeout(ctx, snapshot.DefaultOperationTimeout)
			defer cancel()
			return ddlGuard.WaitObserved(wait, high)
		}
	}
	reader, err := snapshot.NewReader(boundaries, opts)
	if err != nil {
		return err
	}
	outMod := amqpout.New(outOpts)
	runner, err := snapshot.NewRunner(snapshot.RunOptions{PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(),
		ReadEndpointUUID:     readUUID,
		EnablePlanExtensions: item.GetCommon().GetSnapshotPlanExtensionsV1(),
		EnableTableCreation:  item.GetCommon().GetSnapshotTableCreationV1(),
		WaitObserved:         ddlGuard.WaitObserved,
		FinishRecovery:       ddlGuard.FinishRecovery,
		TableCreation:        ddlGuard.TableCreation,
		ShadowTablePolicy:    iproto.CloneShadowTablePolicy(binOpts.ShadowTablePolicy),
		UpdateObjects:        func(objects []schemamig.SchemaTable) { ddlGuard.AddObjects(objects) },
		ReinitPlan:           src.GetReinitPlan(),
		SnapshotUUID:         src.GetSnapshotUuid(), ChunkRows: chunkRows, OperationTimeout: snapshot.DefaultOperationTimeout, WaitHigh: ddlGuard.WaitHigh,
		Report: func(progress *iproto.SnapshotProgress) {
			outMod.Cast(reporter.ModuleName, reporter.ProgressReport{Progress: &iproto.PipelineProgress{PipelineId: item.GetPipelineId(), Role: iproto.PipelineRole_PIPELINE_ROLE_SOURCE, RunId: item.GetCommon().GetRunId(), TsMs: time.Now().UnixMilli(), Snapshot: progress}})
		}}, l.nc.Store, boundaries, reader, outMod)
	if err != nil {
		return err
	}
	if item.GetCommon().GetSnapshotTableRecoveryV1() {
		runner.SetRecovery(ddlGuard.RecoveryTables, ddlGuard.BeginRecovery, ddlGuard.BeginRecoveryAt)
	}
	start, err := runner.Prepare(ctx)
	if err != nil {
		return err
	}
	// Always replay from the durable L while resuming a snapshot. Duplicate
	ddlGuard.SetObjects(runner.PlannedObjects(), binOpts.SourceCaseSensitive)
	if item.GetCommon().GetSnapshotTableCreationV1() {
		if err := ddlGuard.EnableTableCreation(start.LowGTIDSet); err != nil {
			return err
		}
	}
	// incremental GTIDs are discarded by the target; this cannot skip a gap that
	// occurred before source checkpoints reached disk.
	binOpts.StartGTIDSet = start.LowGTIDSet
	binOpts.StartGTIDSetProvided = true
	binOpts.Snapshotting = true
	if err := l.prepareSourceRecovery(ctx, item, &binOpts); err != nil {
		return err
	}
	binOpts.SnapshotDDLGuard = ddlGuard
	ddlGuard.Observe(start.LowGTIDSet)
	if high, complete, err := runner.CompletedHigh(ctx); err != nil {
		return err
	} else if complete {
		if err := ddlGuard.RestoreHigh(high); err != nil {
			return err
		}
	}
	binOpts.Sink = outMod
	binOpts.HeartbeatPeriod = time.Second
	if secure := src.GetEndpoint().GetTls(); secure.GetEnabled() {
		connector, err := snapshotConnectorConfig(src.GetEndpoint())
		if err != nil {
			return err
		}
		binOpts.TLSConfig = connector.TLS
	}
	binMod := binlogin.New(binOpts)
	results, err := xhive.AddDynamicModules(outMod, binMod)
	if err != nil {
		_ = applyAddResults(results)
		return err
	}
	if err := applyAddResults(results); err != nil {
		return err
	}
	rollback := func() {
		xhive.RemoveDynamicModule(binlogin.ModuleName(item.GetPipelineId()))
		xhive.RemoveDynamicModule(amqpout.ModuleName(item.GetPipelineId()))
	}
	readyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = binMod.WaitReady(readyCtx)
	cancel()
	if err != nil {
		rollback()
		return fmt.Errorf("controller: incremental subscription not ready before snapshot: %w", err)
	}
	snapMod := snapreader.New(snapreader.Options{PipelineID: item.GetPipelineId(), RunID: item.GetCommon().GetRunId(),
		InstanceID: binOpts.InstanceID, ConfigVersion: item.GetConfigVersion(), Runner: runner, Reader: reader, DB: db, OnFailure: binMod.CancelStream, IncrementalDone: binMod.StreamDone(), IncrementalFailure: binMod.SnapshotFailure})
	results, err = xhive.AddDynamicModules(snapMod)
	if err != nil {
		_ = applyAddResults(results)
		rollback()
		return err
	}
	if err := applyAddResults(results); err != nil {
		rollback()
		return err
	}
	transferred = true
	l.mu.Lock()
	l.snapshots[item.GetPipelineId()] = true
	l.mu.Unlock()
	return nil
}

func openSnapshotDB(ep *iproto.EndpointConn) (*sql.DB, error) {
	cfg, err := snapshotConnectorConfig(ep)
	if err != nil {
		return nil, err
	}
	connector, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("controller: invalid snapshot connection options")
	}
	return sql.OpenDB(connector), nil
}

func snapshotConnectorConfig(ep *iproto.EndpointConn) (*mysql.Config, error) {
	if ep == nil || ep.GetHost() == "" || ep.GetPort() == 0 || ep.GetPort() > 65535 {
		return nil, fmt.Errorf("controller: invalid snapshot endpoint")
	}
	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd = ep.GetUsername(), ep.GetPassword()
	cfg.Net, cfg.Addr = "tcp", net.JoinHostPort(ep.GetHost(), strconv.FormatUint(uint64(ep.GetPort()), 10))
	cfg.Timeout, cfg.ReadTimeout, cfg.WriteTimeout = 10*time.Second, 30*time.Second, 10*time.Second
	if secure := ep.GetTls(); secure.GetEnabled() {
		cfg.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: ep.GetHost(), InsecureSkipVerify: secure.GetSkipVerify()}
		if len(secure.GetCaCert()) > 0 {
			cfg.TLS.RootCAs = x509.NewCertPool()
			if !cfg.TLS.RootCAs.AppendCertsFromPEM(secure.GetCaCert()) {
				return nil, fmt.Errorf("controller: invalid snapshot CA certificate")
			}
		}
		if len(secure.GetClientCert()) > 0 || len(secure.GetClientKey()) > 0 {
			certificate, err := tls.X509KeyPair(secure.GetClientCert(), secure.GetClientKey())
			if err != nil {
				return nil, fmt.Errorf("controller: invalid snapshot client certificate")
			}
			cfg.TLS.Certificates = []tls.Certificate{certificate}
		}
	}
	return cfg, nil
}
