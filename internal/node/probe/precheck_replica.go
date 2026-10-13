package probe

import (
	"context"
	"database/sql"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/snapshot"
)

func (m *Module) checkSnapshotReplica(ctx context.Context, req *iproto.ProbeRequest, run *precheckRun) {
	ep := req.GetPrecheck().GetSnapshotReplica()
	verify := func() error {
		db, err := sql.Open("mysql", probeMySQLDSN(ep))
		if err != nil {
			return err
		}
		defer func(cleanup func() error) { _ = cleanup() }(db.Close)
		reader, err := snapshot.NewSQLBoundaryReader(db, m.opts.MySQLConnectTimeout)
		if err != nil {
			return err
		}
		if err := reader.SetReplicaPolicy(snapshot.ReplicaReadPolicy{SourceUUID: run.result.GetServerUuid(), ReplicaUUID: ep.GetServerUuid(), CaseSensitive: run.result.GetKeyParams()["lower_case_table_names"] == "0"}); err != nil {
			return err
		}
		if _, err := reader.GTIDSet(ctx); err != nil {
			return err
		}
		for _, table := range req.GetPrecheck().GetTables() {
			if _, err := reader.Table(ctx, table.GetSchema(), table.GetTable()); err != nil {
				return err
			}
		}
		return nil
	}
	err := verify()
	run.add("PRE-34", err == nil, false, "全量读取端点必须是对应主库的活动只读 GTID 从库，且业务表可读取", map[string]any{"replica_uuid": ep.GetServerUuid(), "primary_uuid": run.result.GetServerUuid(), "verified": err == nil})
}
