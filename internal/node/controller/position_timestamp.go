package controller

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net"
	"strconv"

	driver "github.com/go-sql-driver/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/node/position"
)

func (m *Module) resolveConfirmedTimestampGTID(ctx context.Context, ep *iproto.EndpointConn, pos *iproto.StartPosition) (string, error) {
	if !pos.GetTimestampConfirmed() || pos.GetSourceTsMs() <= 0 || pos.GetSourceTsMs() > math.MaxInt64/1000 || pos.GetTxnCommitTsMs() < pos.GetSourceTsMs() || pos.GetSourceFile() == "" || pos.GetSourceOffset() < 4 || pos.GetSourceOffset() > math.MaxUint32 {
		return "", fmt.Errorf("controller: timestamp position requires a confirmed transaction boundary")
	}
	return m.validateResolvedPosition(ctx, ep, pos.GetGtidSet(), false)
}

func (m *Module) validateResolvedPosition(ctx context.Context, ep *iproto.EndpointConn, gtid string, allowFuture bool) (string, error) {
	if ep == nil || ep.GetHost() == "" || ep.GetPort() == 0 || ep.GetPort() > math.MaxUint16 {
		return "", fmt.Errorf("controller: invalid timestamp source endpoint")
	}
	cfg := driver.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = ep.GetUsername(), ep.GetPassword(), "tcp"
	cfg.Addr = net.JoinHostPort(ep.GetHost(), strconv.FormatUint(uint64(ep.GetPort()), 10))
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return "", fmt.Errorf("controller: invalid timestamp source connection")
	}
	defer func(cleanup func() error) { _ = cleanup() }(db.Close)
	queryCtx, cancel := context.WithTimeout(ctx, m.opts.MySQLConnectTimeout)
	defer cancel()
	set, warning, err := position.ResolveGTIDSet(queryCtx, db, gtid)
	if err != nil {
		return "", fmt.Errorf("controller: confirmed timestamp start validation failed: %w", err)
	}
	if warning != "" && !allowFuture {
		return "", fmt.Errorf("controller: confirmed timestamp contains future GTIDs")
	}
	return set, nil
}
