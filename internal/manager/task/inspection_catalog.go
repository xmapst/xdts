package task

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/manager/repo"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var ErrCatalogNotProbed = errors.New("endpoint has no successful probe")
var ErrCatalogFailed = errors.New("endpoint catalog probe failed")
var ErrCatalogTooLarge = errors.New("endpoint catalog exceeds limit")

// CatalogEntry 是库表浏览 HTTP 视图，不携带连接凭据。
type CatalogEntry struct {
	Schema        string `json:"schema"`
	Table         string `json:"table"`
	EstimatedRows *int64 `json:"estimatedRows"`
	HasPrimaryKey bool   `json:"hasPrimaryKey"`
	HasUniqueKey  bool   `json:"hasUniqueKey"`
}

// GetEndpointCatalog 向同中心在线节点发起只读目录查询，不更新探测结果。
func (l *InspectionLoop) GetEndpointCatalog(ctx context.Context, endpointID uuidx.UUID) ([]CatalogEntry, error) {
	dispatcher := l.dispatcherSnapshot()
	if l.db == nil || dispatcher == nil {
		return nil, ErrProbeUnavailable
	}
	ep, err := repo.GetEndpoint(ctx, l.db, endpointID)
	if err != nil {
		return nil, err
	}
	var prior struct {
		Reachable bool `json:"reachable"`
	}
	if len(ep.LastProbeResult) == 0 || json.Unmarshal(ep.LastProbeResult, &prior) != nil || !prior.Reachable {
		return nil, ErrCatalogNotProbed
	}
	nodes, err := repo.ListNodes(ctx, l.db, ep.CenterID)
	if err != nil {
		return nil, err
	}
	node := pickInspectorCandidate(nodes)
	if node == nil {
		return nil, ErrProbeNodeOffline
	}
	dispatchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := dispatcher.DispatchCatalog(dispatchCtx, *ep, *node, uuidx.New().String())
	if err != nil {
		return nil, ErrCatalogFailed
	}
	if !result.GetSuccess() || result.GetCatalog() == nil {
		if result.GetError().GetCode() == "CATALOG_TOO_LARGE" {
			return nil, ErrCatalogTooLarge
		}
		return nil, ErrCatalogFailed
	}
	return catalogEntries(result.GetCatalog()), nil
}

func catalogEntries(result *iproto.CatalogResult) []CatalogEntry {
	entries := make([]CatalogEntry, 0, len(result.GetTables()))
	for _, table := range result.GetTables() {
		entry := CatalogEntry{Schema: table.GetSchema(), Table: table.GetTable(),
			HasPrimaryKey: len(table.GetPrimaryKeyColumns()) > 0,
			HasUniqueKey:  len(table.GetUniqueKeys()) > 0}
		if table.GetEstimatedRowsKnown() {
			rows := table.GetEstimatedRows()
			entry.EstimatedRows = &rows
		}
		entries = append(entries, entry)
	}
	return entries
}
