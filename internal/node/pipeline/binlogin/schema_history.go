package binlogin

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdoc"
	"github.com/xmapst/xdts/internal/node/schema"
	"github.com/xmapst/xdts/internal/node/store"
	"github.com/xmapst/xdts/pkg/uuidx"
)

var errSchemaHistory = errors.New("binlogin: schema history unavailable")

const schemaHistoryTimeout = 10 * time.Second

// Versions own their Snapshot, and only a committed head makes a new version
// discoverable. Retain all versions until their binlog is retired.
type schemaHistoryHead struct {
	Version    uint32
	PipelineID string
	RunID      uint64
	LatestID   string
}

type schemaHistoryVersion struct {
	Version    uint32
	PipelineID string
	RunID      uint64
	PreviousID string
	Snapshot   schema.Snapshot
}

func (m *Module) schemaHistoryHeadID() string {
	return fmt.Sprintf("%s.source.history.%d", m.opts.PipelineID, m.opts.RunID)
}

func (m *Module) schemaHistoryVersionPrefix() string {
	return fmt.Sprintf("%s.source.history-version.%d.", m.opts.PipelineID, m.opts.RunID)
}

func (m *Module) restoreSchemaHistory(ctx context.Context, startGTIDSet string) error {
	m.schemaTracker = schema.NewTracker(m.opts.SourceCaseSensitive)
	m.schemaHistoryLatestID = ""
	if m.opts.Store == nil {
		return nil
	}
	start, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, startGTIDSet)
	if err != nil {
		return fmt.Errorf("%w: invalid recovery GTID", errSchemaHistory)
	}
	var head schemaHistoryHead
	if err := m.opts.Store.Get(ctx, schemaHistoryCollection, m.schemaHistoryHeadID(), &head); err != nil {
		if !errors.Is(err, xdoc.ErrNotFound) {
			return fmt.Errorf("%w: read head: %w", errSchemaHistory, err)
		}
		return nil
	}
	if head.Version != 1 || head.PipelineID != m.opts.PipelineID || head.RunID != m.opts.RunID || head.LatestID == "" {
		return fmt.Errorf("%w: invalid head identity", errSchemaHistory)
	}
	m.schemaHistoryLatestID = head.LatestID
	seen := make(map[string]struct{})
	var selected *schema.Snapshot
	var selectedSet gmysql.GTIDSet
	for id := head.LatestID; id != ""; {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: history traversal: %w", errSchemaHistory, err)
		}
		suffix, valid := strings.CutPrefix(id, m.schemaHistoryVersionPrefix())
		if _, err := uuidx.Parse(suffix); !valid || err != nil {
			return fmt.Errorf("%w: invalid version key", errSchemaHistory)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: cyclic version chain", errSchemaHistory)
		}
		seen[id] = struct{}{}
		var version schemaHistoryVersion
		if err := m.opts.Store.Get(ctx, schemaHistoryCollection, id, &version); err != nil {
			return fmt.Errorf("%w: read version: %w", errSchemaHistory, err)
		}
		if version.Version != 1 || version.PipelineID != m.opts.PipelineID || version.RunID != m.opts.RunID {
			return fmt.Errorf("%w: invalid version identity", errSchemaHistory)
		}
		set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, version.Snapshot.ValidThroughGTIDSet)
		if err != nil {
			return fmt.Errorf("%w: invalid version GTID", errSchemaHistory)
		}
		if start.Contain(set) && version.Snapshot.CaseSensitive == m.opts.SourceCaseSensitive {
			// A backward edit can append an older baseline after a newer snapshot.
			// Select the maximal contained position, never the first stored match.
			if selectedSet == nil || (set.Contain(selectedSet) && !selectedSet.Contain(set)) {
				value := version.Snapshot
				selected, selectedSet = &value, set
			} else if !selectedSet.Contain(set) {
				return fmt.Errorf("%w: incomparable recovery snapshots", errSchemaHistory)
			}
		}
		id = version.PreviousID
	}
	if selected != nil {
		m.schemaTracker.Restore(*selected)
	}
	return nil
}

func (m *Module) commitSchemaHistory(ctx context.Context, validThroughGTIDSet string) error {
	if m.opts.Store == nil {
		return nil
	}
	if _, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, validThroughGTIDSet); err != nil {
		return fmt.Errorf("%w: invalid commit GTID", errSchemaHistory)
	}
	snapshot := m.schemaTracker.Snapshot()
	snapshot.ValidThroughGTIDSet = validThroughGTIDSet
	id := m.schemaHistoryVersionPrefix() + uuidx.New().String()
	version := schemaHistoryVersion{Version: 1, PipelineID: m.opts.PipelineID, RunID: m.opts.RunID,
		PreviousID: m.schemaHistoryLatestID, Snapshot: snapshot}
	if err := m.opts.Store.PutCritical(ctx, store.Op{Collection: schemaHistoryCollection, ID: id, Doc: version, InsertOnly: true}); err != nil {
		return fmt.Errorf("%w: commit version: %w", errSchemaHistory, err)
	}
	head := schemaHistoryHead{Version: 1, PipelineID: m.opts.PipelineID, RunID: m.opts.RunID, LatestID: id}
	if err := m.opts.Store.PutCritical(ctx, store.Op{Collection: schemaHistoryCollection, ID: m.schemaHistoryHeadID(), Doc: head}); err != nil {
		return fmt.Errorf("%w: commit head: %w", errSchemaHistory, err)
	}
	m.schemaHistoryLatestID = id
	return nil
}
