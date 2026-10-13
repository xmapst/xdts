package snapshot

import (
	"context"
	"fmt"
	"strings"
	"sync"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/schemamig"
	"google.golang.org/protobuf/proto"
)

// DDLGuard coordinates the background snapshot and binlog workflows. High and
// seen positions are private to the guard; no actor-owned state is accessed.
type DDLGuard struct {
	mu               sync.Mutex
	high, seen       gmysql.GTIDSet
	changed          chan struct{}
	failure          error
	objects          []schemamig.SchemaTable
	caseSensitive    bool
	events           []ddlEvent
	creationEnabled  bool
	low              gmysql.GTIDSet
	creations        map[[2]string]*iproto.Txn
	recoveryEnabled  bool
	isolated         map[[2]string]bool
	recoveryLow      gmysql.GTIDSet
	recoveryFinished bool
}

type ddlEvent struct{ gtid, schema, ddl string }

func NewDDLGuard() *DDLGuard { return &DDLGuard{changed: make(chan struct{})} }

// SetObjects fixes the durable snapshot scope before either workflow starts.
func (g *DDLGuard) SetObjects(objects []schemamig.SchemaTable, caseSensitive bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.objects = append([]schemamig.SchemaTable{}, objects...)
	g.caseSensitive = caseSensitive
}

// AddObjects extends the guarded snapshot scope after an append-only plan
// extension. Existing entries remain in place so DDL already observed against
// them cannot be forgotten.
func (g *DDLGuard) AddObjects(objects []schemamig.SchemaTable) {
	g.mu.Lock()
	defer g.mu.Unlock()
	seen := make(map[[2]string]bool, len(g.objects))
	for _, object := range g.objects {
		seen[[2]string{object.Schema, object.Table}] = true
	}
	for _, object := range objects {
		if !seen[[2]string{object.Schema, object.Table}] {
			g.objects = append(g.objects, object)
			seen[[2]string{object.Schema, object.Table}] = true
		}
	}
	for _, event := range g.events {
		if !g.matchesLocked(event.schema, event.ddl) {
			continue
		}
		if ok, reason := g.compatibleLocked(event.gtid, event.schema, event.ddl); !ok {
			if g.isolateLocked(event.schema, event.ddl) {
				continue
			}
			g.failure = fmt.Errorf("%s", reason)
			close(g.changed)
			g.changed = make(chan struct{})
			return
		}
	}
}

func (g *DDLGuard) Failure() error { g.mu.Lock(); defer g.mu.Unlock(); return g.failure }

// RestoreHigh runs before the incremental subscription is created on a resume.
func (g *DDLGuard) RestoreHigh(high string) error {
	parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, high)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.high = parsed
	g.mu.Unlock()
	return nil
}

func (g *DDLGuard) Check(gtid, schema, ddl string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.recoveryLow != nil {
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
		if err != nil {
			return err
		}
		if g.recoveryLow.Contain(one) {
			return nil
		}
	}
	if g.high != nil {
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
		if err != nil {
			return err
		}
		if !g.high.Contain(one) {
			if g.recoveryEnabled && !g.recoveryFinished {
				if len(g.events) >= 4096 {
					return fmt.Errorf("snapshot: recovery DDL history bound exceeded")
				}
				g.events = append(g.events, ddlEvent{gtid: gtid, schema: schema, ddl: ddl})
			}
			return nil
		}
	}
	if len(g.events) >= 4096 {
		g.failure = fmt.Errorf("全量期间 DDL 事件过多，无法安全扩展快照对象范围")
		close(g.changed)
		g.changed = make(chan struct{})
		return g.failure
	}
	g.events = append(g.events, ddlEvent{gtid: gtid, schema: schema, ddl: ddl})
	if !g.matchesLocked(schema, ddl) {
		return nil
	}
	ok, reason := g.compatibleLocked(gtid, schema, ddl)
	if !ok {
		if g.isolateLocked(schema, ddl) {
			return nil
		}
		g.failure = fmt.Errorf("%s", reason)
		close(g.changed)
		g.changed = make(chan struct{})
		return g.failure
	}
	return nil
}

func (g *DDLGuard) EnableRecovery() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recoveryEnabled = true
	g.isolated = map[[2]string]bool{}
}

func (g *DDLGuard) FinishRecovery() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.recoveryFinished = true
	g.events = nil
}

func (g *DDLGuard) isolateLocked(schema, ddl string) bool {
	if !g.recoveryEnabled {
		return false
	}
	objects, known := schemamig.DDLObjects(schema, ddl)
	if !known || len(objects) == 0 {
		return false
	}
	for _, object := range objects {
		for _, selected := range g.objects {
			if object.Schema == selected.Schema && object.Table == selected.Table || !g.caseSensitive && strings.EqualFold(object.Schema, selected.Schema) && strings.EqualFold(object.Table, selected.Table) {
				g.isolated[[2]string{selected.Schema, selected.Table}] = true
			}
		}
	}
	return len(g.isolated) > 0
}

func (g *DDLGuard) Isolated(schema, table string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.isolated[[2]string{schema, table}]
}

func (g *DDLGuard) RecoveryTables() []*iproto.SnapshotRecoveryTable {
	g.mu.Lock()
	defer g.mu.Unlock()
	var tables []*iproto.SnapshotRecoveryTable
	for _, object := range g.objects {
		if g.isolated[[2]string{object.Schema, object.Table}] {
			tables = append(tables, &iproto.SnapshotRecoveryTable{Schema: object.Schema, Table: object.Table})
		}
	}
	return tables
}

// The source owner invokes this only between completed read passes. Check runs
// concurrently, so reset the guarded interval under the guard's private lock.
func (g *DDLGuard) BeginRecovery(tables []*iproto.SnapshotRecoveryTable) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.high = nil
	g.events = nil
	g.recoveryLow = nil
	for _, table := range tables {
		delete(g.isolated, [2]string{table.Schema, table.Table})
	}
}

func (g *DDLGuard) BeginRecoveryAt(tables []*iproto.SnapshotRecoveryTable, low string) error {
	set, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, low)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.high = nil
	g.recoveryLow = set
	for _, table := range tables {
		delete(g.isolated, [2]string{table.Schema, table.Table})
	}
	kept := g.events[:0]
	for _, event := range g.events {
		one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, event.gtid)
		if err != nil {
			return err
		}
		if !set.Contain(one) {
			kept = append(kept, event)
			if ok, _ := g.compatibleLocked(event.gtid, event.schema, event.ddl); !ok {
				g.isolateLocked(event.schema, event.ddl)
			}
		}
	}
	g.events = kept
	return nil
}

func (g *DDLGuard) EnableTableCreation(low string) error {
	parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, low)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.creationEnabled, g.low = true, parsed
	g.creations = make(map[[2]string]*iproto.Txn)
	return nil
}

func (g *DDLGuard) compatibleLocked(gtid, schema, ddl string) (bool, string) {
	if g.creationEnabled {
		if _, ok := schemamig.SnapshotCreation(schema, ddl); ok {
			one, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, gtid)
			if err == nil && g.low != nil && !g.low.Contain(one) {
				return true, ""
			}
		}
	}
	return schemamig.SnapshotDDLCompatible(ddl)
}

// RecordCreation receives an already-filtered complete transaction. Its copy
// belongs to the guard, never to either source workflow.
func (g *DDLGuard) RecordCreation(txn *iproto.Txn) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.creationEnabled || txn.GetDdl() == nil {
		return nil
	}
	object, ok := schemamig.SnapshotCreation(txn.GetDdl().GetSchema(), txn.GetDdl().GetStatement())
	if !ok {
		return nil
	}
	if err := iproto.ValidateSnapshotCreation(txn, object.Schema, object.Table); err != nil {
		return err
	}
	one, _ := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, txn.GetGtid())
	if g.low.Contain(one) || g.high != nil && !g.high.Contain(one) {
		return nil
	}
	key := [2]string{object.Schema, object.Table}
	if !g.caseSensitive {
		key = [2]string{strings.ToLower(key[0]), strings.ToLower(key[1])}
	}
	if previous := g.creations[key]; previous != nil && !proto.Equal(previous, txn) {
		return fmt.Errorf("snapshot: table was recreated during snapshot")
	}
	g.creations[key] = proto.Clone(txn).(*iproto.Txn)
	return nil
}

func (g *DDLGuard) TableCreation(schema, table string) *iproto.Txn {
	g.mu.Lock()
	defer g.mu.Unlock()
	key := [2]string{schema, table}
	if !g.caseSensitive {
		key = [2]string{strings.ToLower(schema), strings.ToLower(table)}
	}
	if txn := g.creations[key]; txn != nil {
		return proto.Clone(txn).(*iproto.Txn)
	}
	return nil
}

// WaitObserved drains through the catalog watermark without fixing final H.
func (g *DDLGuard) WaitObserved(ctx context.Context, set string) error {
	parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return err
	}
	for {
		g.mu.Lock()
		ready := g.seen != nil && g.seen.Contain(parsed)
		failure, changed := g.failure, g.changed
		g.mu.Unlock()
		if failure != nil {
			return failure
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

func (g *DDLGuard) matchesLocked(schema, ddl string) bool {
	if g.objects == nil {
		return true
	}
	affected, known := schemamig.DDLObjects(schema, ddl)
	if !known {
		return true
	}
	equal := func(a, b string) bool {
		if g.caseSensitive {
			return a == b
		}
		return strings.EqualFold(a, b)
	}
	for _, change := range affected {
		for _, object := range g.objects {
			if equal(change.Schema, object.Schema) && (change.Table == "" || equal(change.Table, object.Table)) {
				return true
			}
		}
	}
	return false
}

func (g *DDLGuard) Observe(set string) {
	parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, set)
	if err != nil {
		return
	}
	g.mu.Lock()
	g.seen = parsed
	close(g.changed)
	g.changed = make(chan struct{})
	g.mu.Unlock()
}

// WaitHigh fixes H and requires the ordered binlog path to validate every DDL
// through H before the Runner can publish End. A late incompatible DDL cannot
// escape merely because the reader finished ahead of the binlog consumer.
func (g *DDLGuard) WaitHigh(ctx context.Context, high string) error {
	parsed, err := gmysql.ParseGTIDSet(gmysql.MySQLFlavor, high)
	if err != nil {
		return err
	}
	g.mu.Lock()
	g.high = parsed
	g.mu.Unlock()
	for {
		g.mu.Lock()
		ready := g.seen != nil && g.seen.Contain(g.high)
		failure := g.failure
		changed := g.changed
		g.mu.Unlock()
		if failure != nil {
			return failure
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}
