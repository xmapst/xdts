// Package snapshot owns source snapshot planning. Planners are confined to one
// background workflow; they must not perform storage IO on an actor event loop.
package snapshot

import (
	"context"
	"errors"
	"slices"

	"github.com/xmapst/xdts/internal/node/store"
)

const planVersion = 1

// MySQL rejects GNO 2^63-1; the greatest accepted transaction sequence is 2^63-2.
const maxChunkNumber uint64 = 1<<63 - 2

var ErrNoUsableKey = errors.New("snapshot: table has no primary or non-null unique key")

// Column describes metadata read from information_schema, including information
// deliberately absent from the binlog schema tracker.
type Column struct {
	Name       string `bson:"name"`
	DataType   string `bson:"data_type"`
	ColumnType string `bson:"column_type,omitempty"`
	Collation  string `bson:"collation,omitempty"`
	Unsigned   bool   `bson:"unsigned"`
	Nullable   bool   `bson:"nullable"`
	Generated  bool   `bson:"generated"`
}

type Index struct {
	Name    string   `bson:"name"`
	Columns []string `bson:"columns"`
	// Incomplete excludes prefix and expression indexes from row positioning.
	Incomplete bool `bson:"incomplete"`
}

type Table struct {
	Schema        string   `bson:"schema"`
	Name          string   `bson:"name"`
	Columns       []Column `bson:"columns"`
	PrimaryKey    Index    `bson:"primary_key"`
	UniqueKeys    []Index  `bson:"unique_keys"`
	EstimatedRows uint64   `bson:"estimated_rows"`
}

type Key struct {
	Name    string   `bson:"name"`
	Columns []Column `bson:"columns"`
}

// Boundary stores raw MySQL values, preserving uint64, binary data and text. SQL
// performs comparison using the source column's type and collation.
type Boundary struct {
	Values [][]byte `bson:"values"`
}

type Page struct {
	Last Boundary
	Rows uint64
}

type IBoundaryReader interface {
	// Empty tables return empty min and max. All other values are base-10 integers.
	IntegerBounds(context.Context, Table, Key) (min, max string, err error)
	// Page returns only the last key and count of a short ordered page strictly
	// after the supplied key; an empty Boundary starts at the beginning.
	Page(context.Context, Table, Key, Boundary, uint64) (Page, error)
}

type IStore interface {
	Get(context.Context, string, string, any) error
	PutCritical(context.Context, store.Op) error
}

type Strategy string

const (
	StrategyInteger Strategy = "integer"
	StrategyKeyset  Strategy = "keyset"
)

// TablePlan is immutable after commit. Integer ranges are compact, so planning
// a large sparse table does not allocate an entry for each empty range.
type TablePlan struct {
	// Baseline is additive: older plans omit it and retain their existing checks.
	Baseline   *Table     `bson:"baseline,omitempty"`
	Schema     string     `bson:"schema"`
	Table      string     `bson:"table"`
	Key        Key        `bson:"key"`
	Strategy   Strategy   `bson:"strategy"`
	FirstChunk uint64     `bson:"first_chunk"`
	ChunkCount uint64     `bson:"chunk_count"`
	Minimum    string     `bson:"minimum"`
	Maximum    string     `bson:"maximum"`
	Step       string     `bson:"step"`
	Boundaries []Boundary `bson:"boundaries"`
}

type Plan struct {
	ReinitDigest string      `bson:"reinit_digest,omitempty"`
	Version      int         `bson:"version"`
	PipelineID   string      `bson:"pipeline_id"`
	RunID        string      `bson:"run_id"`
	ChunkRows    uint64      `bson:"chunk_rows"`
	Total        uint64      `bson:"total"`
	Tables       []TablePlan `bson:"tables"`
}

// PlanExtension is an append-only record for objects discovered after the
// initial plan was committed. Existing chunk numbers are never rewritten;
// extension table plans start at the next global number.
type PlanExtension struct {
	Version    int         `bson:"version"`
	PipelineID string      `bson:"pipeline_id"`
	RunID      string      `bson:"run_id"`
	Index      uint64      `bson:"index"`
	Total      uint64      `bson:"total"`
	Tables     []TablePlan `bson:"tables"`
}

// Chunk selects all rows in its interval. Integer: lower <= key < upper;
// keyset: lower < key <= upper. Empty boundaries are unbounded. This covers
// keys inserted beyond the extrema observed during planning.
type Chunk struct {
	Number   uint64
	Schema   string
	Table    string
	Key      Key
	Strategy Strategy
	Lower    Boundary
	Upper    Boundary
}

func (b Boundary) clone() Boundary {
	copy := Boundary{Values: make([][]byte, len(b.Values))}
	for i, value := range b.Values {
		copy.Values[i] = slices.Clone(value)
	}
	return copy
}

func (t TablePlan) clone() TablePlan {
	if t.Baseline != nil {
		baseline := *t.Baseline
		baseline.Columns = slices.Clone(baseline.Columns)
		baseline.PrimaryKey.Columns = slices.Clone(baseline.PrimaryKey.Columns)
		baseline.UniqueKeys = slices.Clone(baseline.UniqueKeys)
		for i := range baseline.UniqueKeys {
			baseline.UniqueKeys[i].Columns = slices.Clone(baseline.UniqueKeys[i].Columns)
		}
		t.Baseline = &baseline
	}
	t.Key.Columns = slices.Clone(t.Key.Columns)
	bounds := make([]Boundary, len(t.Boundaries))
	for i, b := range t.Boundaries {
		bounds[i] = b.clone()
	}
	t.Boundaries = bounds
	return t
}

func (p Plan) clone() Plan {
	tables := make([]TablePlan, len(p.Tables))
	for i, table := range p.Tables {
		tables[i] = table.clone()
	}
	p.Tables = tables
	return p
}
