// Package datacheck compares explicit source/target tables using independent
// read-only snapshots. It does not lock business writes or modify either side.
package datacheck

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrConfig = errors.New("datacheck: invalid configuration")
	ErrSchema = errors.New("datacheck: incompatible table, columns or primary key")
	ErrRead   = errors.New("datacheck: database read failed")
	ErrRow    = errors.New("datacheck: row exceeds limits or contains invalid JSON")
)

type Column struct {
	Source string `json:"source"`
	Target string `json:"target"`
}
type Table struct {
	SourceSchema string   `json:"sourceSchema"`
	SourceTable  string   `json:"sourceTable"`
	TargetSchema string   `json:"targetSchema"`
	TargetTable  string   `json:"targetTable"`
	Columns      []Column `json:"columns"`
}
type Config struct {
	Tables []Table `json:"tables"`
}
type Options struct {
	SampleModulo uint64
	MaxDetails   int
}
type Difference struct {
	Kind      string   `json:"kind"`
	KeyDigest string   `json:"keyDigest"`
	Columns   []string `json:"columns"`
}
type TableResult struct {
	SourceSchema   string       `json:"sourceSchema"`
	SourceTable    string       `json:"sourceTable"`
	TargetSchema   string       `json:"targetSchema"`
	TargetTable    string       `json:"targetTable"`
	Columns        []Column     `json:"columns"`
	ReadSource     uint64       `json:"readSource"`
	ReadTarget     uint64       `json:"readTarget"`
	Compared       uint64       `json:"compared"`
	MissingSource  uint64       `json:"missingSource"`
	MissingTarget  uint64       `json:"missingTarget"`
	Changed        uint64       `json:"changed"`
	DetailsOmitted uint64       `json:"detailsOmitted"`
	Differences    []Difference `json:"differences"`
}
type Result struct {
	StartedAt    time.Time     `json:"startedAt"`
	FinishedAt   time.Time     `json:"finishedAt"`
	SnapshotMode string        `json:"snapshotMode"`
	SampleModulo uint64        `json:"sampleModulo"`
	Equal        bool          `json:"equal"`
	Tables       []TableResult `json:"tables"`
}

type Checker struct{ source, target *sql.DB }

func New(source, target *sql.DB) *Checker { return &Checker{source: source, target: target} }

func identifier(value string) bool {
	return value != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= 64 && !strings.ContainsRune(value, 0)
}
func quoted(value string) string { return "`" + strings.ReplaceAll(value, "`", "``") + "`" }

func (config Config) Validate() error {
	if len(config.Tables) == 0 || len(config.Tables) > 100 {
		return ErrConfig
	}
	seenTables := map[string]bool{}
	for _, table := range config.Tables {
		if !identifier(table.SourceSchema) || !identifier(table.SourceTable) || !identifier(table.TargetSchema) || !identifier(table.TargetTable) || len(table.Columns) == 0 || len(table.Columns) > 200 {
			return ErrConfig
		}
		identity := table.SourceSchema + "\x00" + table.SourceTable + "\x00" + table.TargetSchema + "\x00" + table.TargetTable
		if seenTables[identity] {
			return ErrConfig
		}
		seenTables[identity] = true
		source, target := map[string]bool{}, map[string]bool{}
		for _, col := range table.Columns {
			left, right := strings.ToLower(col.Source), strings.ToLower(col.Target)
			if !identifier(col.Source) || !identifier(col.Target) || source[left] || target[right] {
				return ErrConfig
			}
			source[left], target[right] = true, true
		}
	}
	return nil
}

type columnMeta struct{ kind, fullType string }

func metadata(ctx context.Context, tx *sql.Tx, schema, table string) (map[string]columnMeta, []string, error) {
	var engine sql.NullString
	if err := tx.QueryRowContext(ctx, "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND TABLE_TYPE='BASE TABLE'", schema, table).Scan(&engine); err != nil || !engine.Valid || !strings.EqualFold(engine.String, "InnoDB") {
		return nil, nil, ErrSchema
	}
	rows, err := tx.QueryContext(ctx, "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", schema, table)
	if err != nil {
		return nil, nil, ErrRead
	}
	columns := map[string]columnMeta{}
	for rows.Next() {
		var name string
		var col columnMeta
		if rows.Scan(&name, &col.kind, &col.fullType) != nil {
			_ = rows.Close()
			return nil, nil, ErrRead
		}
		columns[strings.ToLower(name)] = col
	}
	err = rows.Err()
	_ = rows.Close()
	if err != nil {
		return nil, nil, ErrRead
	}
	keys, err := tx.QueryContext(ctx, "SELECT COLUMN_NAME FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND INDEX_NAME='PRIMARY' ORDER BY SEQ_IN_INDEX", schema, table)
	if err != nil {
		return nil, nil, ErrRead
	}
	defer func(cleanup func() error) { _ = cleanup() }(keys.Close)
	var primary []string
	for keys.Next() {
		var key string
		if keys.Scan(&key) != nil {
			return nil, nil, ErrRead
		}
		primary = append(primary, strings.ToLower(key))
	}
	if keys.Err() != nil {
		return nil, nil, ErrRead
	}
	if len(primary) == 0 {
		return nil, nil, ErrSchema
	}
	return columns, primary, nil
}

type rowStream struct {
	rows     *sql.Rows
	keyIndex []int
	jsonCols []bool
	width    int
	read     *uint64
}
type rowValue struct {
	values [][]byte
	key    [][]byte
}

func (s *rowStream) next() (*rowValue, error) {
	if !s.rows.Next() {
		if s.rows.Err() != nil {
			return nil, ErrRead
		}
		return nil, nil
	}
	values := make([][]byte, s.width)
	targets := make([]any, s.width)
	for i := range values {
		targets[i] = &values[i]
	}
	if s.rows.Scan(targets...) != nil {
		return nil, ErrRead
	}
	size := 0
	for i, value := range values {
		size += len(value)
		if size > 16*1024*1024 {
			return nil, ErrRow
		}
		if s.jsonCols[i] && value != nil {
			decoder := json.NewDecoder(bytes.NewReader(value))
			decoder.UseNumber()
			var parsed, trailing any
			if decoder.Decode(&parsed) != nil || decoder.Decode(&trailing) != io.EOF {
				return nil, ErrRow
			}
			canonical, err := json.Marshal(parsed)
			if err != nil {
				return nil, ErrRow
			}
			values[i] = canonical
		}
	}
	key := make([][]byte, len(s.keyIndex))
	for i, index := range s.keyIndex {
		if values[index] == nil {
			return nil, ErrSchema
		}
		key[i] = values[index]
	}
	(*s.read)++
	return &rowValue{values: values, key: key}, nil
}

func compareKeys(left, right [][]byte) int {
	for i := range left {
		if result := bytes.Compare(left[i], right[i]); result != 0 {
			return result
		}
	}
	return 0
}
func digestKey(key [][]byte) [32]byte {
	hash := sha256.New()
	var size [8]byte
	for _, value := range key {
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		hash.Write(size[:])
		hash.Write(value)
	}
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func tableQuery(schema, table string, columns []string, keys []string) string {
	selects := make([]string, len(columns))
	order := make([]string, len(keys))
	for i, col := range columns {
		selects[i] = "CAST(" + quoted(col) + " AS BINARY)"
	}
	for i, key := range keys {
		order[i] = "CAST(" + quoted(key) + " AS BINARY)"
	}
	return "SELECT " + strings.Join(selects, ",") + " FROM " + quoted(schema) + "." + quoted(table) + " ORDER BY " + strings.Join(order, ",")
}

func (c *Checker) Check(ctx context.Context, config Config, options Options) (Result, error) {
	result := Result{StartedAt: time.Now().UTC(), SnapshotMode: "independent-repeatable-read", SampleModulo: options.SampleModulo, Tables: []TableResult{}}
	if config.Validate() != nil || c == nil || c.source == nil || c.target == nil || options.SampleModulo == 0 || options.MaxDetails < 0 || options.MaxDetails > 100 {
		return result, ErrConfig
	}
	result.Equal = true
	for index, table := range config.Tables {
		row, err := c.checkTable(ctx, table, options)
		if err != nil {
			result.Equal = false
			return result, fmt.Errorf("table %d: %w", index+1, err)
		}
		if row.MissingSource+row.MissingTarget+row.Changed != 0 {
			result.Equal = false
		}
		result.Tables = append(result.Tables, row)
	}
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func (c *Checker) checkTable(ctx context.Context, table Table, options Options) (TableResult, error) {
	result := TableResult{SourceSchema: table.SourceSchema, SourceTable: table.SourceTable, TargetSchema: table.TargetSchema, TargetTable: table.TargetTable, Columns: append([]Column(nil), table.Columns...), Differences: []Difference{}}
	leftTx, err := c.source.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return result, ErrRead
	}
	defer func(cleanup func() error) { _ = cleanup() }(leftTx.Rollback)
	rightTx, err := c.target.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return result, ErrRead
	}
	defer func(cleanup func() error) { _ = cleanup() }(rightTx.Rollback)
	leftCols, leftKeys, err := metadata(ctx, leftTx, table.SourceSchema, table.SourceTable)
	if err != nil {
		return result, err
	}
	rightCols, rightKeys, err := metadata(ctx, rightTx, table.TargetSchema, table.TargetTable)
	if err != nil {
		return result, err
	}
	if len(leftKeys) != len(rightKeys) {
		return result, ErrSchema
	}
	leftNames, rightNames, jsonCols := make([]string, len(table.Columns)), make([]string, len(table.Columns)), make([]bool, len(table.Columns))
	indices := make([]int, len(leftKeys))
	for key := range indices {
		indices[key] = -1
	}
	for index, col := range table.Columns {
		left, leftOK := leftCols[strings.ToLower(col.Source)]
		right, rightOK := rightCols[strings.ToLower(col.Target)]
		if !leftOK || !rightOK || left.fullType != right.fullType {
			return result, ErrSchema
		}
		leftNames[index], rightNames[index], jsonCols[index] = col.Source, col.Target, left.kind == "json"
		for key, name := range leftKeys {
			if strings.EqualFold(col.Source, name) {
				if !strings.EqualFold(col.Target, rightKeys[key]) {
					return result, ErrSchema
				}
				indices[key] = index
			}
		}
	}
	for _, index := range indices {
		if index < 0 {
			return result, ErrSchema
		}
	}
	leftRows, err := leftTx.QueryContext(ctx, tableQuery(table.SourceSchema, table.SourceTable, leftNames, leftKeys))
	if err != nil {
		return result, ErrRead
	}
	defer func(cleanup func() error) { _ = cleanup() }(leftRows.Close)
	rightRows, err := rightTx.QueryContext(ctx, tableQuery(table.TargetSchema, table.TargetTable, rightNames, rightKeys))
	if err != nil {
		return result, ErrRead
	}
	defer func(cleanup func() error) { _ = cleanup() }(rightRows.Close)
	left := &rowStream{rows: leftRows, keyIndex: indices, jsonCols: jsonCols, width: len(leftNames), read: &result.ReadSource}
	right := &rowStream{rows: rightRows, keyIndex: indices, jsonCols: jsonCols, width: len(rightNames), read: &result.ReadTarget}
	add := func(kind string, key [][]byte, columns []string) {
		if len(result.Differences) < options.MaxDetails {
			digest := digestKey(key)
			result.Differences = append(result.Differences, Difference{Kind: kind, KeyDigest: hex.EncodeToString(digest[:]), Columns: columns})
		} else {
			result.DetailsOmitted++
		}
	}
	l, err := left.next()
	if err != nil {
		return result, err
	}
	r, err := right.next()
	if err != nil {
		return result, err
	}
	for l != nil || r != nil {
		if ctx.Err() != nil {
			return result, ErrRead
		}
		order := 0
		key := [][]byte(nil)
		if l == nil {
			order, key = 1, r.key
		} else if r == nil {
			order, key = -1, l.key
		} else {
			order, key = compareKeys(l.key, r.key), l.key
			if order > 0 {
				key = r.key
			}
		}
		digest := digestKey(key)
		if binary.BigEndian.Uint64(digest[:8])%options.SampleModulo == 0 {
			result.Compared++
			switch {
			case order < 0:
				result.MissingTarget++
				add("missing_target", key, []string{})
			case order > 0:
				result.MissingSource++
				add("missing_source", key, []string{})
			default:
				changed := []string{}
				for i := range l.values {
					if (l.values[i] == nil) != (r.values[i] == nil) || !bytes.Equal(l.values[i], r.values[i]) {
						changed = append(changed, table.Columns[i].Source)
					}
				}
				if len(changed) != 0 {
					result.Changed++
					add("changed", key, changed)
				}
			}
		}
		if order <= 0 {
			l, err = left.next()
			if err != nil {
				return result, err
			}
		}
		if order >= 0 {
			r, err = right.next()
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}
