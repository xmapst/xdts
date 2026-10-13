package datacheck

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"strings"
	"time"
)

const DigestBucketCount = 256

type LocalTable struct {
	Schema  string   `json:"schema"`
	Table   string   `json:"table"`
	Columns []string `json:"columns"`
}

type DigestBucket struct {
	Count   uint64   `json:"count"`
	Keys    string   `json:"keys"`
	Rows    string   `json:"rows"`
	Columns []string `json:"columns"`
}

type DigestResult struct {
	StartedAt    time.Time      `json:"startedAt"`
	FinishedAt   time.Time      `json:"finishedAt"`
	Read         uint64         `json:"read"`
	Sampled      uint64         `json:"sampled"`
	SampleModulo uint64         `json:"sampleModulo"`
	ColumnTypes  []string       `json:"columnTypes"`
	KeyIndices   []int          `json:"keyIndices"`
	Buckets      []DigestBucket `json:"buckets"`
}

type BucketDifference struct {
	Bucket     int      `json:"bucket"`
	SourceRows uint64   `json:"sourceRows"`
	TargetRows uint64   `json:"targetRows"`
	KeysEqual  bool     `json:"keysEqual"`
	Columns    []string `json:"columns"`
}

type DigestComparison struct {
	Table           Table              `json:"table"`
	Equal           bool               `json:"equal"`
	ReadSource      uint64             `json:"readSource"`
	ReadTarget      uint64             `json:"readTarget"`
	SampledSource   uint64             `json:"sampledSource"`
	SampledTarget   uint64             `json:"sampledTarget"`
	SourceStartedAt time.Time          `json:"sourceStartedAt"`
	TargetStartedAt time.Time          `json:"targetStartedAt"`
	Differences     []BucketDifference `json:"differences"`
}

type Scanner struct{ db *sql.DB }

func NewScanner(db *sql.DB) *Scanner { return &Scanner{db: db} }

func (t LocalTable) Validate() error {
	if !identifier(t.Schema) || !identifier(t.Table) || len(t.Columns) == 0 || len(t.Columns) > 200 {
		return ErrConfig
	}
	switch strings.ToLower(t.Schema) {
	case "mysql", "information_schema", "performance_schema", "sys", "xdtsmeta":
		return ErrConfig
	}
	seen := map[string]bool{}
	for _, name := range t.Columns {
		key := strings.ToLower(name)
		if !identifier(name) || seen[key] {
			return ErrConfig
		}
		seen[key] = true
	}
	return nil
}

type bucketAccumulator struct {
	count      uint64
	keys, rows [32]byte
	columns    [][32]byte
}

func addDigest(sum *[32]byte, value [32]byte) {
	carry := uint16(0)
	for i := 31; i >= 0; i-- {
		n := uint16(sum[i]) + uint16(value[i]) + carry
		sum[i] = byte(n)
		carry = n >> 8
	}
}

func scalarDigest(key [32]byte, value []byte) [32]byte {
	h := sha256.New()
	h.Write(key[:])
	var length [9]byte
	if value != nil {
		length[0] = 1
		binary.BigEndian.PutUint64(length[1:], uint64(len(value)))
	}
	h.Write(length[:])
	h.Write(value)
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func (s *Scanner) Scan(ctx context.Context, table LocalTable, options Options) (DigestResult, error) {
	result := DigestResult{StartedAt: time.Now().UTC(), SampleModulo: options.SampleModulo}
	if s == nil || s.db == nil || table.Validate() != nil || options.SampleModulo == 0 || options.SampleModulo > 1000000 {
		return result, ErrConfig
	}
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return result, ErrRead
	}
	defer conn.Close()
	if _, err = conn.ExecContext(ctx, "SET SESSION time_zone='+00:00'"); err != nil {
		return result, ErrRead
	}
	tx, err := conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return result, ErrRead
	}
	defer tx.Rollback()
	cols, keys, err := metadata(ctx, tx, table.Schema, table.Table)
	if err != nil {
		return result, err
	}
	result.ColumnTypes = make([]string, len(table.Columns))
	jsonCols := make([]bool, len(table.Columns))
	result.KeyIndices = make([]int, len(keys))
	for i := range result.KeyIndices {
		result.KeyIndices[i] = -1
	}
	for index, name := range table.Columns {
		col, ok := cols[strings.ToLower(name)]
		if !ok {
			return result, ErrSchema
		}
		result.ColumnTypes[index] = col.fullType
		jsonCols[index] = col.kind == "json"
		for k, key := range keys {
			if strings.EqualFold(key, name) {
				result.KeyIndices[k] = index
			}
		}
	}
	for _, index := range result.KeyIndices {
		if index < 0 {
			return result, ErrSchema
		}
	}
	// Digest addition is independent of order; no sort or business write lock.
	selects := make([]string, len(table.Columns))
	for i, name := range table.Columns {
		selects[i] = "CAST(" + quoted(name) + " AS BINARY)"
	}
	query := "SELECT " + strings.Join(selects, ",") + " FROM " + quoted(table.Schema) + "." + quoted(table.Table)
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return result, ErrRead
	}
	defer rows.Close()
	stream := rowStream{rows: rows, keyIndex: result.KeyIndices, jsonCols: jsonCols, width: len(table.Columns), read: &result.Read}
	buckets := make([]bucketAccumulator, DigestBucketCount)
	for i := range buckets {
		buckets[i].columns = make([][32]byte, len(table.Columns))
	}
	for {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		row, err := stream.next()
		if err != nil {
			return result, err
		}
		if row == nil {
			break
		}
		key := digestKey(row.key)
		if binary.BigEndian.Uint64(key[:8])%options.SampleModulo != 0 {
			continue
		}
		bucket := &buckets[int(key[8])]
		bucket.count++
		result.Sampled++
		addDigest(&bucket.keys, key)
		h := sha256.New()
		h.Write(key[:])
		for index, value := range row.values {
			digest := scalarDigest(key, value)
			addDigest(&bucket.columns[index], digest)
			h.Write(digest[:])
		}
		var digest [32]byte
		copy(digest[:], h.Sum(nil))
		addDigest(&bucket.rows, digest)
	}
	result.Buckets = make([]DigestBucket, len(buckets))
	for i, bucket := range buckets {
		out := DigestBucket{Count: bucket.count, Keys: hex.EncodeToString(bucket.keys[:]), Rows: hex.EncodeToString(bucket.rows[:]), Columns: make([]string, len(table.Columns))}
		for c, value := range bucket.columns {
			out.Columns[c] = hex.EncodeToString(value[:])
		}
		result.Buckets[i] = out
	}
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func (r DigestResult) Validate(width int) error {
	if len(r.Buckets) != DigestBucketCount || len(r.ColumnTypes) != width || len(r.KeyIndices) == 0 || r.SampleModulo == 0 || r.Sampled > r.Read || r.StartedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) {
		return ErrRow
	}
	seen := map[int]bool{}
	for _, i := range r.KeyIndices {
		if i < 0 || i >= width || seen[i] {
			return ErrSchema
		}
		seen[i] = true
	}
	count := uint64(0)
	for _, b := range r.Buckets {
		if len(b.Columns) != width || count > r.Sampled || b.Count > r.Sampled-count {
			return ErrRow
		}
		count += b.Count
		for _, text := range append([]string{b.Keys, b.Rows}, b.Columns...) {
			raw, err := hex.DecodeString(text)
			if err != nil || len(raw) != 32 {
				return ErrRow
			}
		}
	}
	if count != r.Sampled {
		return ErrRow
	}
	return nil
}

func CompareDigests(table Table, source, target DigestResult) (DigestComparison, error) {
	out := DigestComparison{Table: table, Equal: true, ReadSource: source.Read, ReadTarget: target.Read, SampledSource: source.Sampled, SampledTarget: target.Sampled, SourceStartedAt: source.StartedAt, TargetStartedAt: target.StartedAt, Differences: []BucketDifference{}}
	if source.Validate(len(table.Columns)) != nil || target.Validate(len(table.Columns)) != nil {
		return out, ErrRow
	}
	if source.SampleModulo != target.SampleModulo || !slices.Equal(source.ColumnTypes, target.ColumnTypes) || !slices.Equal(source.KeyIndices, target.KeyIndices) {
		return out, ErrSchema
	}
	for i, left := range source.Buckets {
		right := target.Buckets[i]
		if left.Count == right.Count && left.Keys == right.Keys && left.Rows == right.Rows && slices.Equal(left.Columns, right.Columns) {
			continue
		}
		out.Equal = false
		d := BucketDifference{Bucket: i, SourceRows: left.Count, TargetRows: right.Count, KeysEqual: left.Count == right.Count && left.Keys == right.Keys, Columns: []string{}}
		for c, col := range table.Columns {
			if left.Columns[c] != right.Columns[c] {
				d.Columns = append(d.Columns, col.Source)
			}
		}
		out.Differences = append(out.Differences, d)
	}
	return out, nil
}
