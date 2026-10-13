package snapshot

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
)

// PlanTable performs bounded database requests through reader. The caller must
// supply a cancellable workflow context; every query is a separate short read.
func PlanTable(ctx context.Context, table Table, chunkRows uint64, reader IBoundaryReader) (TablePlan, error) {
	if chunkRows == 0 || reader == nil {
		return TablePlan{}, fmt.Errorf("snapshot: chunk rows and boundary reader required")
	}
	key, err := ChooseKey(table)
	if err != nil {
		return TablePlan{}, err
	}
	baseline := table
	plan := TablePlan{Schema: table.Schema, Table: table.Name, Key: key, ChunkCount: 1, Baseline: &baseline}
	plan = plan.clone()
	if len(key.Columns) == 1 && integerBits(key.Columns[0].DataType) != 0 {
		min, max, err := reader.IntegerBounds(ctx, table, key)
		if err != nil {
			return TablePlan{}, err
		}
		return integerPlan(plan, min, max, table.EstimatedRows, chunkRows)
	}
	plan.Strategy = StrategyKeyset
	var after Boundary
	for {
		if err := ctx.Err(); err != nil {
			return TablePlan{}, err
		}
		page, err := reader.Page(ctx, table, key, after.clone(), chunkRows)
		if err != nil {
			return TablePlan{}, err
		}
		if page.Rows > chunkRows || (page.Rows > 0 && !validBoundary(page.Last, key)) {
			return TablePlan{}, fmt.Errorf("snapshot: invalid keyset page")
		}
		if page.Rows == 0 {
			break
		}
		if sameBoundary(after, page.Last) {
			return TablePlan{}, fmt.Errorf("snapshot: keyset page did not advance")
		}
		if page.Rows < chunkRows {
			break
		}
		if plan.ChunkCount == maxChunkNumber {
			return TablePlan{}, fmt.Errorf("snapshot: chunk number exceeds MySQL GTID range")
		}
		after = page.Last.clone()
		plan.Boundaries = append(plan.Boundaries, after)
		plan.ChunkCount++
	}
	return plan, nil
}

func integerPlan(plan TablePlan, min, max string, estimated, chunkRows uint64) (TablePlan, error) {
	plan.Strategy = StrategyInteger
	if min == "" && max == "" {
		return plan, nil
	}
	lo, err := parseInteger(min, plan.Key.Columns[0])
	if err != nil {
		return TablePlan{}, err
	}
	hi, err := parseInteger(max, plan.Key.Columns[0])
	if err != nil || lo.Cmp(hi) > 0 {
		return TablePlan{}, fmt.Errorf("snapshot: invalid integer extrema")
	}
	count := estimated / chunkRows
	if estimated%chunkRows != 0 {
		count++
	}
	if count == 0 {
		count = 1
	}
	width := new(big.Int).Sub(hi, lo)
	width.Add(width, big.NewInt(1))
	step := ceilDiv(width, new(big.Int).SetUint64(count))
	actual := ceilDiv(width, step)
	if !actual.IsUint64() || actual.Uint64() > maxChunkNumber {
		return TablePlan{}, fmt.Errorf("snapshot: chunk number exceeds MySQL GTID range")
	}
	plan.ChunkCount = actual.Uint64()
	plan.Minimum, plan.Maximum, plan.Step = lo.String(), hi.String(), step.String()
	return plan, nil
}

func ceilDiv(n, d *big.Int) *big.Int {
	v := new(big.Int).Sub(d, big.NewInt(1))
	v.Add(v, n)
	return v.Div(v, d)
}

func parseInteger(text string, column Column) (*big.Int, error) {
	v, ok := new(big.Int).SetString(text, 10)
	bits := integerBits(column.DataType)
	if !ok || bits == 0 || v.String() != text {
		return nil, fmt.Errorf("snapshot: invalid integer boundary")
	}
	limit := new(big.Int).Lsh(big.NewInt(1), uint(bits))
	if column.Unsigned {
		if v.Sign() < 0 || v.Cmp(limit) >= 0 {
			return nil, fmt.Errorf("snapshot: unsigned boundary out of range")
		}
	} else {
		limit.Rsh(limit, 1)
		if v.Cmp(new(big.Int).Neg(limit)) < 0 || v.Cmp(limit) >= 0 {
			return nil, fmt.Errorf("snapshot: signed boundary out of range")
		}
	}
	return v, nil
}

// Chunk computes one chunk without allocating the other integer ranges.
func (p TablePlan) Chunk(index uint64) (Chunk, error) {
	if index >= p.ChunkCount || index > maxChunkNumber || p.FirstChunk == 0 || p.FirstChunk > maxChunkNumber-index {
		return Chunk{}, fmt.Errorf("snapshot: chunk index out of range")
	}
	c := Chunk{Number: p.FirstChunk + index, Schema: p.Schema, Table: p.Table,
		Key: Key{Name: p.Key.Name, Columns: append([]Column(nil), p.Key.Columns...)}, Strategy: p.Strategy}
	switch p.Strategy {
	case StrategyInteger:
		if p.ChunkCount > 1 {
			lo, okLo := new(big.Int).SetString(p.Minimum, 10)
			step, okStep := new(big.Int).SetString(p.Step, 10)
			if !okLo || !okStep || step.Sign() <= 0 {
				return Chunk{}, fmt.Errorf("snapshot: invalid integer plan")
			}
			boundary := func(offset uint64) Boundary {
				v := new(big.Int).Mul(step, new(big.Int).SetUint64(offset))
				v.Add(v, lo)
				return Boundary{Values: [][]byte{[]byte(v.String())}}
			}
			if index > 0 {
				c.Lower = boundary(index)
			}
			if index+1 < p.ChunkCount {
				c.Upper = boundary(index + 1)
			}
		}
	case StrategyKeyset:
		if uint64(len(p.Boundaries))+1 != p.ChunkCount {
			return Chunk{}, fmt.Errorf("snapshot: invalid keyset plan")
		}
		if index > 0 {
			c.Lower = p.Boundaries[index-1].clone()
		}
		if index+1 < p.ChunkCount {
			c.Upper = p.Boundaries[index].clone()
		}
	default:
		return Chunk{}, fmt.Errorf("snapshot: unknown planning strategy")
	}
	return c, nil
}

func validBoundary(b Boundary, key Key) bool {
	if len(b.Values) != len(key.Columns) {
		return false
	}
	for _, value := range b.Values {
		if value == nil {
			return false
		}
	}
	return true
}

func sameBoundary(a, b Boundary) bool {
	if len(a.Values) != len(b.Values) {
		return false
	}
	for i := range a.Values {
		if !bytes.Equal(a.Values[i], b.Values[i]) {
			return false
		}
	}
	return true
}
