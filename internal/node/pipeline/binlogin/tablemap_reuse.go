package binlogin

import (
	"bytes"
	"slices"

	"github.com/go-mysql-org/go-mysql/replication"
)

// These independently copied fields belong only to the current original GTID.
// Include every exported TableMap attribute so metadata changes force a rebuild.
func cloneTableMapShape(e *replication.TableMapEvent) *replication.TableMapEvent {
	out := replication.TableMapEvent{TableID: e.TableID, Flags: e.Flags, ColumnCount: e.ColumnCount}
	out.Schema, out.Table = bytes.Clone(e.Schema), bytes.Clone(e.Table)
	out.ColumnType, out.NullBitmap = bytes.Clone(e.ColumnType), bytes.Clone(e.NullBitmap)
	out.ColumnMeta = slices.Clone(e.ColumnMeta)
	out.SignednessBitmap = bytes.Clone(e.SignednessBitmap)
	out.DefaultCharset, out.ColumnCharset = slices.Clone(e.DefaultCharset), slices.Clone(e.ColumnCharset)
	out.SetStrValue, out.EnumStrValue = cloneTableMapStrings(e.SetStrValue), cloneTableMapStrings(e.EnumStrValue)
	out.ColumnName = make([][]byte, len(e.ColumnName))
	for i := range e.ColumnName {
		out.ColumnName[i] = bytes.Clone(e.ColumnName[i])
	}
	out.GeometryType = slices.Clone(e.GeometryType)
	out.PrimaryKey, out.PrimaryKeyPrefix = slices.Clone(e.PrimaryKey), slices.Clone(e.PrimaryKeyPrefix)
	out.EnumSetDefaultCharset, out.EnumSetColumnCharset = slices.Clone(e.EnumSetDefaultCharset), slices.Clone(e.EnumSetColumnCharset)
	out.VisibilityBitmap = bytes.Clone(e.VisibilityBitmap)
	return &out
}

func cloneTableMapStrings(values [][][]byte) [][][]byte {
	out := make([][][]byte, len(values))
	for i, group := range values {
		out[i] = make([][]byte, len(group))
		for j, value := range group {
			out[i][j] = bytes.Clone(value)
		}
	}
	return out
}

func tableMapStringsEqual(a, b [][][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !slices.EqualFunc(a[i], b[i], bytes.Equal) {
			return false
		}
	}
	return true
}

func tableMapShapesEqual(a, b *replication.TableMapEvent) bool {
	return a != nil && b != nil && a.TableID == b.TableID && a.Flags == b.Flags && a.ColumnCount == b.ColumnCount &&
		bytes.Equal(a.Schema, b.Schema) && bytes.Equal(a.Table, b.Table) && bytes.Equal(a.ColumnType, b.ColumnType) &&
		slices.Equal(a.ColumnMeta, b.ColumnMeta) && bytes.Equal(a.NullBitmap, b.NullBitmap) && bytes.Equal(a.SignednessBitmap, b.SignednessBitmap) &&
		slices.Equal(a.DefaultCharset, b.DefaultCharset) && slices.Equal(a.ColumnCharset, b.ColumnCharset) &&
		tableMapStringsEqual(a.SetStrValue, b.SetStrValue) && tableMapStringsEqual(a.EnumStrValue, b.EnumStrValue) &&
		slices.EqualFunc(a.ColumnName, b.ColumnName, bytes.Equal) && slices.Equal(a.GeometryType, b.GeometryType) &&
		slices.Equal(a.PrimaryKey, b.PrimaryKey) && slices.Equal(a.PrimaryKeyPrefix, b.PrimaryKeyPrefix) &&
		slices.Equal(a.EnumSetDefaultCharset, b.EnumSetDefaultCharset) && slices.Equal(a.EnumSetColumnCharset, b.EnumSetColumnCharset) &&
		bytes.Equal(a.VisibilityBitmap, b.VisibilityBitmap)
}

// Charge the copied arrays and nested slice headers before retaining a shape.
func tableMapShapeBytes(e *replication.TableMapEvent) uint64 {
	cost := uint64(1024 + len(e.Schema) + len(e.Table) + len(e.ColumnType) + len(e.NullBitmap) + len(e.SignednessBitmap) + len(e.VisibilityBitmap))
	cost += uint64(len(e.ColumnMeta)) * 2
	for _, values := range [][]uint64{e.DefaultCharset, e.ColumnCharset, e.GeometryType, e.PrimaryKey, e.PrimaryKeyPrefix, e.EnumSetDefaultCharset, e.EnumSetColumnCharset} {
		cost += uint64(len(values)) * 8
	}
	for _, value := range e.ColumnName {
		cost += 24 + uint64(len(value))
	}
	for _, groups := range [][][][]byte{e.SetStrValue, e.EnumStrValue} {
		for _, group := range groups {
			cost += 24
			for _, value := range group {
				cost += 24 + uint64(len(value))
			}
		}
	}
	return cost
}
