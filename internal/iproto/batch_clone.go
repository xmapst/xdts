package iproto

import (
	"bytes"
	"slices"

	"google.golang.org/protobuf/proto"
)

// CloneDataBatch gives its caller independent ownership of all mutable data.
// The hot row messages use typed copies; less frequent policies and DDL keep
// the protobuf implementation. Adding a protocol field disables this fast path
// until its copy has been explicitly implemented.
func CloneDataBatch(in *DataBatch) *DataBatch {
	if in == nil {
		return nil
	}
	if in.ProtoReflect().Descriptor().Fields().Len() != 10 ||
		(*Txn)(nil).ProtoReflect().Descriptor().Fields().Len() != 12 ||
		(*RowChange)(nil).ProtoReflect().Descriptor().Fields().Len() != 8 ||
		(*Row)(nil).ProtoReflect().Descriptor().Fields().Len() != 1 ||
		(*ColumnValue)(nil).ProtoReflect().Descriptor().Fields().Len() != 9 ||
		(*UniqueKeyColumns)(nil).ProtoReflect().Descriptor().Fields().Len() != 1 {
		return proto.Clone(in).(*DataBatch)
	}
	out := &DataBatch{PipelineId: in.PipelineId, RunId: in.RunId, Epoch: in.Epoch,
		BatchSeq: in.BatchSeq, BeginGtidSet: in.BeginGtidSet, EndGtidSet: in.EndGtidSet,
		PublicationWindowFirstSeq: in.PublicationWindowFirstSeq,
		unknownFields:             bytes.Clone(in.unknownFields)}
	if in.Chunk != nil {
		out.Chunk = proto.Clone(in.Chunk).(*Chunk)
	}
	if in.PublicationWindowPolicy != nil {
		out.PublicationWindowPolicy = proto.Clone(in.PublicationWindowPolicy).(*IncrementalWindowPolicy)
	}
	if in.Txns != nil {
		out.Txns = make([]*Txn, len(in.Txns))
		values := make([]Txn, len(in.Txns))
		for i, t := range in.Txns {
			if t == nil {
				continue
			}
			c := &values[i]
			c.Gtid, c.OriginCenter, c.Hops = t.Gtid, t.OriginCenter, t.Hops
			c.CommitTsMs, c.LastCommitted, c.SequenceNumber = t.CommitTsMs, t.LastCommitted, t.SequenceNumber
			c.Path, c.EndpointPath, c.LineageVersion = slices.Clone(t.Path), slices.Clone(t.EndpointPath), t.LineageVersion
			c.unknownFields = bytes.Clone(t.unknownFields)
			if t.Ddl != nil {
				c.Ddl = proto.Clone(t.Ddl).(*DDL)
			}
			if t.ShadowTablePolicy != nil {
				c.ShadowTablePolicy = proto.Clone(t.ShadowTablePolicy).(*ShadowTablePolicy)
			}
			c.Rows = cloneBatchChanges(t.Rows)
			out.Txns[i] = c
		}
	}
	return out
}

func cloneBatchChanges(in []*RowChange) []*RowChange {
	if in == nil {
		return nil
	}
	out := make([]*RowChange, len(in))
	values := make([]RowChange, len(in))
	for i, r := range in {
		if r == nil {
			continue
		}
		c := &values[i]
		c.Type, c.Schema, c.Table = r.Type, r.Schema, r.Table
		c.ColumnNames, c.PrimaryKeyColumns = slices.Clone(r.ColumnNames), slices.Clone(r.PrimaryKeyColumns)
		c.unknownFields = bytes.Clone(r.unknownFields)
		if r.UniqueKeys != nil {
			c.UniqueKeys = make([]*UniqueKeyColumns, len(r.UniqueKeys))
			keys := make([]UniqueKeyColumns, len(r.UniqueKeys))
			for k, key := range r.UniqueKeys {
				if key != nil {
					keys[k].Columns, keys[k].unknownFields = slices.Clone(key.Columns), bytes.Clone(key.unknownFields)
					c.UniqueKeys[k] = &keys[k]
				}
			}
		}
		c.Before, c.After = cloneBatchRow(r.Before), cloneBatchRow(r.After)
		out[i] = c
	}
	return out
}

func cloneBatchRow(in *Row) *Row {
	if in == nil {
		return nil
	}
	out := &Row{unknownFields: bytes.Clone(in.unknownFields)}
	if in.Values == nil {
		return out
	}
	out.Values = make([]*ColumnValue, len(in.Values))
	values := make([]ColumnValue, len(in.Values))
	for i, v := range in.Values {
		if v == nil {
			continue
		}
		c := &values[i]
		c.ZeroDate, c.unknownFields = v.ZeroDate, bytes.Clone(v.unknownFields)
		switch x := v.Value.(type) {
		case *ColumnValue_IsNull:
			if x != nil {
				c.Value = &ColumnValue_IsNull{IsNull: x.IsNull}
			}
		case *ColumnValue_I64:
			if x != nil {
				c.Value = &ColumnValue_I64{I64: x.I64}
			}
		case *ColumnValue_U64:
			if x != nil {
				c.Value = &ColumnValue_U64{U64: x.U64}
			}
		case *ColumnValue_F64:
			if x != nil {
				c.Value = &ColumnValue_F64{F64: x.F64}
			}
		case *ColumnValue_Decimal:
			if x != nil {
				c.Value = &ColumnValue_Decimal{Decimal: x.Decimal}
			}
		case *ColumnValue_RawBytes:
			if x != nil {
				c.Value = &ColumnValue_RawBytes{RawBytes: bytes.Clone(x.RawBytes)}
			}
		case *ColumnValue_Text:
			if x != nil {
				c.Value = &ColumnValue_Text{Text: x.Text}
			}
		case *ColumnValue_TemporalUsec:
			if x != nil {
				c.Value = &ColumnValue_TemporalUsec{TemporalUsec: x.TemporalUsec}
			}
		}
		out.Values[i] = c
	}
	return out
}
