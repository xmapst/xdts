package binlogin

import (
	"fmt"
	"slices"

	gmysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/xmapst/xdts/internal/iproto"
	"github.com/xmapst/xdts/internal/mysqlx"
)

// rowsEventChangeType 把顶层 BinlogEvent.Header.EventType 映射为 ChangeType。
func rowsEventChangeType(eventType replication.EventType) (iproto.ChangeType, bool) {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return iproto.ChangeType_CHANGE_TYPE_INSERT, true
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2,
		replication.PARTIAL_UPDATE_ROWS_EVENT:
		return iproto.ChangeType_CHANGE_TYPE_UPDATE, true
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return iproto.ChangeType_CHANGE_TYPE_DELETE, true
	default:
		return iproto.ChangeType_CHANGE_TYPE_UNSPECIFIED, false
	}
}

// 行变更要求 binlog_row_image=FULL；缺失必须的列值或元数据时拒绝聚合，
// 防止错误的镜像进入数据面。
func buildRowChanges(e *replication.RowsEvent, changeType iproto.ChangeType, info *tableMapInfo, columnFilter IColumnFilter) ([]*iproto.RowChange, error) {
	projection, err := buildSourceRowProjection(info, columnFilter)
	if err != nil {
		return nil, err
	}
	return buildProjectedRowChanges(e, changeType, info, projection)
}

type sourceRowProjection struct {
	names []string
	keep  []int
	keys  []string
}

func buildSourceRowProjection(info *tableMapInfo, columnFilter IColumnFilter) (*sourceRowProjection, error) {
	columnNames, keepIdx := filterColumns(info, columnFilter)
	primaryKeyColumns := primaryKeyColumnNames(info, keepIdx)
	if len(primaryKeyColumns) != len(info.primaryKeyIdx) {
		return nil, fmt.Errorf("binlogin: column projection excludes part of the positioning key")
	}
	if len(columnNames) == 0 {
		return nil, fmt.Errorf("binlogin: column projection excludes every column")
	}
	return &sourceRowProjection{names: columnNames, keep: keepIdx, keys: primaryKeyColumns}, nil
}

func buildProjectedRowChanges(e *replication.RowsEvent, changeType iproto.ChangeType, info *tableMapInfo, projection *sourceRowProjection) ([]*iproto.RowChange, error) {
	columnNames, keepIdx, primaryKeyColumns := projection.names, projection.keep, projection.keys
	for _, skipped := range e.SkippedColumns {
		for _, index := range skipped {
			if slices.Contains(keepIdx, index) {
				return nil, fmt.Errorf("binlogin: incomplete source row image for %s.%s column %q; require binlog_row_image=FULL", info.schema, info.table, info.meta[index].Name)
			}
		}
	}

	switch changeType {
	case iproto.ChangeType_CHANGE_TYPE_INSERT:
		out := make([]*iproto.RowChange, 0, len(e.Rows))
		for _, raw := range e.Rows {
			after, err := decodeRow(raw, info, keepIdx)
			if err != nil {
				return nil, err
			}
			out = append(out, &iproto.RowChange{
				Type:              iproto.ChangeType_CHANGE_TYPE_INSERT,
				Schema:            info.schema,
				Table:             info.table,
				ColumnNames:       columnNames,
				PrimaryKeyColumns: primaryKeyColumns,
				After:             after,
			})
		}
		return out, nil

	case iproto.ChangeType_CHANGE_TYPE_DELETE:
		out := make([]*iproto.RowChange, 0, len(e.Rows))
		for _, raw := range e.Rows {
			before, err := decodeRow(raw, info, keepIdx)
			if err != nil {
				return nil, err
			}
			out = append(out, &iproto.RowChange{
				Type:              iproto.ChangeType_CHANGE_TYPE_DELETE,
				Schema:            info.schema,
				Table:             info.table,
				ColumnNames:       columnNames,
				PrimaryKeyColumns: primaryKeyColumns,
				Before:            before,
			})
		}
		return out, nil

	case iproto.ChangeType_CHANGE_TYPE_UPDATE:
		// UPDATE_ROWS_EVENTv1/v2/PARTIAL_UPDATE_ROWS_EVENT 都是 before/after 成对出现
		// （Rows[2k]=前镜像，Rows[2k+1]=后镜像），已通过源码核实（github.com/go-mysql-org/go-mysql/replication/row_event.go 的
		// DecodeData 对 needBitmap2=true 的事件类型固定按此顺序追加）。
		if len(e.Rows)%2 != 0 {
			return nil, fmt.Errorf("binlogin: UPDATE RowsEvent 行数为奇数（%d），无法按前后镜像配对", len(e.Rows))
		}
		out := make([]*iproto.RowChange, 0, len(e.Rows)/2)
		for i := 0; i < len(e.Rows); i += 2 {
			before, err := decodeRow(e.Rows[i], info, keepIdx)
			if err != nil {
				return nil, err
			}
			after, err := decodeRow(e.Rows[i+1], info, keepIdx)
			if err != nil {
				return nil, err
			}
			out = append(out, &iproto.RowChange{
				Type:              iproto.ChangeType_CHANGE_TYPE_UPDATE,
				Schema:            info.schema,
				Table:             info.table,
				ColumnNames:       columnNames,
				PrimaryKeyColumns: primaryKeyColumns,
				Before:            before,
				After:             after,
			})
		}
		return out, nil

	default:
		return nil, fmt.Errorf("binlogin: 不支持的 ChangeType: %s", changeType)
	}
}

// filterColumns 依据 IColumnFilter 计算参与同步的列名及其在原始列序列中的下标。
func filterColumns(info *tableMapInfo, columnFilter IColumnFilter) (names []string, keepIdx []int) {
	names = make([]string, 0, len(info.meta))
	keepIdx = make([]int, 0, len(info.meta))
	for i, cm := range info.meta {
		if !columnFilter.Allow(info.schema, info.table, cm.Name) {
			continue
		}
		names = append(names, cm.Name)
		keepIdx = append(keepIdx, i)
	}
	return names, keepIdx
}

// primaryKeyColumnNames 把 TableMapEvent.PrimaryKey（列下标）映射为列名，只保留同时
// 通过 columnFilter 的那些。unique_keys（非主键唯一键）恒不填充——见 internal/node/pipeline/binlogin/interfaces.go
// 对该范围边界的说明。
func primaryKeyColumnNames(info *tableMapInfo, keepIdx []int) []string {
	if len(info.primaryKeyIdx) == 0 {
		return nil
	}
	kept := make(map[int]bool, len(keepIdx))
	for _, i := range keepIdx {
		kept[i] = true
	}
	out := make([]string, 0, len(info.primaryKeyIdx))
	for _, idx := range info.primaryKeyIdx {
		i := int(idx)
		if i < 0 || i >= len(info.meta) || !kept[i] {
			continue
		}
		out = append(out, info.meta[i].Name)
	}
	return out
}

func decodeRow(raw []any, info *tableMapInfo, keepIdx []int) (*iproto.Row, error) {
	if len(raw) != len(info.meta) {
		return nil, fmt.Errorf("binlogin: 表 %s.%s 的行数据列数（%d）与列元数据列数（%d）不一致，"+
			"要求源库 binlog_row_image=FULL", info.schema, info.table, len(raw), len(info.meta))
	}
	values := make([]*iproto.ColumnValue, 0, len(keepIdx))
	for _, i := range keepIdx {
		if _, ok := raw[i].(*replication.JsonDiff); ok {
			return nil, fmt.Errorf("binlogin: 表 %s.%s 列 %q 的部分 JSON 更新未完整重建",
				info.schema, info.table, info.meta[i].Name)
		}
		cv, err := mysqlx.DecodeColumnValue(normalizeUnsigned(raw[i], info.meta[i]), info.meta[i])
		if err != nil {
			return nil, fmt.Errorf("binlogin: 表 %s.%s 列 %q 解码失败: %w",
				info.schema, info.table, info.meta[i].Name, err)
		}
		values = append(values, cv)
	}
	return &iproto.Row{Values: values}, nil
}

// Without optional signedness metadata go-mysql decodes unsigned integers as
// signed values. Reinterpret the original width using the restored definition.
func normalizeUnsigned(raw any, meta mysqlx.ColumnMeta) any {
	if !meta.Unsigned || raw == nil {
		return raw
	}
	var value uint64
	switch v := raw.(type) {
	case int8:
		value = uint64(v)
	case int16:
		value = uint64(v)
	case int32:
		value = uint64(v)
	case int64:
		value = uint64(v)
	case int:
		value = uint64(v)
	case uint8:
		return uint64(v)
	case uint16:
		return uint64(v)
	case uint32:
		return uint64(v)
	default:
		return raw
	}
	switch meta.MySQLType {
	case gmysql.MYSQL_TYPE_TINY:
		return value & 0xff
	case gmysql.MYSQL_TYPE_SHORT:
		return value & 0xffff
	case gmysql.MYSQL_TYPE_INT24:
		return value & 0xffffff
	case gmysql.MYSQL_TYPE_LONG:
		return value & 0xffffffff
	case gmysql.MYSQL_TYPE_LONGLONG:
		return value
	default:
		return raw
	}
}
