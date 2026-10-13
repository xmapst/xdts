package schema

import (
	"strings"

	"github.com/xmapst/xdts/internal/schemamig"
)

// 结构快照由 xdoc 使用 Go 字段名保存；修改字段名必须显式处理存档。

// ColumnStructure 保存生成列判定及缺失 TableMap 可选元数据时的解码属性。
// 已发布的 Name/Generated/GeneratedStored 磁盘键保持不变；新增属性由
// TableStructure.MetadataComplete 声明完整性，旧快照不能被当作完整元数据。
type ColumnStructure struct {
	Name       string   `json:"name"`
	MySQLType  byte     `json:"mysql_type"`
	Unsigned   bool     `json:"unsigned"`
	EnumValues []string `json:"enum_values,omitempty"`
	SetValues  []string `json:"set_values,omitempty"`
	// Generated 为 true 时该列是生成列（GENERATED ALWAYS AS(...)）。
	// GeneratedStored 区分 STORED（true）与 VIRTUAL（false）；两者在
	// NODE-BIN-06 里处理方式相同（都要剔除），保留这个字段只是方便排障/展示
	// 时看出具体种类，不影响 Tracker.IsGenerated 的判定结果。
	Generated       bool `json:"generated"`
	GeneratedStored bool `json:"generated_stored"`
}

// UniqueKey 保存 DDL 解析得到的唯一索引及列名，Name 为空表示主键。
// 全量规划另行读取 information_schema，校验 nullable、类型和索引完整性。
type UniqueKey struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
}

// TableStructure 是本包对一张表结构在某个时间点的完整认知快照
// （"schema_history"：表结构版本及生效位点）。
type TableStructure struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	// Only complete ordered definitions may supply absent TableMap column names.
	MetadataComplete bool `json:"metadata_complete"`
	// Original named dependencies precede column projection. Incomplete records
	// cannot authorize named ALTER operations.
	DDLDependencies schemamig.TableDependencies `json:"ddl_dependencies"`
	// Columns 在完整 CREATE/目录加载后按物理顺序排列，后续 FIRST/AFTER
	// 与 CHANGE/MODIFY 维护该顺序；事件自带列名仍优先。
	Columns    []ColumnStructure `json:"columns"`
	PrimaryKey []string          `json:"primary_key,omitempty"`
	UniqueKeys []UniqueKey       `json:"unique_keys,omitempty"`
	// UpdatedGTID/UpdatedAtMs 记录最近一次让这张表结构发生变化的 DDL 事务
	// （"表结构版本及生效位点"里的"生效位点"）。
	UpdatedGTID string `json:"updated_gtid,omitempty"`
	UpdatedAtMs int64  `json:"updated_at_ms,omitempty"`
}

// columnIndex 按列名查找列。固定用 strings.EqualFold，不接 caseSensitive
// 参数——MySQL 列名的大小写比较不受 lower_case_table_names 约束（该变量只
// 管库名/表名，见 internal/node/schema/tracker.go Tracker.caseSensitive 字段文档），这一点与
// internal/schemamig/ident.go foldKey（列名折叠，同样不接 caseSensitive）
// 是同一条约定，不是本函数遗漏接线。下面 removeUniqueKeyByName（索引名）、
// removeFromSlice/renameInSlice（主键/唯一键里的列名）是同一类判断，同一个
// 理由，不逐条重复。
func (t TableStructure) columnIndex(name string) int {
	for i, c := range t.Columns {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}
	return -1
}

// clone 返回一份列表/切片互不共享底层数组的深拷贝，供 Snapshot/Restore 使用，
// 避免持久化路径持有的数据被 Tracker 后续的原地修改污染（见 internal/node/schema/tracker.go
// upsertColumn/removeColumn 等就地修改切片的实现）。
func (t TableStructure) clone() TableStructure {
	t.DDLDependencies = t.DDLDependencies.Clone()
	if len(t.Columns) > 0 {
		cols := make([]ColumnStructure, len(t.Columns))
		copy(cols, t.Columns)
		for i := range cols {
			cols[i].EnumValues = append([]string(nil), cols[i].EnumValues...)
			cols[i].SetValues = append([]string(nil), cols[i].SetValues...)
		}
		t.Columns = cols
	}
	if len(t.PrimaryKey) > 0 {
		t.PrimaryKey = append([]string(nil), t.PrimaryKey...)
	}
	if len(t.UniqueKeys) > 0 {
		uks := make([]UniqueKey, len(t.UniqueKeys))
		for i, uk := range t.UniqueKeys {
			uks[i] = UniqueKey{Name: uk.Name, Columns: append([]string(nil), uk.Columns...)}
		}
		t.UniqueKeys = uks
	}
	return t
}

func (t *TableStructure) upsertColumn(cs ColumnStructure) {
	if i := t.columnIndex(cs.Name); i >= 0 {
		t.Columns[i] = cs
		return
	}
	t.Columns = append(t.Columns, cs)
}

func (t *TableStructure) removeColumn(name string) {
	if i := t.columnIndex(name); i >= 0 {
		t.Columns = append(t.Columns[:i], t.Columns[i+1:]...)
	}
	t.PrimaryKey = removeFromSlice(t.PrimaryKey, name)
	for i := range t.UniqueKeys {
		t.UniqueKeys[i].Columns = removeFromSlice(t.UniqueKeys[i].Columns, name)
	}
}

func (t *TableStructure) renameColumn(oldName, newName string) {
	if i := t.columnIndex(oldName); i >= 0 {
		t.Columns[i].Name = newName
	}
	renameInSlice(t.PrimaryKey, oldName, newName)
	for i := range t.UniqueKeys {
		renameInSlice(t.UniqueKeys[i].Columns, oldName, newName)
	}
}

// removeUniqueKeyByName 按索引名删除唯一键。固定 strings.EqualFold，不接
// caseSensitive——索引名不是 lower_case_table_names 的管辖范围，理由同
// columnIndex 文档。
func (t *TableStructure) removeUniqueKeyByName(name string) {
	kept := t.UniqueKeys[:0:0]
	for _, uk := range t.UniqueKeys {
		if !strings.EqualFold(uk.Name, name) {
			kept = append(kept, uk)
		}
	}
	t.UniqueKeys = kept
}

// removeFromSlice 从主键/唯一键的列名列表里按名字删除一列。固定
// strings.EqualFold，不接 caseSensitive，理由同 columnIndex 文档——这里的
// name 是列名，不是库名/表名。
func removeFromSlice(s []string, name string) []string {
	if len(s) == 0 {
		return s
	}
	kept := s[:0:0]
	for _, v := range s {
		if !strings.EqualFold(v, name) {
			kept = append(kept, v)
		}
	}
	return kept
}

// renameInSlice 在主键/唯一键的列名列表里原地改名。固定 strings.EqualFold，
// 不接 caseSensitive，理由同 columnIndex 文档——这里的 oldName/newName 是
// 列名，不是库名/表名。
func renameInSlice(s []string, oldName, newName string) {
	for i, v := range s {
		if strings.EqualFold(v, oldName) {
			s[i] = newName
		}
	}
}
