package mysqlx

// Caps 是 MySQL 实例的能力集合，由 DetectCaps 从版本号计算。
// 调用方缓存和下发探测结果；本包不持有实例状态。
type Caps struct {
	RowMetadataFull    bool // 8.0.1+：Table_map 携带列名与主键元数据
	SessionVarAdmin    bool // 8.0.14+：可用 SESSION_VARIABLES_ADMIN 设置 gtid_next
	ReplicationApplier bool // 8.0.18+：可用 REPLICATION_APPLIER 角色
	ShowRoutinePriv    bool // 8.0.20+：SHOW_ROUTINE 权限读取例程定义
	TxnCompression     bool // 8.0.20+：可能出现 Transaction_payload 事件
	InvisibleColumn    bool // 8.0.23+
	GIPK               bool // 8.0.30+
	SetAnyDefiner      bool // 8.2+（更早用 SET_USER_ID）
	BinlogStatusStmt   bool // 8.4+：SHOW BINARY LOG STATUS 取代 SHOW MASTER STATUS
	VectorType         bool // 9.0+
}

// 各项能力的起始版本集中维护，供能力检测和 SQL 选择共用。
var (
	verRowMetadataFull    = Version{8, 0, 1}
	verSessionVarAdmin    = Version{8, 0, 14}
	verReplicationApplier = Version{8, 0, 18}
	verShowRoutinePriv    = Version{8, 0, 20}
	verTxnCompression     = Version{8, 0, 20}
	verInvisibleColumn    = Version{8, 0, 23}
	verGIPK               = Version{8, 0, 30}
	verSetAnyDefiner      = Version{8, 2, 0}
	verBinlogStatusStmt   = Version{8, 4, 0}
	verVectorType         = Version{9, 0, 0}
)

// DetectCaps 按各项能力的起始版本计算能力集合。
func DetectCaps(ver Version) Caps {
	return Caps{
		RowMetadataFull:    ver.AtLeast(verRowMetadataFull),
		SessionVarAdmin:    ver.AtLeast(verSessionVarAdmin),
		ReplicationApplier: ver.AtLeast(verReplicationApplier),
		ShowRoutinePriv:    ver.AtLeast(verShowRoutinePriv),
		TxnCompression:     ver.AtLeast(verTxnCompression),
		InvisibleColumn:    ver.AtLeast(verInvisibleColumn),
		GIPK:               ver.AtLeast(verGIPK),
		SetAnyDefiner:      ver.AtLeast(verSetAnyDefiner),
		BinlogStatusStmt:   ver.AtLeast(verBinlogStatusStmt),
		VectorType:         ver.AtLeast(verVectorType),
	}
}
