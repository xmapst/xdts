package repo

// 注册补偿路径要能按已核销的注册码反查节点，而 nodes 与 node_register_codes
// 之间原本没有任何一列相连。这一列是那条归属关系的唯一表达处。
//
// 允许 NULL：人工建节点没有注册码，v47 之前注册的存量节点也没有这个信息。
// 红线「已发布的契约只增不改语义」⇒ 新增可空列，不改写 v1 的建表语句。
func nodeRegisterCodeIDFieldMigration() migration {
	return migration{
		Version:     47,
		Description: "add nodes register_code_id",
		Statements: []string{
			"ALTER TABLE nodes ADD COLUMN register_code_id BINARY(16) NULL, ADD KEY idx_nodes_register_code_id (register_code_id)",
		},
	}
}
