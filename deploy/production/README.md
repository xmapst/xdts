# 生产配置示例

`manager.yaml.example` 和 `node.yaml.example` 是与当前配置加载器保持同步的 YAML 模板。复制后再按环境修改，不要直接把示例文件当作生产密钥存储。

配置加载顺序固定为：配置文件、`XDTS_*` 环境变量、命令行参数。环境变量名称由配置键的大写下划线形式生成，例如：

```sh
export XDTS_DATABASE_DSN='xdts_manager:***@tcp(127.0.0.1:3306)/xdts_manager?parseTime=true&loc=UTC'
export XDTS_AMQP_URL='amqps://xdts_manager:***@rabbitmq.example.com:5671/xdts'
export XDTS_AUTH_LOCAL_BOOTSTRAP_USER='admin'
export XDTS_AUTH_LOCAL_BOOTSTRAP_PASSWORD='use-a-secret-store'
export XDTS_NODE_REGISTER_CODE='one-time-node-code'
```

Manager 的 `database.dsn` 指向独立管理库，Node 不配置管理库 DSN。Node 的 `node.data_dir` 必须是持久化目录；注册码既是注册凭据，也是本地 xdoc 状态库口令，丢失或更换后不能解密原状态库。

保留期使用标准 duration（`360h`、`720h`）；日志使用 `json` 或 `console`，级别使用 `debug`、`info`、`warn`、`error`。Manager 与 Node 必须使用相同协议版本；旧状态恢复和升级边界见 [运行维护](../../docs/operations.md)。

如果启用 LDAP 或 OIDC，先将对应 `enabled` 设为 `true`，再补齐该认证方式要求的字段。所有认证方式都关闭会阻止 Manager 启动；本地账号表为空时还必须提供 bootstrap 用户名和密码。
