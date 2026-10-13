# 运行维护

## 启动和关闭顺序

Manager 的启动顺序是：加载并校验配置 → 打开管理库 → 执行迁移 → 初始化运行设置 → 启动后台任务和 AMQP 重试循环 → 启动 HTTP。HTTP 接流量前，管理库迁移已经完成。收到 `SIGTERM` 或 `SIGINT` 后，先停止 HTTP，再等待后台和 AMQP 资源按关闭超时退出；`SIGHUP` 只重新加载日志配置。

Node 的启动顺序是：加载配置 → 打开并自检 `data_dir` 状态库 → 接入上报面 → 恢复或注册身份 → 启动 reporter、logship、probe、controller 和流水线模块。状态库在其它模块前初始化，在其它模块关闭后再关闭。

## 健康检查和日志

- `GET /healthz` 返回 `{"status":"ok"}`，适合作为 liveness 探针。
- 管理 API 前缀是 `/api/v1`；认证使用会话 Cookie 和 CSRF Cookie。控制台由 Manager 二进制内嵌提供。
- 日志默认写 stdout，生产可将 `log.file` 指向轮转文件。Manager 与 Node 都使用 `log/slog` 和 JSON/console handler。
- 修改 `log.*` 后发送 `SIGHUP`：`systemctl kill -s HUP xdts-manager` 或 `systemctl kill -s HUP xdts-node`。配置无效时保留旧 logger，不会替换为半初始化的输出。

## 数据库迁移和升级

升级步骤：

1. 阅读目标版本变更说明并备份 Manager 管理库。
2. 停止 Manager；协议版本变化时，同时停止相关 Node。保留 Node 的 `data_dir` 和注册码。
3. 替换二进制，执行 `xdts-manager migrate --config ...`。
4. 检查迁移返回码和日志，再启动 Manager。
5. 替换 Node 二进制后启动。Manager 与 Node 必须使用相同的 `ProtocolVersion`；先验证一台已更新的节点，再启动其余节点。

不要跳过迁移直接接入流量。迁移由代码维护的版本表驱动，重复执行是幂等的。

当前协议版本是 `0x00010031`，注册、身份确认和消息信封都要求完全一致。旧版二进制和旧协议积压消息会被拒绝，不能通过分批混用版本保持通信；升级前应停止投递并确认队列中的未消费数据已处理。

信封必须显式携带 protobuf `content_type` 和 `identity`/`zstd` 编码；缺失编码不再解释为未压缩消息。控制面和数据面均在解析阶段检查协议版本，数据面不再绕过版本检查。

当前版本不转换旧 Node 状态：仅恢复按源/目标侧保存且协议版本一致的已应用配置；检查点必须包含运行身份和位点修订号，结构历史只读取版本化记录。旧的已应用快照不能作为启动兜底，缺失必要字段的检查点会拒绝恢复。升级后检查配置重新下发和恢复结果，不能恢复的流水线需重新确认起点或重新初始化。不要直接删除状态目录。

重新初始化的预览必须包含输入摘要和源表定义。旧预览缺少这些信息时会被拒绝，需要重新生成并确认。

Node 不再从配置导入身份，删除 `node.center`、`node.node_id`、`node.token` 配置键。管理库只查询 `transaction_isolation`。

配置保留期使用 Go 标准 duration，例如 `360h`、`720h`，不接受 `15d`、`30d`。日志格式仅支持 `json`、`console`，级别仅支持 `debug`、`info`、`warn`、`error`。

## 备份和恢复

Manager 管理库是平台元数据、账号、会话、流水线配置、检查结果和运行记录的权威存储，应按数据库团队策略做一致性备份。恢复后先运行迁移，再启动 Manager。

Node 的 `data_dir` 至少包含 `identity.xdoc` 和 `state.xdoc`。备份时停止 Node 或使用文件系统一致性快照，保留注册码。Node 启动自检发现可恢复损坏时会把旧文件放入 `backup/` 或 `corrupt/` 并上报重建事件；不要在未确认数据损失前删除这些目录。

## RabbitMQ 运维

Manager 会声明以下固定拓扑：`xdts.register`、`xdts.rpc`、`xdts.reply`、`xdts.control`、`xdts.report`、`xdts.data`、`xdts.dlx`，以及中心内的 `xdts.manager.register`、`xdts.manager.rpc`、`xdts.manager.report`、`xdts.manager.log`。Node 还会按节点和流水线声明控制/数据队列。

队列参数是 RabbitMQ 的不可变声明属性。若 `amqp.log_queue_max_length` 与已有 `xdts.manager.log` 不一致，Manager 会报告 `406 PRECONDITION_FAILED` 并每 2 分钟重试。确认影响后由运维手动删除该日志队列，再让 Manager 重新声明；这会丢弃队列中尚未消费的日志，不能自动执行。

流水线数据队列和死信队列的参数变更也需要停流、评估未消费消息，再按 RabbitMQ 队列迁移流程处理。不要把删除队列当作普通重启步骤。

## 常见故障定位

| 现象                           | 检查                                                                                                            |
|--------------------------------|-----------------------------------------------------------------------------------------------------------------|
| Manager 启动时报登录方式不可用 | 至少启用一个可用的 local/LDAP/OIDC；本地空账号表需要 bootstrap 用户名和密码。                                   |
| Manager 报事务隔离级别错误     | 检查会话和全局 `transaction_isolation`，设置为 `REPEATABLE-READ` 后重启。                                       |
| Manager 一直重试 AMQP          | 检查 URL、TLS CA、vhost 权限和交换机/队列参数；区分连接失败与 406 参数冲突。                                    |
| Node 无法启动状态库            | 检查 `node.data_dir` 权限、注册码是否与首次注册相同、是否已有其它进程持有 `.lock`。                             |
| Node 一直等待注册              | 检查注册码是否有效、Manager 是否能消费 `xdts.register`、RabbitMQ vhost 是否一致；被撤销的身份需要重新注册流程。 |
| 控制台打开但 API 失败          | 先访问 `/healthz`，再检查 `/api/v1/auth/me`、Cookie 的 Secure 属性、反向代理是否保留 HTTPS。                    |
