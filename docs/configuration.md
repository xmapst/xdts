# 配置参考

Manager 和 Node 都使用 Viper 加载 YAML。优先级从低到高为：配置文件、环境变量、命令行参数。未传 `--config` 时，Manager 查找当前目录或 `/etc/xdts/manager.yaml`，Node 查找当前目录或 `/etc/xdts/node.yaml`。

环境变量前缀统一为 `XDTS`，将配置路径转换为大写下划线，例如 `http.listen` 对应 `XDTS_HTTP_LISTEN`，`node.data_dir` 对应 `XDTS_NODE_DATA_DIR`。Node 命令行还暴露 AMQP、Node 数据目录、注册码、日志和资源限制等同名 flag；Manager 命令行主要用于 `--config`，其余设置通过 YAML 或环境变量覆盖。

## Manager

| 键                                        | 默认值                              | 说明                                                                                                    |
|-------------------------------------------|-------------------------------------|---------------------------------------------------------------------------------------------------------|
| `http.listen`                             | `:8080`                             | HTTP 监听地址；生产建议监听 `127.0.0.1` 并由反向代理终止 TLS。                                          |
| `http.shutdown_timeout`                   | `15s`                               | HTTP 优雅关闭等待时间。                                                                                 |
| `http.cookie_secure`                      | `true`                              | 会话和 CSRF Cookie 是否设置 `Secure`。                                                                  |
| `database.dsn`                            | 无                                  | 独立管理库 MySQL DSN，启动时会执行迁移并检查 `REPEATABLE READ`。                                        |
| `amqp.url`                                | 无                                  | 管理中心 RabbitMQ URL，支持 `amqp`/`amqps`。                                                            |
| `amqp.log_queue_max_length`               | `100000`                            | `xdts.manager.log` 队列长度；已有队列的参数不可原地修改。                                               |
| `auth.session_ttl`                        | `12h`                               | 登录会话有效期。                                                                                        |
| `auth.local.enabled`                      | `true`                              | 本地账号登录。首次空账号表需要同时配置 bootstrap 用户名和密码。                                         |
| `auth.ldap.*`                             | 关闭                                | LDAP 地址、绑定账号、用户过滤器、CA 和组到角色映射。                                                    |
| `auth.oidc.*`                             | 关闭                                | OIDC issuer、client、回调 URL、scope 和 claim 映射。                                                    |
| `auth.default_external_role`              | `readonly`                          | 外部用户未命中映射时的角色，可设 `admin`、`readonly` 或 `deny`。                                        |
| `auth.lockout.*`                          | `5 / 15m / 15m`                     | 连续失败阈值、计数窗口、锁定时长。窗口不能大于锁定时长。                                                |
| `dispatch.reconcile_interval`             | `15s`                               | 配置对账循环周期。                                                                                      |
| `background_tasks.*`                      | `30s / 1m / 15s / 24h`              | 回收、巡检、告警、分区预创建周期。                                                                      |
| `background_tasks.endpoint_probe_enabled` | `false`                             | 是否启用周期性端点探测；手动探测不受此开关影响。                                                        |
| `pipeline.binlog_server_id_range_min/max` | `4000000000 / 4294967295`           | 分配 MySQL binlog `server_id` 的闭区间；最小值不能为 0，最大值必须更大。                                |
| `log.*`                                   | `json / info / stdout / 50 / 7 / 7` | 日志格式、级别、文件和轮转参数。格式支持 `json`、`console`；级别支持 `debug`、`info`、`warn`、`error`。 |
| `retention.*`                             | `360h / 360h / 720h / 720h`         | 日志、指标、事件、冲突保留期，使用 Go 标准 duration。                                                   |

管理库不是业务端点库。代码在启动期间先连接并迁移管理库，再初始化运行设置，最后启动 HTTP；AMQP 不可达时后台连接会按 10 秒周期重试，队列参数冲突会按 2 分钟周期重试。

`retention.*` 不支持天数缩写 `d`；15 天写为 `360h`，30 天写为 `720h`。`log.format: text` 和 `log.level: warning` 均无别名处理。

## Node

| 键                                | 默认值                              | 说明                                                                          |
|-----------------------------------|-------------------------------------|-------------------------------------------------------------------------------|
| `amqp.url`                        | 无                                  | Node 所属中心的 RabbitMQ URL。                                                |
| `node.data_dir`                   | 无                                  | 持久化目录，保存 `identity.xdoc`、`state.xdoc`、备份和损坏文件。              |
| `node.register_code`              | 无                                  | 注册凭据，同时作为本地状态库口令；建议只通过 `XDTS_NODE_REGISTER_CODE` 注入。 |
| `store.batch_interval`            | `100ms`                             | 状态批量提交周期。                                                            |
| `store.batch_max_ops`             | `200`                               | 达到该写入数立即提交。                                                        |
| `store.shutdown_timeout`          | `30s`                               | 排空状态写入并提交最后一批的等待时间。                                        |
| `controller.shutdown_timeout`     | `5s`                                | 控制队列 handler 关闭等待时间，必须为正数。                                   |
| `resources.max_pipelines`         | `100`                               | 最大流水线数；`0` 表示不限。                                                  |
| `resources.max_buffer_bytes`      | `1GiB`                              | Node 业务缓冲总上限，不是进程 RSS 上限。                                      |
| `resources.pipeline_buffer_bytes` | `128MiB`                            | 单流水线业务缓冲上限。                                                        |
| `resources.snapshot_concurrency`  | `8`                                 | 全量读写 SQL 并发上限。                                                       |
| `log.*`                           | `json / info / stdout / 50 / 7 / 7` | 与 Manager 相同，Node 格式支持 `json`、`console`。                            |

Node 身份只从 `identity.xdoc` 恢复或通过注册取得，配置不接受 `node.center`、`node.node_id`、`node.token`。Node 首次启动会在 45 秒上下文内解析本地身份并通过 AMQP 注册。身份被撤销时进程进入等待重新注册状态，收到 `SIGINT`/`SIGTERM` 才退出。

## 敏感值

不要把密码、注册码、Node token、OIDC client secret 或 LDAP bind password 写入日志、命令历史或提交的配置。配置回显会脱敏 AMQP 密码、数据库 DSN 和这些凭据，但文件权限仍应限制为服务账号可读，例如：

```sh
install -o xdts -g xdts -m 0600 manager.yaml /etc/xdts/manager.yaml
install -o xdts-node -g xdts-node -m 0600 node.yaml /etc/xdts/node.yaml
```
