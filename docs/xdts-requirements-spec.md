# xdts 当前实现支持范围

这份清单用于部署和验收时对照当前代码，不是未来路线图。以本仓库可执行文件、`proto/*.proto` 和 OpenAPI 为最终判定依据。

## 已具备的运行能力

- Manager HTTP 管理面、嵌入式 React 控制台、会话/CSRF、local/LDAP/OIDC 认证入口和 admin/readonly 角色。
- Manager 管理库自动迁移、运行设置、日志/指标/事件/冲突保留、节点注册和注册码管理。
- RabbitMQ 控制面、上报面、日志面、RPC、流水线增量数据和快照数据队列。
- Node 身份注册与撤销等待、本地 xdoc 状态、状态自检/备份、日志上报、节点/流水线资源配额。
- MySQL 端点探测、流水线配置下发、启动/暂停/恢复/停止/重试、全量与增量同步、DDL/冲突/错误策略，以及数据检查和结构迁移预览接口。

## 部署验收要点

1. Manager 使用独立 MySQL 管理库，启动日志确认事务隔离级别为 `REPEATABLE READ`。
2. Manager 与每个 Node 使用相同中心的 RabbitMQ vhost，能声明所需拓扑。
3. `/healthz` 返回 200；登录后 `/api/v1/auth/me` 返回启用的认证方式和当前用户。
4. Node `data_dir` 可写且重启后身份保持不变；注册码未写入日志。
5. 新建测试流水线通过预检查后，控制指令、心跳、进度、事件和日志能在 Manager 中看到。
6. 停止 Manager/Node 时连接、消费者、状态写入按超时退出，没有遗留锁或重复消费者。

## 明确的边界

- `/healthz` 是 liveness，不代表数据库、RabbitMQ 或数据面健康。
- `resources.max_buffer_bytes` 是业务缓冲记账上限，不是进程 RSS 限制。
- 日志队列允许丢弃旧日志；删除队列会丢弃未消费消息，必须由运维确认。
- Node 的身份和状态依赖注册码保护；没有注册码不能恢复加密状态库。
