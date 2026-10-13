# xdts 设计基线

本文是对当前实现的运维级设计摘要。协议字段和 API 的完整契约分别以 `proto/*.proto` 与 `web/openapi/xdts.yaml` 为准；本文不替代代码中的校验逻辑。

## 组件边界

- **Manager** 是中心控制面和管理库所有者：保存中心、端点、节点、注册码、流水线配置、下发记录、事件、冲突、指标和账号。
- **Node** 是数据面执行者：持有本地身份和 xdoc 状态，连接业务 MySQL，执行源端读取、目标端写入、全量、增量、DDL 和探测。
- **RabbitMQ** 是跨进程消息边界。模块间不共享 Manager/Node 内部可变状态。
- **Web** 是 Manager 的内嵌静态资源，Vite 输出到 `internal/manager/static/dist`，由 `go:embed` 进入 Manager 二进制。

## 启动生命周期

Manager 必须先完成配置校验、管理库连通性、事务隔离检查和 schema migration，然后才开始 HTTP 服务。AMQP wiring 在后台拥有自己的连接、消费者和发布者；断线会重试，停止时先停消费者再关发布者和连接。

Node 先初始化本地 store，再解析身份，再启动依赖身份的控制和数据模块。store 负责 `identity.xdoc`、`state.xdoc` 和锁文件；业务模块通过值消息和 store client 访问它，不直接持有数据库句柄。

## RabbitMQ 拓扑

| 类型           | 名称                                                                                              |
|----------------|---------------------------------------------------------------------------------------------------|
| 交换机         | `xdts.register`、`xdts.rpc`、`xdts.reply`、`xdts.control`、`xdts.report`、`xdts.data`、`xdts.dlx` |
| Manager 队列   | `xdts.manager.register`、`xdts.manager.rpc`、`xdts.manager.report`、`xdts.manager.log`            |
| Node 控制队列  | `xdts.node.<node_id>.ctl`                                                                         |
| 流水线数据队列 | `xdts.pl.<pipeline_id>`、`xdts.pl.<pipeline_id>.snap`                                             |
| 死信队列       | `xdts.dlq`                                                                                        |

交换机和持久队列由消费方所在中心声明。日志队列带 `x-max-length` 与 `drop-head`，允许在积压时丢弃旧日志；流水线数据和快照队列使用 quorum 相关参数，参数变化需要显式迁移。

## 配置和身份

配置文件、环境变量、命令行按固定优先级合并，所有字段在启动阶段校验。Manager 的数据库 DSN 不得指向业务库。Node 首次注册取得 `center`、`node_id`、`token` 后写入本地身份文档；注册码同时是注册凭据和 xdoc 口令，不能在升级时轮换。

## 持久化和关闭

Manager 的迁移和运行设置初始化完成后才启动后台持久化循环。Node store 在关闭时排空异步写入和最后一次批量提交；共享 AMQP 连接在消费者和发布者退出后释放。所有跨边界副作用都以消息 ID、配置版本、命令序号或数据库状态实现幂等/去重。

## 协议演进

`proto` 使用 protobuf。消息信封要求生产者和消费者使用完全相同的 `ProtocolVersion`；协议变更必须同时发布 Manager 与 Node。已发布字段不复用字段号或改变既有语义。消息信封、签名、压缩和路由键由 `internal/iproto`、`internal/transport` 统一实现，新增消息应同时更新生产者、消费者、OpenAPI/前端展示和运维文档。
