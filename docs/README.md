# xdts 文档

xdts 是由 Manager、Node、RabbitMQ 和 MySQL 组成的多中心 MySQL 同步平台。Manager 提供管理 API 和内嵌 Web 控制台，Node 在各中心执行同步，RabbitMQ 承载控制、上报和数据消息，Manager 管理库保存平台元数据与运行记录。

按使用场景阅读：

- [部署指南](deployment.md)：准备依赖、构建、安装二进制和启动服务。
- [配置参考](configuration.md)：Manager/Node 的 YAML 键、默认值、环境变量覆盖和敏感项处理。
- [运行维护](operations.md)：迁移、升级、备份、健康检查、日志重载和常见故障。
- [部署示例](../deploy/production/README.md)：生产 YAML 与 systemd 模板。
- [设计基线](xdts-design.md)：当前代码实现的组件边界、消息拓扑和生命周期。
- [需求与支持范围](xdts-requirements-spec.md)：以当前实现为准的运维可见能力清单。
- [OpenAPI](../web/openapi/xdts.yaml)：HTTP API 的机器可读契约。

文档中的命令以 Linux amd64/arm64 为目标。路径、版本和资源限制以当前代码及配置校验为准；如果文档与可执行文件行为不一致，应先以可执行文件为准并同步修正文档。
