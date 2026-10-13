# 部署指南

## 拓扑和前置条件

最小生产拓扑包含：

1. 一个 Manager 进程和一个独立的 Manager MySQL 管理库。
2. 每个管理中心一个 RabbitMQ vhost；Manager 和该中心的 Node 连接同一 vhost。
3. 每个中心至少一个 Node。Node 只需要 RabbitMQ、自己的业务 MySQL 端点，以及一个持久化 `data_dir`。
4. 反向代理（可选）把 HTTPS 流量转发到 Manager 的 HTTP 监听地址。

Manager 管理库的事务隔离级别必须是 `REPEATABLE READ`。Manager 启动时会拒绝其它隔离级别，因为多处互斥查询依赖该语义。RabbitMQ 账号至少需要声明交换机/队列、发布和消费相应 vhost 内消息的权限。

Manager 和 Node 必须使用相同协议版本的构建。MySQL 业务端点支持 8.x/9.x；启用副本全量读取时，副本必须支持 `SHOW REPLICA STATUS` 及其 `Source_*`、`Replica_*` 列名，不再使用 `SHOW SLAVE STATUS` 回退。

## 构建

开发构建：

```sh
make frontend
make backend
```

`make frontend` 会运行 `web` 的 Vite 构建，并将资源写入 `internal/manager/static/dist`；Manager 通过 `go:embed` 将这些资源打进二进制。只构建 Go 二进制而不先构建前端，可能得到缺少最新控制台资源的 Manager。

生产 release：

```sh
VERSION=1.0.0 REVISION="$(git rev-parse --short HEAD)" make release
```

脚本会安装前端依赖、构建并嵌入控制台，然后生成 Linux `amd64` 和 `arm64` 的四个文件及 `SHA256SUMS`：

```text
bin/release/1.0.0/
  xdts-manager-linux-amd64
  xdts-manager-linux-arm64
  xdts-node-linux-amd64
  xdts-node-linux-arm64
  SHA256SUMS
```

release 使用 `npm ci` 按锁文件安装依赖。`SHA256SUMS` 使用相对文件名，将发布目录复制到目标机器后，在该目录执行 `sha256sum -c SHA256SUMS` 校验。

## 安装 Manager

```sh
install -d -o xdts -g xdts -m 0750 /etc/xdts /usr/local/lib/xdts /var/log/xdts
install -o root -g xdts -m 0750 bin/release/1.0.0/xdts-manager-linux-amd64 /usr/local/lib/xdts/xdts-manager
install -o xdts -g xdts -m 0600 deploy/production/manager.yaml.example /etc/xdts/manager.yaml
```

修改 `manager.yaml`，或复制 `deploy/systemd/manager.env.example` 为 `/etc/xdts/manager.env` 并注入真实凭据。首次启动前建议显式执行迁移：

```sh
/usr/local/lib/xdts/xdts-manager migrate --config /etc/xdts/manager.yaml
```

迁移成功后再启动服务。Manager 的 `/healthz` 是存活检查，只表示 HTTP 进程存活，不检查 MySQL 或 RabbitMQ。

## 安装 Node

每个 Node 使用独立服务账号和持久化目录：

```sh
install -d -o xdts-node -g xdts-node -m 0750 /var/lib/xdts-node
install -d -o xdts-node -g xdts-node -m 0750 /etc/xdts
install -o root -g xdts-node -m 0750 bin/release/1.0.0/xdts-node-linux-amd64 /usr/local/lib/xdts/xdts-node
install -o xdts-node -g xdts-node -m 0600 deploy/production/node.yaml.example /etc/xdts/node.yaml
```

为 Node 配置中心 RabbitMQ URL、注册码和 `node.data_dir`。首次启动不要手工伪造 `node_id`、`center`、`token`；留空时 Node 会注册并持久化身份。注册码是本地 xdoc 的解密口令，必须在重启和升级时保持不变。

## systemd

`deploy/systemd/xdts-manager.service` 和 `xdts-node.service` 是模板，默认二进制路径分别为 `/usr/local/lib/xdts/xdts-manager` 和 `/usr/local/lib/xdts/xdts-node`。复制到 `/etc/systemd/system/` 后执行：

```sh
systemctl daemon-reload
systemctl enable --now xdts-manager.service
systemctl enable --now xdts-node.service
systemctl status xdts-manager.service xdts-node.service
curl --fail http://127.0.0.1:8080/healthz
```

systemd 模板使用 `SIGTERM` 优雅退出、失败自动重启和只读根文件系统保护。按实际日志目录、服务账号和外部 MySQL/RabbitMQ 服务名调整 `ReadWritePaths`、依赖和权限。
