# 部署材料

- `production/`：Manager 和 Node 的生产 YAML 示例，以及配置注入说明。
- `systemd/`：Linux systemd 服务单元和环境变量文件示例。
- `release/build.sh`：构建前端并生成 Linux amd64/arm64 的 Manager、Node 二进制和 SHA256 校验文件。

完整流程见 [`docs/deployment.md`](../docs/deployment.md)。生产示例中的凭据均为占位值；部署前请使用密钥管理系统或受限环境变量替换，不要把真实凭据提交到仓库。
