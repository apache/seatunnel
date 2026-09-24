---
sidebar_position: 6
title: FAQ
---

# YARN FAQ

## 状态与取消

使用提交时返回的 YARN application ID：

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf
```

关闭客户端连接不会取消 application。`cancel` 会终止 YARN application，并清理本次提交的 staging 文件；它不会创建 savepoint。

## 日志

启用 YARN 日志聚合时：

```bash
yarn logs -applicationId application_0000000000000_0001
```

未启用日志聚合时，在对应 NodeManager 查看 AM 和 Worker Container 日志。排障时先检查 AM `stderr`，再检查所有 Worker 日志。

## 常见问题

| 现象 | 检查项 |
| --- | --- |
| AM 启动超时 | 队列资源、最大 Container 规格、发行包路径、Hadoop 配置和 NodeManager Java |
| Worker 注册超时 | 队列容量、NodeManager 网络、Master 实际端口和防火墙 |
| 找不到 Connector | 发行包 `connectors/` 是否包含版本匹配的插件和驱动 |
| 作业失败后 staging 残留 | 使用原部署配置执行 `status` 触发重试清理，或按 application ID 检查私有 staging 目录 |
| 无法恢复 checkpoint | 历史 Zeta job ID、checkpoint 保留策略、存储权限和配置一致性 |

## 清理边界

正常终止时，ApplicationMaster 会释放 Container、注销最终状态并删除 staging。状态客户端观察到终态后也会重试 staging 清理。

如果 AM 在 shutdown hook 执行前被强杀，且之后没有客户端查询状态，HDFS staging 可能残留。NodeManager 的本地缓存和日志保留由 Hadoop 独立管理。checkpoint 使用独立保留策略，不随 staging 删除。

## 安全建议

- staging 目录保持提交用户私有，不要公开作业配置。
- 不在日志中输出数据库密码、对象存储密钥或 token。
- 使用底层 Hadoop 文件系统支持的 credential provider 或受保护的运行时配置。
- 当前版本检测到 Kerberos 配置时会拒绝提交。
