---
sidebar_position: 7
title: FAQ
---

# YARN FAQ

## 运维

### 如何查询或取消应用？

使用提交时返回的 YARN application ID：

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf
```

关闭客户端连接不会取消 application。`cancel` 会终止 YARN application，并清理本次提交的 staging 文件；它不会创建 savepoint。

### 可以从 ResourceManager 打开 SeaTunnel Web UI 吗？

提交时设置 `yarn.master.http-port`，在应用运行期间即可通过 AM 主机和端口访问。ResourceManager 也会提供代理跟踪链接。如果通过该链接拿到了 HTML 但页面空白，请检查浏览器把 `/assets/` 和 `/overview` 请求发往哪个站点。可用 AM 直连地址或专用网关域名打开完整 UI。省略端口时 AM 关闭 HTTP。详见 [AM 的 HTTP 端口](configuration.md#am-的-http-端口)。

### 在哪里查看容器日志？

启用 YARN 日志聚合时：

```bash
yarn logs -applicationId application_0000000000000_0001
```

未启用日志聚合时，在对应 NodeManager 查看 AM 和 Worker Container 日志。排障时先检查 AM `stderr`，再检查所有 Worker 日志。

## 常见故障

| 现象 | 检查项 |
| --- | --- |
| AM 启动超时 | 队列资源、最大 Container 规格、发行包路径、Hadoop 配置和 NodeManager Java |
| Worker 注册超时 | 队列容量、NodeManager 网络、Master 实际端口和防火墙 |
| 找不到 Connector | 发行包 `connectors/` 是否包含版本匹配的插件和驱动 |
| 作业失败后 staging 残留 | 使用原部署配置执行 `status` 触发重试清理，或按 application ID 检查私有 staging 目录 |
| 找不到作业文件 | 使用 `-c` 或 `yarn.job-config-file`。提交端源文件的相对路径按当前工作目录解析。 |
| 容器找不到配置文件 | 检查 `yarn.local-resources` 中的源文件和目标路径，确认运行选项指向同一 `files/…` 路径。 |
| 安全 HDFS 拒绝访问 | 检查提交身份、HDFS 权限、token 有效期和 `yarn.resourcemanager.principal`。 |
| 无法恢复 checkpoint | 历史 Zeta job ID、checkpoint 保留策略、存储权限和配置一致性 |

## 生命周期

### 应用结束后会如何处理文件？

正常终止时，ApplicationMaster 会释放 Container、注销最终状态并删除 staging。查询终态时，如果 staging 清理失败，客户端会记录警告并返回 ResourceManager 确认的结果。使用有权限的用户和原 staging 路径再次执行 `status` 即可重试清理。

如果 AM 在 shutdown hook 执行前被强杀，且之后没有客户端查询状态，HDFS staging 可能残留。NodeManager 的本地缓存和日志保留由 Hadoop 独立管理。checkpoint 使用独立保留策略，不随 staging 删除。

## 安全建议

- staging 目录保持提交用户私有，不要公开作业配置。
- 不在日志中输出数据库密码、对象存储密钥或 token。
- 使用底层 Hadoop 文件系统支持的 credential provider 或受保护的运行时配置。
- Kerberos 集群需要有效的票据缓存或提交端 keytab，并需要配置 ResourceManager principal。详见 [Kerberos 配置](configuration.md#kerberos)。
