---
sidebar_position: 1
title: YARN 概览
---

# YARN Application Mode

YARN Application Mode 为一个 SeaTunnel 作业启动一个独立的 YARN application。ApplicationMaster 在进程内运行唯一的 Zeta Master，申请固定数量的 Worker Container，等待 Worker 注册后提交作业，并在作业结束时释放资源。

它适合已经使用 YARN 管理计算资源、但不希望预先部署长期 Zeta 集群的场景。批作业完成后资源立即回收；流作业作为独立 application 持续运行，直到取消或失败。

## 运行形态

| 组件 | YARN 资源 | 职责 |
| --- | --- | --- |
| Application Master | 一个 AM Container | 启动 Zeta Master、申请 Worker、提交作业和协调清理 |
| Worker | N 个普通 Container | 加入独立 Hazelcast 集群并提供固定 task slot |
| Staging | HDFS 等共享文件系统 | 保存本次 application 的发行包、作业配置和 Hadoop 配置 |
| Checkpoint | HDFS 或受支持的对象存储 | 在 application 之外保存可恢复的作业状态 |

ApplicationMaster 或任意 Worker 丢失都会使当前 application 失败。YARN application 固定为一次 attempt，不会自动替换 Master 或 Worker。持久化 checkpoint 可用于创建新的 application 恢复执行。

## 前置条件

- 可访问的 YARN ResourceManager、NodeManager 和使用 simple 认证的 HDFS。
- 提交用户具有目标队列的提交权限和 staging 目录写权限。
- NodeManager 使用 Linux/Unix 环境，并配置兼容的 Java 版本。
- AM 与 Worker Container 之间允许 Hazelcast TCP 和 Zeta 执行通信。
- Hadoop 配置包含 `core-site.xml`、`hdfs-site.xml` 和 `yarn-site.xml`。
- 发行包包含 YARN provider、作业所需 Connector、Format、Transform 和驱动。

多节点 YARN 必须使用 HDFS 等共享 staging 文件系统。当前版本不支持 Kerberos、delegation token 或 keytab。

## 推荐阅读顺序

| 阶段 | 文档 | 内容 |
| --- | --- | --- |
| 理解部署 | [集群架构](architecture.md) | AM、Worker、staging、网络和资源生命周期 |
| 首次运行 | [快速开始](quick-start.md) | 构建发行包并提交第一个作业 |
| 调整参数 | [配置参考](configuration.md) | 完整 YARN 与公共 application 选项 |
| 配置容错 | [Checkpoint 恢复](checkpoint-recovery.md) | HDFS/对象存储与故障后重新提交 |
| 问题排查 | [FAQ](faq.md) | 状态、取消、日志、清理和常见问题 |

公共能力边界参见 [Application Mode 概览](../overview.md) 和[架构概览](../architecture.md)。
