---
sidebar_position: 5
title: Checkpoint 恢复
---

# YARN Checkpoint 恢复

Application Mode 通过显式提交新的 YARN application 恢复历史作业。它不会自动重启失败的 ApplicationMaster，也不提供运行中 Master 接管。

## 1. 配置持久化存储

在第一次提交前，在发行包的 `config/seatunnel.yaml` 中配置原生 checkpoint 存储。checkpoint 目录必须位于 `yarn.staging-dir` 之外。

HDFS 示例：

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 5000
      timeout: 60000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode:8020
          namespace: /seatunnel/checkpoints
```

Application Mode 会原样使用完整的 `plugin-config`。OSS、S3、COS、HDFS nameservice、endpoint 和认证配置参见 [Checkpoint Storage](../../engines/zeta/checkpoint-storage.md)。发行包必须包含对应文件系统实现和依赖。

## 2. 在作业中启用 checkpoint

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  checkpoint.retain-after-job-cancelled = true
}
```

Application Mode 默认在取消后保留 checkpoint。作业显式设置 `checkpoint.retain-after-job-cancelled = false` 会覆盖默认值，并可能使后续恢复失效。

## 3. 记录原生 job ID

首次提交时记录命令打印的 Zeta job ID，也可以显式指定：

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config streaming-job.conf --deployment-config yarn-deployment.conf \
  --job-id 10001
```

等待至少一个 checkpoint 完成，再停止或等待原 application 失败。

## 4. 提交恢复 application

使用新的 job ID，并把历史 ID 作为恢复来源：

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config streaming-job.conf --deployment-config yarn-deployment.conf \
  --job-id 10002 --restore-from-checkpoint 10001 --wait
```

新 application 会读取 Job `10001` 的最新有效 checkpoint，并把后续 checkpoint 写入 Job `10002` 的目录。找不到有效 checkpoint 时提交失败，不会静默从头执行。

## 恢复要求

- 原 application 已经停止，避免两个执行同时操作同一外部系统。
- 新 application 能访问相同的 checkpoint 存储和 Hadoop 配置。
- 作业拓扑、Connector 版本、状态序列化格式和并行度与 checkpoint 兼容。
- Connector 自身支持 checkpoint；重放范围和 Sink 交付保证仍由 Connector 语义决定。
- checkpoint 目录不位于 application staging 下。
