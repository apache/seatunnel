---
sidebar_position: 6
title: Checkpoint 恢复
---

# Kubernetes Checkpoint 恢复

显式恢复创建新的 Kubernetes Job 和新的 Zeta job ID。启用 [Application HA](architecture.md) 后，单个 Master/Worker Pod 丢失可在同一 application 和原生 job ID 内恢复。以下步骤用于 application 停止后的显式恢复。

## 选择存储

可以使用：

- 调用方持有的 PVC，挂载到 Master 的 `/opt/seatunnel/checkpoints`；
- HDFS；
- OSS、S3、COS 等原生 checkpoint storage 支持的对象存储。

:::note
远程存储不需要 `kubernetes.checkpoint-pvc`。镜像必须包含对应文件系统实现和依赖。
:::

## 使用 PVC

先创建 PVC：

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: seatunnel-checkpoints
  namespace: seatunnel-apps
spec:
  accessModes: [ReadWriteOnce]
  resources:
    requests:
      storage: 10Gi
```

部署配置增加：

```hocon
kubernetes.checkpoint-pvc = "seatunnel-checkpoints"
```

在运行时 ConfigMap 的 `seatunnel.yaml` 中配置原生存储：

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
          fs.defaultFS: file:///
          namespace: /opt/seatunnel/checkpoints/orders/
```

:::caution
Provider 只负责挂载 PVC；存储类型和 namespace 由原生 checkpoint plugin 解释。Master 用户必须
具有卷目录写权限。application 不会删除该 PVC。
:::

## 使用远程存储

在运行时 ConfigMap 的 `seatunnel.yaml` 中配置完整 `seatunnel.engine.checkpoint.storage.plugin-config`。Application Mode 不解析 checkpoint URI，也不维护存储后端白名单。HDFS、OSS、S3、COS、endpoint 和认证配置参见 [Checkpoint Storage](../../../engines/zeta/checkpoint-storage.md)。

## 提交恢复作业

在原作业中启用周期 checkpoint，并保留提交时打印的 Zeta job ID：

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  checkpoint.retain-after-job-cancelled = true
}
```

确认原 application 已停止，并且存在已完成 checkpoint 后提交：

```bash
bin/seatunnel-application.sh submit --target kubernetes \
  --config job.conf --application-config kubernetes.conf \
  --restore-job-id PREVIOUS_JOB_ID
```

:::caution
新提交使用新的 job ID，并从历史 ID 的最新有效 checkpoint 恢复。找不到 checkpoint 时作业在
Master 上失败（此时 `submit` 命令早已返回），
不会静默从头执行。
:::

保持作业拓扑、Connector 版本、状态格式和并行度兼容。checkpoint 后产生的数据是否重放以及 Sink 交付保证取决于 Connector 语义。

## 普通 HA 存储

HA 的全部 Master 必须访问同一持久 checkpoint 后端及 namespace。文件后端应使用支持实际多节点部署的共享 PVC，或采用原生远程后端配置。`--restore-job-id` 使用 checkpoint 恢复；无可用 checkpoint 时失败，目标使用新的 job ID。没有 connector 专用协调时，不应让历史与恢复 application 同时写入同一 sink。
