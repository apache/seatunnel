---
sidebar_position: 5
title: Checkpoint 恢复
---

# Kubernetes Checkpoint 恢复

恢复会创建新的 Kubernetes Job 和新的 Zeta job ID。当前版本不提供自动 Master 切换或 Job 自动重启。

## 选择存储

可以使用：

- 调用方持有的 PVC，挂载到 Master 的 `/opt/seatunnel/checkpoints`；
- HDFS；
- OSS、S3、COS 等原生 checkpoint storage 支持的对象存储。

远程存储不需要 `kubernetes.checkpoint-pvc`。镜像必须包含对应文件系统实现和依赖。

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

镜像内 `config/seatunnel.yaml` 配置原生存储：

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

Provider 只负责挂载 PVC；存储类型和 namespace 由原生 checkpoint plugin 解释。Master 用户必须具有卷目录写权限。application 不会删除该 PVC。

## 使用远程存储

在镜像的 `seatunnel.yaml` 中配置完整 `seatunnel.engine.checkpoint.storage.plugin-config`。Application Mode 不解析 checkpoint URI，也不维护存储后端白名单。HDFS、OSS、S3、COS、endpoint 和认证配置参见 [Checkpoint Storage](../../engines/zeta/checkpoint-storage.md)。

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
  --config job.conf --deployment-config kubernetes.conf \
  --restore-from-checkpoint PREVIOUS_JOB_ID --wait
```

新提交使用新的 job ID，并从历史 ID 的最新有效 checkpoint 恢复。找不到 checkpoint 时提交失败，不会静默从头执行。

保持作业拓扑、Connector 版本、状态格式和并行度兼容。checkpoint 后产生的数据是否重放以及 Sink 交付保证取决于 Connector 语义。
