---
sidebar_position: 2
---

# 本地模式

本地模式用于在 Kubernetes 中快速运行一个 SeaTunnel 作业。该模式只启动一个 Pod 或 Job，不提供集群高可用能力，也不适合作为长期运行的生产集群。

## 创建作业配置

创建 `seatunnel.streaming.conf`：

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 2000
}

source {
  FakeSource {
    parallelism = 2
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Console {
  }
}
```

创建 ConfigMap：

```bash
kubectl create configmap seatunnel-job-config \
  --from-file=seatunnel.streaming.conf=seatunnel.streaming.conf
```

## 创建 Pod

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: seatunnel-local
  labels:
    app: seatunnel
    mode: local
spec:
  restartPolicy: Never
  containers:
    - name: seatunnel
      image: seatunnel:3.0.0
      imagePullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -c
        - /opt/seatunnel/bin/seatunnel.sh --config /data/seatunnel.streaming.conf -e local
      env:
        - name: SEATUNNEL_HOME
          value: /opt/seatunnel
      resources:
        requests:
          cpu: "1"
          memory: 2Gi
        limits:
          cpu: "1"
          memory: 4Gi
      volumeMounts:
        - name: job-config
          mountPath: /data/seatunnel.streaming.conf
          subPath: seatunnel.streaming.conf
  volumes:
    - name: job-config
      configMap:
        name: seatunnel-job-config
```

应用配置：

```bash
kubectl apply -f seatunnel-local.yaml
```

查看日志：

```bash
kubectl logs -f seatunnel-local
```

删除 Pod：

```bash
kubectl delete -f seatunnel-local.yaml
```

## 使用建议

本地模式适合验证镜像、连接器和作业配置是否可用。需要长期运行、REST API 提交任务、集群容错或独立扩缩容时，请使用 [分离集群模式](separated-cluster-mode.md)。
