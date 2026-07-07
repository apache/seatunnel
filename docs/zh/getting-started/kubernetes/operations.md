---
sidebar_position: 6
---

# Kubernetes 运维

本页介绍 SeaTunnel Zeta Engine 在 Kubernetes 上的常见运维操作。

## 查看集群状态

```bash
kubectl get pods -l app=seatunnel
kubectl get statefulset
kubectl get svc
```

查看 Master 日志：

```bash
kubectl logs -f seatunnel-master-0
```

查看 Worker 日志：

```bash
kubectl logs -f seatunnel-worker-0
```

## 访问 REST API

在集群内可以通过 `seatunnel-master` Service 访问 REST API。调试时可以使用端口转发：

```bash
kubectl port-forward svc/seatunnel-master 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
curl http://127.0.0.1:8080/running-jobs
```

生产环境可以通过 Ingress 或 LoadBalancer 暴露 REST API，并按需增加认证、网络策略和访问控制。

## 扩容 Worker

Worker 扩容会增加集群可用 slot：

```bash
kubectl scale statefulset seatunnel-worker --replicas=4
```

扩容后确认 Worker Ready：

```bash
kubectl get pods -l app=seatunnel,component=worker
```

提交大任务前，建议先完成 Worker 扩容，避免任务提交后因 slot 不足等待过久。

## 缩容 Worker

缩容前需要确认：

- 当前运行作业不依赖即将被删除的 Worker。
- 剩余 Worker 的 slot 足够承载当前和后续任务。
- checkpoint 已正常完成。

不建议直接一次性缩容多个 Worker。可以逐个降低副本数，并观察作业状态。

## 滚动更新

更新镜像或配置时，StatefulSet 会按顺序滚动更新 Pod。建议：

- 配置 `preStop` 调用 `stop-seatunnel-cluster.sh`。
- 设置足够长的 `terminationGracePeriodSeconds`。
- 更新前确认 Master 副本数和 Worker slot 余量。
- 避免在高峰期同时更新 Master 和 Worker。
- 如果配置文件通过 `subPath` 挂载，ConfigMap 更新不会自动同步到容器内，需要执行滚动重启或通过 Reloader 等工具触发重启。

如果使用 Reloader 等工具自动重启 Pod，需要确保不会在短时间内同时重启过多 Worker。

## PodDisruptionBudget

生产环境建议为 Master 和 Worker 配置 PodDisruptionBudget，限制自愿驱逐时的不可用 Pod 数：

```yaml
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: seatunnel-master-pdb
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: seatunnel
      component: master
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: seatunnel-worker-pdb
spec:
  maxUnavailable: 1
  selector:
    matchLabels:
      app: seatunnel
      component: worker
```

## 常见问题

### Pod 无法加入集群

检查：

- `seatunnel-cluster` Headless Service 是否存在。
- `hazelcast.yaml`、`hazelcast-master.yaml` 或 `hazelcast-worker.yaml` 中的 `namespace`、`service-name` 和 `service-port` 是否与 Service 一致。
- 5801 端口是否被 Service 暴露。
- Pod 标签是否匹配 Service selector。

### Worker Ready 失败

检查：

- 容器是否正常启动。
- `/opt/seatunnel/config/hazelcast-worker.yaml` 或 `/opt/seatunnel/config/hazelcast.yaml` 是否挂载正确。
- 作业需要的连接器或自定义插件 jar 是否存在。
- 5801 端口是否监听。

### 可用 slot 不足

检查：

- Worker 副本数是否足够。
- `slot-service.dynamic-slot` 和 `slot-num` 是否符合预期。
- 是否有 Worker 正在滚动更新、驱逐或重启。
- 是否一次性终止了过多 Worker Pod。

### Checkpoint 或 MapStore 写入失败

检查：

- 存储路径是否为共享存储或对象存储。
- Pod 是否具备网络访问能力。
- Secret 或挂载凭据是否正确。
- 存储路径是否有读写权限。

### REST API 无法访问

检查：

- `seatunnel-master` Service 是否存在。
- `seatunnel.yaml` 中 `seatunnel.engine.http.enable-http` 是否为 `true`。
- REST API 端口是否与 Service targetPort 一致。
- Ingress 或 LoadBalancer 的转发规则是否正确。
