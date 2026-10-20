---
sidebar_position: 5
title: Session 集群
---

# Session 集群

Session 集群是一个长期存活的 Zeta 集群，多个 job 共享同一组 master 和 worker。两个 master 通过
Hazelcast 选主，active master 负责创建 worker。job 在集群启动后独立提交，共享 worker slot。

它和 [Application Mode](../application/overview.md) 不同：Application Mode 一个 job 一套专属
master 和 worker。

## 集群形态

```text
kubectl apply -k deploy/kubernetes/session
  -> 2 个 Master Pod（StatefulSet）
  -> active master 创建 min-workers 个 Worker Pod
  -> Worker 注册 slot
  -> 通过 REST 或原生客户端提交 job
```

- Master：StatefulSet，2 副本，`backup-count: 1`。
- Worker：由 active master 创建的 bare Pod，不是 Deployment/StatefulSet。
- Worker owner：Session root ConfigMap。master 故障切换后 Worker 继续存活，由新 active master 接管。
- Slot：MVP 只支持固定 slot。`session.worker.slots` 必须等于 worker `seatunnel.yaml` 的
  `slot-service.slot-num`。
- 自动扩容：pending job slot 不足时，master 按
  `ceil(missingSlots / slotsPerWorker)` 创建 worker，上限为 `max-workers`。
- 自动缩容：MVP 不支持。

## 前置条件

- Kubernetes 集群和 `kubectl`。
- 包含 Session 启动脚本和 Kubernetes resource-manager provider 的 SeaTunnel 镜像。
- 一个用于 Session 资源的 namespace。
- 用户自有的 worker ConfigMap，包含 `seatunnel.yaml`、`hazelcast-worker.yaml`、
  `log4j2.properties`、`jvm_worker_options`。
- 用户自有的 master ConfigMap，包含 `session.yaml`、`seatunnel.yaml`、`hazelcast-master.yaml`、
  `hazelcast-client.yaml`、`log4j2.properties`、`jvm_master_options`。
- 所有 Pod 都能访问 checkpoint 和 IMap 存储。示例使用 `file:///tmp/` 便于本地单节点验证；
  生产环境应使用 HDFS、S3 或 PVC。

## 配置

### session.yaml

```yaml
session:
  name: seatunnel-session-cluster
  cluster-name: seatunnel-session-cluster
  root-config-map: seatunnel-session-cluster-meta

  worker:
    min-workers: 2
    max-workers: 32
    slots: 8
    cpu-cores: 8
    memory-mb: 24576
    heap-mb: 16384
    image: apache/seatunnel:3.0.1
    image-pull-policy: IfNotPresent
    service-account: default
    config-map: seatunnel-session-cluster-worker
    jvm-options: "-Xms16g -Xmx16g"

  dynamic-worker:
    enabled: true
    startup-timeout-millis: 120000
    retry-interval-millis: 3000
```

`min-workers` 默认是 `2`。`dynamic-worker.enabled` 默认 `false`；开启后才会根据 pending job 的
slot 缺口扩容，且不超过 `max-workers`。

### Worker seatunnel.yaml

```yaml
seatunnel:
  engine:
    slot-service:
      dynamic-slot: false
      slot-num: 8
    checkpoint:
      interval: 30000
      timeout: 60000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          namespace: /seatunnel/checkpoint
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode:8020
```

### Master seatunnel.yaml

```yaml
seatunnel:
  engine:
    backup-count: 1
    history-job-expire-minutes: 1440
    http:
      enable-http: true
      port: 8080
```

Master 不提供 slot，只运行 coordinator 和 REST 服务。

## 启动

```bash
kubectl apply -k deploy/kubernetes/session
kubectl -n seatunnel get pods -w
```

等待 Master StatefulSet 和 Worker Pod 就绪：

```bash
kubectl -n seatunnel get statefulset
kubectl -n seatunnel get pods -l seatunnel.apache.org/role=worker
kubectl -n seatunnel logs -f statefulset/seatunnel-session-cluster-master
```

## 提交 job

REST：

```bash
kubectl -n seatunnel port-forward svc/seatunnel-session-cluster-rest 8080:8080
curl -X POST http://127.0.0.1:8080/submit-job \
  -H 'Content-Type: application/json' \
  -d @job.json
```

集群内原生客户端：

```bash
/opt/seatunnel/bin/seatunnel.sh --config job.conf -m cluster
```

客户端配置指向
`seatunnel-session-cluster-client.seatunnel.svc.cluster.local:5801`。

## Worker 扩容

调度器按 task group 申请 slot。资源不足时：

1. `ResourceRequestHandler` 请求 `SessionResourceManager` 扩容；
2. 计算 `neededWorkers = ceil(missingSlots / slotsPerWorker)`；
3. 在 `max-workers` 范围内创建 Worker；
4. 等待 Worker 注册后重试 slot 分配。

`REJECT` 策略在等待超时后直接失败，`WAIT` 策略继续等待。超时后注册的 Worker 仍会参与后续调度。

## 停止

先停止 job，再删除 Session。建议先删 Master StatefulSet，避免 active master 在 Worker 被删除时
继续补 Worker：

```bash
kubectl -n seatunnel delete statefulset seatunnel-session-cluster-master
kubectl delete -k deploy/kubernetes/session
```

用户自有的 checkpoint、PVC、被引用的 ConfigMap/Secret 不会被删除。

## MVP 限制

- Worker 模板不支持原地修改；修改镜像、slot、CPU、内存需要新建 Session。
- 不支持自动缩容。
- MVP 不提供手动 scale/spec API。
- Master 只有 Hazelcast active/standby + StatefulSet 重建，不支持额外 HA 编排。
- 不支持多租户配额、Kerberos、CPU 感知的 slot 调度。
