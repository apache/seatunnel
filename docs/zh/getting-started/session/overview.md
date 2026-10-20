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
- Slot：实验版本只支持固定 slot。`session.worker.slots` 必须等于 worker `seatunnel.yaml` 的
  `slot-service.slot-num`。
- 自动扩容：pending job slot 不足时，master 按
  `ceil(missingSlots / slotsPerWorker)` 创建 worker，上限为 `max-workers`。
- 自动缩容：实验版本不支持。

### 同 namespace 多 Session

这些 manifest 是普通的外部 YAML。要在同一 namespace 运行另一个 Session，复制目录并替换该
Session 的所有 `seatunnel-session-cluster` 名称：

```bash
cp -R deploy/kubernetes/session /tmp/payments-session
# 编辑 /tmp/payments-session：
#   资源名、标签、session.yaml、kustomization configMapGenerator、
#   Hazelcast/client Service DNS。
kubectl apply -k /tmp/payments-session
```

每个 Session 作用域内的资源名必须唯一。这里没有部署脚本，也没有 Operator；用户完全拥有这些
manifest，也可以自行创建 Service、ConfigMap、Secret、RBAC，只保留自己需要的资源。

## 前置条件

Kubernetes 集群、namespace、ConfigMap/Secret、镜像拉取和 Hazelcast 发现等通用要求，请先阅读
[Kubernetes 部署](../kubernetes/kubernetes.mdx) 和
[Kubernetes 配置](../kubernetes/configuration.md)。Session 特有的要求是：

- 包含 Session 启动脚本和 Kubernetes resource-manager provider 的 SeaTunnel 镜像。
- `conf/` 目录，包含 `session.yaml`、`seatunnel.yaml`、master/worker 的 Hazelcast 配置、
  client 配置、日志配置和两份 JVM 参数文件。
- 示例 `kustomization.yaml` 会从 `conf/` 生成一个 `<session-name>-config` ConfigMap。
- 所有 Pod 都能访问 checkpoint 和 IMap 存储。示例使用 `file:///tmp/` 仅用于本地单节点验证；
  生产存储请参考 [分离集群模式](../kubernetes/separated-cluster-mode.md)。

## 配置

### session.yaml

```yaml
session:
  # 必填，DNS-1123 label，最长 40 个字符；同 namespace 多个 Session 时必须唯一。
  name: seatunnel-session-cluster

  # 默认 seatunnel-session-<name>，master 和 worker 共享。
  cluster-name: seatunnel-session-cluster

  # 默认 seatunnel-session-<name>-meta；必须已存在且有 UID。
  root-config-map: seatunnel-session-cluster-meta

  master:
    # 默认 5801；修改时要同步 Service、StatefulSet 和客户端配置。
    port: 5801

  worker:
    # 必填；master 镜像在 StatefulSet 中单独配置。
    image: apache/seatunnel:3.0.1

    # 默认 min-workers=2、max-workers=32。
    min-workers: 2
    max-workers: 32

    # 每个 Worker 的固定 slot 数，必须等于 seatunnel.yaml 的 slot-service.slot-num。
    slots: 8

    # 默认 cpu-cores=1、memory-mb=1024。
    cpu-cores: 2
    memory-mb: 4096

    # 默认 memory-mb 的 3/4；必须大于 0 且小于 memory-mb。
    heap-mb: 3072

    # 默认 -Xms<heap>m -Xmx<heap>m。
    jvm-options: "-Xms3g -Xmx3g"

    # 默认 IfNotPresent、default、/opt/seatunnel。
    image-pull-policy: IfNotPresent
    service-account: default
    seatunnel-home: /opt/seatunnel

    # 已存在的 ConfigMap，包含 seatunnel.yaml、hazelcast-worker.yaml、
    # log4j2.properties、jvm_worker_options。
    config-map: seatunnel-session-cluster-config

    # 可选 Worker Pod 元数据。
    # labels: {}
    # annotations: {}

    # 可选环境变量。引用格式为 secret:name/key 或 configmap:name/key；
    # env-from 会注入指定对象的所有 key。
    # env:
    #   AWS_REGION: us-east-1
    #   S3_SECRET: secret:s3-credentials/secret-key
    # env-from:
    #   secret: s3-credentials
    #   configmap: shared-settings

    # 可选 Secret 挂载。
    # secret-ref: session-secret
    # secret-directory: /etc/seatunnel-secrets/session

  dynamic-worker:
    # 默认 false；开启后根据 pending job 的 slot 缺口自动创建 Worker。
    enabled: true
    # 默认 120000 和 3000，均必须大于 0。
    startup-timeout-millis: 120000
    retry-interval-millis: 3000

kubernetes:
  # 默认 default；必须包含所有 Session 资源。
  namespace: seatunnel
```

示例将配置保留为 `deploy/kubernetes/session/conf/session.yaml`。`kustomization.yaml` 使用
`configMapGenerator` 从整个 `conf/` 目录生成一个 `<session-name>-config` ConfigMap。master
StatefulSet 挂载整个 ConfigMap，worker Pod 从同一个对象挂载自己需要的 key。master 启动时解析
该文件；未知字段会被忽略，非法值会在 coordinator 发布前导致启动失败。

master StatefulSet、Service、PDB、生成的 ConfigMap 名称由 manifest 定义，运行时不会根据
`session.name` 自动改名。复制示例部署另一个 Session 时，要一起修改这些资源名和 DNS。

### Engine 与 Hazelcast 配置

master/worker 的 engine 和 Hazelcast 配置请参考
[分离集群模式](../kubernetes/separated-cluster-mode.md)。Session 特有的要求是：

- Worker 的 `seatunnel.yaml` 必须设置 `slot-service.dynamic-slot: false`，且 `slot-num` 必须等于
  `session.worker.slots`。
- Master StatefulSet 以只读方式挂载生成的 `<session-name>-config` ConfigMap 到
  `/opt/seatunnel/config`；worker 从同一个 ConfigMap 挂载自己需要的四个 key。
- 示例使用 `file:///tmp/` 存储 checkpoint 和 IMap，仅用于本地验证；生产存储请参考分离集群模式。

## 启动

```bash
# 示例 Session。
kubectl apply -k deploy/kubernetes/session

# 复制并改名后的另一个 Session。
kubectl apply -k /tmp/payments-session

kubectl -n seatunnel get pods -w
```

等待 Master StatefulSet 和 Worker Pod 就绪。状态查看、日志和 REST Service 访问方式请参考
[Kubernetes 运维](../kubernetes/operations.md)。

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
继续补 Worker。namespace 中有多个 Session 时不要使用 `kubectl delete -k`，因为 kustomization
包含共享的 namespace 资源。

```bash
SESSION_NAME=seatunnel-session-cluster  # 或复制后的 manifest 中使用的名称

kubectl -n seatunnel delete statefulset "${SESSION_NAME}-master"
kubectl -n seatunnel delete configmap \
  "${SESSION_NAME}-meta" "${SESSION_NAME}-config"
kubectl -n seatunnel delete service \
  "${SESSION_NAME}-headless" "${SESSION_NAME}-rest" "${SESSION_NAME}-client"
kubectl -n seatunnel delete pdb "${SESSION_NAME}-master"
```

用户自有的 checkpoint、PVC、被引用的 ConfigMap/Secret 不会被删除。

## 实验性限制

- Worker 模板不支持原地修改；修改镜像、slot、CPU、内存需要新建 Session。
- 不支持自动缩容。
- 实验版本不提供手动 scale/spec API。
- Master 只有 Hazelcast active/standby + StatefulSet 重建，不支持额外 HA 编排。
- 不支持多租户配额、Kerberos、CPU 感知的 slot 调度。
