---
sidebar_position: 4
---

# 分离集群模式

分离集群模式中，SeaTunnel Engine 的 Master 与 Worker 分别运行在独立进程中。Master 负责作业调度、REST API、任务提交和 IMap 状态存储；Worker 负责执行任务，不参与 Master 选举，也不存储 IMap 数据。

这是 Kubernetes 生产环境推荐的部署方式。Master 和 Worker 都应使用 StatefulSet 部署，避免滚动更新、节点驱逐或重新调度时同时终止过多节点，导致选主抖动或可用 slot 突然不足。

## 推荐拓扑

| 组件 | 推荐工作负载 | 最小副本数 | 生产建议 |
| --- | --- | --- | --- |
| Master | StatefulSet | 1 | 至少 2 个，用于 HA 和 IMap 备份 |
| Worker | StatefulSet | 1 | 按任务并行度和 slot 规划扩缩容 |
| Hazelcast discovery | Headless Service | 1 | `publishNotReadyAddresses: true` |
| REST API | ClusterIP Service | 1 | 可按需通过 Ingress 或 LoadBalancer 暴露 |

:::tip 提示
单 Master 可以启动集群，但不具备高可用能力。若 `backup-count: 1`，建议至少部署 2 个 Master，否则 Master 宕机后集群无法依赖备份副本恢复 IMap 状态。
:::

## 创建 ConfigMap

建议按角色拆分 ConfigMap，避免单个 YAML 过长，也便于分别更新 Master、Worker 和客户端配置。生产环境中的访问密钥、密码和 token 应通过 Secret 管理，不应直接写入 ConfigMap。

### Master Hazelcast 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-master-hazelcast-config
data:
  hazelcast-master.yaml: |
    hazelcast:
      cluster-name: seatunnel-cluster
      network:
        rest-api:
          enabled: true
          endpoint-groups:
            CLUSTER_WRITE:
              enabled: true
            DATA:
              enabled: true
        port:
          auto-increment: false
          port: 5801
        join:
          kubernetes:
            enabled: true
            namespace: default
            service-name: seatunnel-cluster
            service-port: 5801
      map:
        engine*:
          map-store:
            enabled: true
            initial-mode: EAGER
            factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
            properties:
              type: hdfs
              namespace: /seatunnel/imap
              clusterName: seatunnel-cluster
              storage.type: hdfs
              fs.defaultFS: hdfs://namenode:8020
      properties:
        hazelcast.shutdownhook.policy: GRACEFUL
        hazelcast.invocation.max.retry.count: 20
        hazelcast.tcp.join.port.try.count: 30
        hazelcast.logging.type: log4j2
        hazelcast.operation.generic.thread.count: 50
        hazelcast.heartbeat.failuredetector.type: phi-accrual
        hazelcast.heartbeat.interval.seconds: 2
        hazelcast.max.no.heartbeat.seconds: 180
        hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
        hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
        hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
```

### Worker Hazelcast 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-worker-hazelcast-config
data:
  hazelcast-worker.yaml: |
    hazelcast:
      cluster-name: seatunnel-cluster
      network:
        rest-api:
          enabled: true
          endpoint-groups:
            CLUSTER_WRITE:
              enabled: true
            DATA:
              enabled: true
        port:
          auto-increment: false
          port: 5801
        join:
          kubernetes:
            enabled: true
            namespace: default
            service-name: seatunnel-cluster
            service-port: 5801
      properties:
        hazelcast.shutdownhook.policy: GRACEFUL
        hazelcast.invocation.max.retry.count: 20
        hazelcast.tcp.join.port.try.count: 30
        hazelcast.logging.type: log4j2
        hazelcast.operation.generic.thread.count: 50
        hazelcast.heartbeat.failuredetector.type: phi-accrual
        hazelcast.heartbeat.interval.seconds: 2
        hazelcast.max.no.heartbeat.seconds: 180
        hazelcast.heartbeat.phiaccrual.failuredetector.threshold: 10
        hazelcast.heartbeat.phiaccrual.failuredetector.sample.size: 200
        hazelcast.heartbeat.phiaccrual.failuredetector.min.std.dev.millis: 100
      member-attributes:
        rule:
          type: string
          value: worker
```

这些示例使用 Hazelcast Kubernetes API 发现。如果希望使用 DNS 发现，并避免 Hazelcast 访问 Kubernetes API，可以将 `hazelcast-master.yaml` 和 `hazelcast-worker.yaml` 中的 `join.kubernetes` 都替换为：

```yaml
join:
  kubernetes:
    enabled: true
    service-dns: seatunnel-cluster.default.svc.cluster.local
    service-dns-timeout: 10
```

:::info 说明
使用 DNS 发现时，下面的 RBAC 章节不是成员发现所必需的。如果跳过 RBAC 清单，也需要从两个 StatefulSet 中移除 `serviceAccountName: seatunnel`，或单独创建这个 ServiceAccount。
:::

### Hazelcast Client 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-client-config
data:
  hazelcast-client.yaml: |
    hazelcast-client:
      cluster-name: seatunnel-cluster
      properties:
        hazelcast.logging.type: log4j2
      connection-strategy:
        connection-retry:
          cluster-connect-timeout-millis: 7000
      network:
        cluster-members:
          # 如果 SeaTunnel 部署在其他命名空间，需要将 default 替换为实际命名空间。
          - seatunnel-cluster.default.svc.cluster.local:5801
```

### SeaTunnel Engine 配置

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: seatunnel-engine-config
data:
  seatunnel.yaml: |
    seatunnel:
      engine:
        backup-count: 1
        history-job-expire-minutes: 1440
        print-execution-info-interval: 300
        classloader-cache-mode: true
        telemetry:
          metric:
            enabled: false
          logs:
            scheduled-deletion-enable: true
        slot-service:
          dynamic-slot: false
          slot-num: 8
        job-schedule-strategy: WAIT
        checkpoint:
          interval: 180000
          timeout: 30000
          storage:
            type: hdfs
            max-retained: 3
            plugin-config:
              storage.type: hdfs
              namespace: /seatunnel/checkpoint/
              fs.defaultFS: hdfs://namenode:8020
        http:
          enable-http: true
          port: 8080
```

:::caution 注意
如果使用 S3、OSS、COS、OBS、TOS 等对象存储作为 checkpoint 或 MapStore 后端，需要保证每个 Pod 都具备网络访问能力和对应凭据。不要在 ConfigMap 的 YAML 内容中写 `secretKeyRef`；需要在部署前渲染最终配置，或使用 Hadoop credential provider、云厂商凭据链、挂载凭据文件等底层文件系统支持的方式。
:::

## 为 API 发现创建 RBAC

`hazelcast-master.yaml` 和 `hazelcast-worker.yaml` 默认使用的 `namespace`、`service-name` 和 `service-port` 属于 Hazelcast Kubernetes API 发现。在启用 RBAC 的集群中，请先创建 ServiceAccount、Role 和 RoleBinding，再启动 StatefulSet。如果改用 `service-dns` 发现，可以跳过本节：

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: seatunnel
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: seatunnel-hazelcast-discovery
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "endpoints"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: seatunnel-hazelcast-discovery
subjects:
  - kind: ServiceAccount
    name: seatunnel
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: seatunnel-hazelcast-discovery
```

## 创建 Service

`seatunnel-cluster` 是 Headless Service，用于 Hazelcast 成员发现。Master 和 Worker 都会通过该服务加入同一个集群。

```yaml
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-cluster
  labels:
    app: seatunnel-cluster
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ports:
    - name: hazelcast
      port: 5801
      targetPort: 5801
  selector:
    app: seatunnel
---
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-master
  labels:
    app: seatunnel-master
    component: master
spec:
  type: ClusterIP
  ports:
    - name: rest-api
      port: 8080
      targetPort: 8080
    - name: hazelcast
      port: 5801
      targetPort: 5801
  selector:
    app: seatunnel
    component: master
---
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-worker
  labels:
    app: seatunnel-worker
    component: worker
spec:
  type: ClusterIP
  ports:
    - name: rest-api
      port: 8080
      targetPort: 8080
    - name: hazelcast
      port: 5801
      targetPort: 5801
  selector:
    app: seatunnel
    component: worker
```

## 创建 Master StatefulSet

以下 StatefulSet 示例使用 16 GB JVM 堆内存，CPU request 设置为 4、limit 设置为 8，容器内存 request 设置为 20 GiB、limit 设置为 24 GiB。内存余量用于 Metaspace、直接内存、线程栈和其他本地内存，CPU 配额用于任务执行、垃圾回收和引擎协调。对于大规模数据处理场景，建议使用 32 GB JVM 堆内存，可将 CPU request 和 limit 分别提高到 8 和 16，并将内存 request 和 limit 分别提高到 36 GiB 和 40 GiB，作为初始配置。请根据 Connector 特性、任务并行度以及实际 CPU、GC 和内存利用率继续调整。

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seatunnel-master
  labels:
    app: seatunnel
    component: master
spec:
  serviceName: seatunnel-cluster
  replicas: 2
  selector:
    matchLabels:
      app: seatunnel
      component: master
  template:
    metadata:
      labels:
        app: seatunnel
        component: master
    spec:
      serviceAccountName: seatunnel
      containers:
        - name: app
          image: seatunnel:3.0.0
          imagePullPolicy: IfNotPresent
          command:
            - /opt/seatunnel/bin/seatunnel-cluster.sh
            - -r
            - master
            - '-DJvmOption=-Xms16g -Xmx16g'
          env:
            - name: SEATUNNEL_HOME
              value: /opt/seatunnel
            - name: HAZELCAST_CLUSTER_NAME
              value: seatunnel-cluster
          ports:
            - containerPort: 8080
              name: rest-api
            - containerPort: 5801
              name: hazelcast
          resources:
            requests:
              cpu: "4"
              memory: 20Gi
            limits:
              cpu: "8"
              memory: 24Gi
          volumeMounts:
            - name: hazelcast-master-config
              mountPath: /opt/seatunnel/config/hazelcast-master.yaml
              subPath: hazelcast-master.yaml
            - name: client-config
              mountPath: /opt/seatunnel/config/hazelcast-client.yaml
              subPath: hazelcast-client.yaml
            - name: engine-config
              mountPath: /opt/seatunnel/config/seatunnel.yaml
              subPath: seatunnel.yaml
      terminationGracePeriodSeconds: 120
      volumes:
        - name: hazelcast-master-config
          configMap:
            name: seatunnel-master-hazelcast-config
        - name: client-config
          configMap:
            name: seatunnel-client-config
        - name: engine-config
          configMap:
            name: seatunnel-engine-config
```

## 创建 Worker StatefulSet

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: seatunnel-worker
  labels:
    app: seatunnel
    component: worker
spec:
  serviceName: seatunnel-cluster
  replicas: 2
  selector:
    matchLabels:
      app: seatunnel
      component: worker
  template:
    metadata:
      labels:
        app: seatunnel
        component: worker
    spec:
      serviceAccountName: seatunnel
      containers:
        - name: app
          image: seatunnel:3.0.0
          imagePullPolicy: IfNotPresent
          command:
            - /opt/seatunnel/bin/seatunnel-cluster.sh
            - -r
            - worker
            - '-DJvmOption=-Xms16g -Xmx16g'
          env:
            - name: SEATUNNEL_HOME
              value: /opt/seatunnel
            - name: HAZELCAST_CLUSTER_NAME
              value: seatunnel-cluster
          ports:
            - containerPort: 8080
              name: rest-api
            - containerPort: 5801
              name: hazelcast
          resources:
            requests:
              cpu: "4"
              memory: 20Gi
            limits:
              cpu: "8"
              memory: 24Gi
          volumeMounts:
            - name: hazelcast-worker-config
              mountPath: /opt/seatunnel/config/hazelcast-worker.yaml
              subPath: hazelcast-worker.yaml
            - name: client-config
              mountPath: /opt/seatunnel/config/hazelcast-client.yaml
              subPath: hazelcast-client.yaml
            - name: engine-config
              mountPath: /opt/seatunnel/config/seatunnel.yaml
              subPath: seatunnel.yaml
      terminationGracePeriodSeconds: 120
      volumes:
        - name: hazelcast-worker-config
          configMap:
            name: seatunnel-worker-hazelcast-config
        - name: client-config
          configMap:
            name: seatunnel-client-config
        - name: engine-config
          configMap:
            name: seatunnel-engine-config
```

自定义插件不包含在基础 StatefulSet 中。需要额外插件时，请参考 [插件加载](configuration.md#插件加载)，通过自定义镜像、initContainer overlay、PersistentVolume 或对象存储单独管理插件。

## 健康检查和优雅停止

Master 和 Worker 都建议添加以下容器片段。`startupProbe` 可以避免启动期误杀；`preStop` 会先调用 SeaTunnel 停止脚本，再等待 `SeaTunnelServer` 进程退出，配合 `terminationGracePeriodSeconds` 可以降低滚动更新和节点驱逐对运行任务的影响。

```yaml
startupProbe:
  tcpSocket:
    port: 5801
  periodSeconds: 10
  failureThreshold: 30
readinessProbe:
  tcpSocket:
    port: 5801
  initialDelaySeconds: 31
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3
livenessProbe:
  tcpSocket:
    port: 5801
  initialDelaySeconds: 30
  periodSeconds: 30
  timeoutSeconds: 5
  failureThreshold: 3
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          /opt/seatunnel/bin/stop-seatunnel-cluster.sh
          while kill -0 $(ps -ef | grep SeaTunnelServer | grep -v grep | awk '{print $2}') 2>/dev/null; do
            sleep 1
          done
```

## 应用配置

:::info 说明
仅在使用 API 发现，或 StatefulSet 保留 `serviceAccountName: seatunnel` 时应用 `seatunnel-rbac.yaml`。
:::

```bash
kubectl apply -f seatunnel-master-hazelcast-config.yaml
kubectl apply -f seatunnel-worker-hazelcast-config.yaml
kubectl apply -f seatunnel-client-config.yaml
kubectl apply -f seatunnel-engine-config.yaml
kubectl apply -f seatunnel-rbac.yaml
kubectl apply -f seatunnel-services.yaml
kubectl apply -f seatunnel-master.yaml
kubectl apply -f seatunnel-worker.yaml
```

检查 Pod 状态：

```bash
kubectl get pods -l app=seatunnel
```

访问 REST API：

```bash
kubectl port-forward svc/seatunnel-master 8080:8080
curl http://127.0.0.1:8080/system-monitoring-information
```

提交作业请参考 [REST API V2](../../engines/zeta/rest-api-v2.md)。

## 扩缩容 Worker

Worker 扩容可以增加可用 slot：

```bash
kubectl scale statefulset seatunnel-worker --replicas=4
```

缩容前应确认被缩容的 Worker 上没有正在运行的关键任务，且剩余 Worker 的 slot 能承载当前作业。建议先停止或迁移作业，再缩容 Worker。
