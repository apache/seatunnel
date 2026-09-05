---
sidebar_position: 5
---

# Kubernetes 配置

本页说明 SeaTunnel 在 Kubernetes 上部署时常见的配置项和推荐实践。

## ConfigMap 与 Secret

非敏感配置建议放入 ConfigMap，并通过 `subPath` 只挂载需要覆盖的文件，例如 `/opt/seatunnel/config/seatunnel.yaml` 或 `/opt/seatunnel/config/hazelcast-master.yaml`。

:::caution 注意
不要直接用 ConfigMap 覆盖整个 `/opt/seatunnel/config` 目录，除非该 ConfigMap 同时包含镜像启动所需的所有文件，例如 `jvm_options`、`jvm_master_options` 和 `jvm_worker_options`。

敏感信息不应直接写入 ConfigMap，包括：

- 数据库密码。
- 对象存储 access key 和 secret key。
- token、证书私钥和其他凭据。

推荐将敏感信息放入 Kubernetes Secret，并通过环境变量或文件挂载方式注入。ConfigMap 文件内容不会解析 Kubernetes `secretKeyRef`；SeaTunnel、Hazelcast 和底层文件系统读取到的必须是最终可用的配置值，或者它们真实支持的凭据机制，例如环境变量、Hadoop credential provider、云厂商凭据链或挂载凭据文件。
:::

## Hazelcast Kubernetes 发现

集群模式依赖 Hazelcast 发现其他成员。Kubernetes 中推荐使用 Headless Service：

```yaml
apiVersion: v1
kind: Service
metadata:
  name: seatunnel-cluster
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  ports:
    - name: hazelcast
      port: 5801
  selector:
    app: seatunnel
```

可以在下面两种 Hazelcast Kubernetes discovery 方式中选择一种。

### API 发现

API 发现会通过 Kubernetes API，根据配置的命名空间和 Service 解析成员。对应的 `hazelcast.yaml` 中需要配置相同的命名空间、服务名和端口：

```yaml
hazelcast:
  network:
    join:
      kubernetes:
        enabled: true
        namespace: default
        service-name: seatunnel-cluster
        service-port: 5801
```

如果部署在非 `default` 命名空间，需要同步修改 `namespace`。同时也要修改 `hazelcast-client.yaml` 的 `cluster-members`，例如 `seatunnel-cluster.<namespace>.svc.cluster.local:5801`，确保提交任务的客户端连接到同一个命名空间。

:::caution 注意
使用 `namespace`、`service-name` 和 `service-port` 时，Hazelcast Kubernetes discovery 会在成员启动阶段查询 Kubernetes API。在启用 RBAC 的集群中，SeaTunnel Pod 需要使用绑定了读取 Pod、Service 和 Endpoint 权限的 ServiceAccount。请在启动 StatefulSet 前创建以下 RBAC 资源，并在 Pod 模板中设置 `serviceAccountName: seatunnel`。
:::

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

### DNS 发现

DNS 发现通过 Kubernetes DNS 解析 Headless Service，不需要 Hazelcast 访问 Kubernetes API，因此成员发现不需要额外的 Role 或 RoleBinding。当集群只需要通过一个稳定的 Headless Service 发现成员时，这种方式更简单：

```yaml
hazelcast:
  network:
    join:
      kubernetes:
        enabled: true
        service-dns: seatunnel-cluster.default.svc.cluster.local
        service-dns-timeout: 10
```

:::info 说明
如果部署在非 `default` 命名空间，需要同步修改 `service-dns` 和 `hazelcast-client.yaml` 的 `cluster-members`。Headless Service 仍建议保留 `publishNotReadyAddresses: true`，这样集群启动阶段 DNS discovery 也能解析到 Pod。
:::

SeaTunnel 服务端配置由 `seatunnel.yaml` 中的 `seatunnel.engine` 提供；集群成员发现由 `hazelcast.yaml`、`hazelcast-master.yaml` 或 `hazelcast-worker.yaml` 提供。

下面这组 Hazelcast `properties` 与当前 SeaTunnel 默认 Hazelcast 配置保持一致，Kubernetes 示例也以这组配置作为基线。生产上线前应结合 CPU limit、网络延迟、Pod 驱逐策略、节点规模和作业并发压测调整。

```yaml
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

## Master 与 Worker 配置分离

分离集群模式下建议为 Master 和 Worker 使用不同的 Hazelcast 配置：

- Master 配置 IMap MapStore 和更稳健的心跳参数。
- Worker 配置 `member-attributes.rule=worker`，并避免承载 IMap MapStore。
- `seatunnel.yaml` 可以共用，但部分配置只对某一类角色生效。

## Slot 配置

Worker 的 slot 是集群调度资源。生产环境建议使用静态 slot：

```yaml
seatunnel:
  engine:
    slot-service:
      dynamic-slot: false
      slot-num: 8
    job-schedule-strategy: WAIT
```

`slot-num` 需要结合 Worker 的 CPU、内存和任务并行度规划。Worker Pod 被同时终止过多时，可用 slot 会快速下降，因此 Worker 也推荐 StatefulSet，并在滚动更新或缩容前确认 slot 余量。

## Checkpoint 存储

多节点集群必须使用共享存储或对象存储作为 checkpoint 后端。常见选择包括 HDFS、S3、OSS、COS、OBS、TOS 或 Kubernetes PersistentVolume。

```yaml
seatunnel:
  engine:
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
```

如果使用对象存储，需要保证每个 Master 和 Worker Pod 都能访问对象存储网络，并具备相同的读写权限。

## IMap MapStore

Master 节点负责 IMap 状态存储。生产环境建议启用 MapStore，并将数据写入共享存储或对象存储。分离集群模式下只需要在 `hazelcast-master.yaml` 中配置 MapStore，Worker 不需要配置。

MapStore 使用 `FileMapStoreFactory`。其中 `type: hdfs` 是 IMap 文件存储工厂的标识，使用 S3 或 OSS 时也保持不变；真正的底层存储由 `storage.type` 决定，当前支持 `hdfs`、`s3` 和 `oss`。

### Checkpoint Monitor MapStore

如果不希望持久化 checkpoint monitor 数据，可以为 `engine_checkpoint_monitor` 添加显式覆盖：

```yaml
hazelcast:
  map:
    engine_checkpoint_monitor:
      map-store:
        enabled: false
```

:::info 说明
这个 IMap 存储的是 REST API 和 UI 观测使用的 checkpoint overview/history 数据，并不是用于恢复的权威 checkpoint 状态。真正的 checkpoint 恢复状态由 `seatunnel.engine.checkpoint.storage` 写入 HDFS、S3、OSS 或其他已配置的 checkpoint 存储后端。

这个覆盖是可选的。需要避免持久化仅用于观测的数据、减少 MapStore/WAL 写放大时可以使用；如果希望 checkpoint monitor 的 overview/history 与其他 `engine*` IMap 使用相同的 MapStore 配置，可以不添加该覆盖。
:::

### HDFS

```yaml
hazelcast:
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
```

### S3 或兼容对象存储

S3 兼容存储使用 Hadoop S3A 配置。使用 AWS S3 时，可以依赖 Hadoop S3A 默认 endpoint 解析；MinIO、Ceph RGW 等兼容服务通常需要配置自己的 `fs.s3a.endpoint`，并设置 `fs.s3a.path.style.access: true`。

如果直接在最终 YAML 中提供静态凭据，使用 Hadoop S3A 的固定 key：`fs.s3a.access.key` 和 `fs.s3a.secret.key`。显式配置 `SimpleAWSCredentialsProvider` 时，必须提供这两个 key。此时 Hadoop 会直接使用 YAML 中的值，不需要写成 `${AWS_SECRET_ACCESS_KEY}` 或 `{AWS_SECRET_ACCESS_KEY}`。

```yaml
hazelcast:
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
          storage.type: s3
          s3.bucket: s3a://seatunnel-bucket
          fs.s3a.access.key: YOUR_ACCESS_KEY
          fs.s3a.secret.key: YOUR_SECRET_KEY
          fs.s3a.aws.credentials.provider: org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

:::caution 注意
如果希望通过 Kubernetes Secret 注入环境变量，不要在 ConfigMap 中写 `secretKeyRef`。需要在 Pod 中注入 AWS SDK 约定的环境变量，并且不要把 `fs.s3a.aws.credentials.provider` 固定为 `SimpleAWSCredentialsProvider`。
:::

当前 Hadoop AWS 3.1.4 在未显式配置 provider 时，默认认证链已经会读取 AWS 环境变量；如果希望只允许环境变量认证，也可以显式配置 `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`：

```yaml
env:
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: seatunnel-s3-credentials
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: seatunnel-s3-credentials
        key: secret-access-key
```

```yaml
properties:
  type: hdfs
  namespace: /seatunnel/imap
  clusterName: seatunnel-cluster
  storage.type: s3
  s3.bucket: s3a://seatunnel-bucket
  fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
```

临时凭据还可以额外注入 `AWS_SESSION_TOKEN`。AWS SDK 也兼容 `AWS_ACCESS_KEY` 和 `AWS_SECRET_KEY`，但推荐使用 `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY`。

如果使用 MinIO、Ceph RGW 或其他 S3 兼容服务，在上述 S3 配置中增加服务自己的 endpoint：

```yaml
properties:
  fs.s3a.endpoint: https://s3-compatible-endpoint.example.com
  fs.s3a.path.style.access: true
```

### OSS

阿里云 OSS 使用 Hadoop OSS 配置，并通过 `oss.bucket` 指定目标 bucket。`fs.oss.endpoint` 需要按 bucket 所在地域、专有云或兼容 OSS 服务的实际地址填写。

Hadoop Aliyun OSS 的默认凭据 provider 读取 `fs.oss.accessKeyId`、`fs.oss.accessKeySecret`，临时凭据可额外配置 `fs.oss.securityToken`。它没有像 Hadoop S3A 一样的默认环境变量认证链，因此 Kubernetes Secret 注入成环境变量后不会自动生效。

```yaml
hazelcast:
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
          storage.type: oss
          oss.bucket: oss://seatunnel-bucket
          fs.oss.endpoint: https://oss-<region>.aliyuncs.com
          fs.oss.accessKeyId: YOUR_ACCESS_KEY_ID
          fs.oss.accessKeySecret: YOUR_ACCESS_KEY_SECRET
```

如果不希望把 OSS 凭据渲染到最终 ConfigMap，可以使用 Hadoop credential provider 存储 `fs.oss.accessKeyId`、`fs.oss.accessKeySecret`，或通过 `fs.oss.credentials.provider` 指定自定义 provider。自定义 provider 需要实现 Aliyun OSS SDK 的 `com.aliyun.oss.common.auth.CredentialsProvider`，并在 SeaTunnel 镜像中可被加载。

`backup-count: 1` 时，Master 至少需要 2 个副本，才能保存 IMap 备份副本。

:::caution 注意
不要把真实 access key、secret key 直接提交到 ConfigMap 或 Git 仓库。Kubernetes 不会在 ConfigMap 文件内容中解析 `secretKeyRef`，SeaTunnel/Hazelcast 读取到的必须是最终可用的 YAML 配置。生产环境建议通过 Helm、Kustomize、External Secrets、CI/CD 或镜像构建流程生成最终 ConfigMap，或者使用 Hadoop credential provider、AWS SDK 环境变量 provider、云厂商凭据链、挂载凭据文件等底层文件系统真正支持的凭据方式，并确保所有 Master Pod 具有相同的对象存储读写权限。
:::

如果使用 S3 或 OSS，SeaTunnel 镜像中还需要包含对应 Hadoop 文件系统实现及其依赖，例如 Hadoop AWS/AWS SDK 或 Hadoop Aliyun/OSS SDK。Checkpoint 存储和 IMap MapStore 可以使用同一个对象存储服务，但建议使用不同的 `namespace` 前缀，例如 `/seatunnel/checkpoint/` 和 `/seatunnel/imap`。

## 插件加载

基础 Master/Worker StatefulSet 不包含自定义插件加载逻辑。自定义插件属于额外部署内容，应按集群需要单独管理，并确保 Master 和 Worker 使用一致的插件版本。

需要根据 jar 的加载方式选择 SeaTunnel 运行目录：

| 目录 | 适合放置 | 不适合放置 |
| --- | --- | --- |
| `${SEATUNNEL_HOME}/plugins` | 自定义连接器插件 jar 或插件包 | Transform 插件 jar 或通用 runtime jar |
| `${SEATUNNEL_HOME}/lib` | Transform 插件 jar，以及少数必须进入 SeaTunnel 进程 classpath 的 runtime extension jar | 连接器插件 jar |

:::info 说明
SeaTunnel 对不同插件类型使用不同的加载路径。`plugins/` 目录用于自定义连接器，不用于 Transform 插件或无关 runtime jar。如果自定义连接器需要驱动或客户端库，应将这些 jar 与自定义连接器插件一起打包后放到 `plugins/`。SeaTunnel 自带的 Transform 插件，例如 `seatunnel-transforms-v2.jar`，会打包到 `lib/`，并从 runtime classpath 中发现。只有当自定义 jar 是 Transform 插件，或者是必须被 SeaTunnel 进程 classpath 可见的 runtime extension jar 时，才放到 `lib/`。
:::

:::caution 注意
仅把 Transform 插件 jar 放到 `plugins/` 下，如果没有进入 runtime classpath，也可能无法被发现。不要把自定义连接器插件 jar 放到 `lib/`。
:::

常见加载方式有三种：

| 方式 | 适用场景 | 说明 |
| --- | --- | --- |
| 自定义连接器插件和 Transform jar 打入 SeaTunnel 镜像 | 稳定生产环境 | 最可重复，推荐生产使用 |
| initContainer overlay 注入自定义连接器插件 | 连接器组合经常变化 | 在 StatefulSet 上额外添加 initContainer，将插件文件从插件镜像复制到 `emptyDir` 后挂载 |
| PersistentVolume 或对象存储挂载 | 连接器包较多 | 需要额外管理版本一致性 |

### 使用自定义镜像

稳定生产环境推荐将自定义连接器插件 jar、Transform 插件 jar 和必需的 runtime extension jar 构建到 SeaTunnel 镜像中：

```Dockerfile
FROM seatunnel:3.0.0

# 可选：自定义连接器插件 jar 或插件包。
COPY plugins/ /opt/seatunnel/plugins/

# 可选：Transform 插件 jar 或需要进程 classpath 的 runtime extension jar。
COPY lib/ /opt/seatunnel/lib/
```

自定义连接器插件 jar 放在 `/opt/seatunnel/plugins/`。只有 Transform 插件 jar 或必须进入 SeaTunnel 进程 classpath 的 runtime extension jar 才放在 `/opt/seatunnel/lib/`。

然后在 Master 和 Worker StatefulSet 中使用同一个镜像 tag。

### 使用 initContainer overlay

如果自定义连接器插件需要与主镜像独立发布，可以把以下片段添加到 Master 和 Worker StatefulSet。该片段不属于基础部署清单。由于 `plugins` 的 `emptyDir` 挂载会在运行时替换镜像内的对应目录，插件镜像必须包含提交作业所需的所有自定义连接器插件文件。

```yaml
initContainers:
  - name: plugin-loader
    image: seatunnel-plugin:v1.0.0
    command:
      - sh
      - -c
      - |
        cp -R /plugin/plugins/. /mnt/plugins/
    volumeMounts:
      - name: plugin-dependencies
        mountPath: /mnt/plugins
containers:
  - name: app
    volumeMounts:
      - name: plugin-dependencies
        mountPath: /opt/seatunnel/plugins
volumes:
  - name: plugin-dependencies
    emptyDir: {}
```

:::caution 注意
不要用 `emptyDir` 覆盖整个 `/opt/seatunnel/lib` 目录，除非其中也包含基础镜像内全部 runtime jar；补充单个 Transform 插件 jar 或 runtime extension jar 时建议使用 `subPath`，或直接构建到镜像中。所有 Master 和 Worker Pod 必须使用相同的插件和依赖版本。
:::

如果还需要注入 Transform 插件 jar，建议用 `subPath` 单独挂载，不要替换整个 `lib` 目录。如果已经使用了上面的 overlay，请把 `runtime-lib` 挂载和复制命令合并到同一个 initContainer 中。下面是一个最小独立片段：

```yaml
initContainers:
  - name: plugin-loader
    image: seatunnel-plugin:v1.0.0
    volumeMounts:
      - name: runtime-lib
        mountPath: /mnt/lib
    command:
      - sh
      - -c
      - cp /plugin/lib/custom-transform.jar /mnt/lib/
containers:
  - name: app
    volumeMounts:
      - name: runtime-lib
        mountPath: /opt/seatunnel/lib/custom-transform.jar
        subPath: custom-transform.jar
volumes:
  - name: runtime-lib
    emptyDir: {}
```

## 日志配置

建议保留控制台日志，便于 Kubernetes 日志系统采集；同时可以使用 `log4j2.properties` 输出本地滚动日志。若使用 sidecar 或 DaemonSet 收集日志，需要保证日志路径、文件名和采集规则一致。

默认日志路径通常为：

```text
/opt/seatunnel/logs
```

生产环境建议限制日志保留周期和单文件大小，避免 Pod 本地磁盘被写满。
