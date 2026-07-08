---
sidebar_position: 5
---

# Kubernetes Configuration

This page describes common configuration and recommended practices for deploying SeaTunnel on Kubernetes.

## ConfigMap and Secret

Store non-sensitive configuration in ConfigMap and mount the required files with `subPath`, for example `/opt/seatunnel/config/seatunnel.yaml` or `/opt/seatunnel/config/hazelcast-master.yaml`. Do not mount a ConfigMap over the whole `/opt/seatunnel/config` directory unless the ConfigMap also contains all files required by the image, such as `jvm_options`, `jvm_master_options`, and `jvm_worker_options`.

Do not write sensitive values directly into ConfigMap, including:

- Database passwords.
- Object storage access keys and secret keys.
- Tokens, private keys, and other credentials.

Store sensitive values in Kubernetes Secret and inject them through environment variables or mounted files. ConfigMap file content does not resolve Kubernetes `secretKeyRef`; SeaTunnel, Hazelcast, and the underlying filesystem must read final usable configuration values or a credential mechanism they support, such as environment variables, Hadoop credential provider, cloud-provider credential chains, or mounted credential files.

## Hazelcast Kubernetes Discovery

Cluster mode depends on Hazelcast to discover other members. On Kubernetes, use a Headless Service:

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

The matching `hazelcast.yaml` must use the same namespace, service name, and port:

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

If SeaTunnel is deployed in a namespace other than `default`, update `namespace` accordingly. Also update `hazelcast-client.yaml` `cluster-members`, for example `seatunnel-cluster.<namespace>.svc.cluster.local:5801`, so the job submission client connects to the same namespace.

SeaTunnel server settings come from `seatunnel.engine` in `seatunnel.yaml`; cluster member discovery comes from `hazelcast.yaml`, `hazelcast-master.yaml`, or `hazelcast-worker.yaml`.

Use the following Hazelcast `properties` as the baseline configuration. They are aligned with the current SeaTunnel default Hazelcast configuration so that the Kubernetes examples stay consistent with the codebase. Tune them before production rollout based on CPU limits, network latency, Pod eviction behavior, cluster size, and job concurrency.

```yaml
properties:
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

## Separate Master and Worker Configuration

In separated cluster mode, use different Hazelcast configuration for Master and Worker:

- Master configures IMap MapStore and conservative heartbeat settings.
- Worker configures `member-attributes.rule=worker` and does not carry IMap MapStore.
- `seatunnel.yaml` can be shared, but some settings only take effect for one role.

## Slot Configuration

Worker slots are cluster scheduling resources. Static slots are recommended for production:

```yaml
seatunnel:
  engine:
    slot-service:
      dynamic-slot: false
      slot-num: 8
    job-schedule-strategy: WAIT
```

Plan `slot-num` together with Worker CPU, memory, and job parallelism. When too many Worker Pods are terminated at the same time, available slots drop quickly. Use StatefulSet for Workers and check slot capacity before rolling updates or scale-down.

## Checkpoint Storage

Multi-node clusters must use shared storage or object storage as checkpoint backend. Common choices include HDFS, S3, OSS, COS, OBS, TOS, or Kubernetes PersistentVolume.

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

If object storage is used, every Master and Worker Pod must have network access and the same read/write permissions.

## IMap MapStore

Master nodes handle IMap state storage. In production, enable MapStore and write data to shared storage or object storage. In separated cluster mode, configure MapStore only in `hazelcast-master.yaml`; Workers do not need MapStore.

MapStore uses `FileMapStoreFactory`. The `type: hdfs` entry is the identifier of the IMap file storage factory and must stay unchanged even when S3 or OSS is used. The actual backend is selected by `storage.type`, which currently supports `hdfs`, `s3`, and `oss`.

### Checkpoint Monitor MapStore

If you do not want to persist checkpoint monitor data, add an explicit override for `engine_checkpoint_monitor`:

```yaml
hazelcast:
  map:
    engine_checkpoint_monitor:
      map-store:
        enabled: false
```

This IMap stores checkpoint overview and history data used by REST APIs and UI observability. It is not the authoritative checkpoint recovery state. Checkpoint recovery state is written by `seatunnel.engine.checkpoint.storage` to HDFS, S3, OSS, or another configured checkpoint storage backend.

This override is optional. Use it when you want to avoid persisting observability-only data and reduce MapStore/WAL write amplification. Omit it if you want checkpoint monitor overview/history to use the same MapStore settings as other `engine*` IMaps.

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

### S3 or Compatible Object Storage

S3-compatible storage uses Hadoop S3A configuration. When AWS S3 is used, rely on Hadoop S3A default endpoint resolution. Compatible services such as MinIO or Ceph RGW usually require their own `fs.s3a.endpoint` and `fs.s3a.path.style.access: true`.

If static credentials are provided in the final YAML, use the fixed Hadoop S3A keys: `fs.s3a.access.key` and `fs.s3a.secret.key`. When `SimpleAWSCredentialsProvider` is configured explicitly, both keys are required. In this mode, Hadoop uses the values from YAML directly; do not write them as `${AWS_SECRET_ACCESS_KEY}` or `{AWS_SECRET_ACCESS_KEY}`.

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

If you want to inject credentials from Kubernetes Secret as environment variables, do not put `secretKeyRef` inside the ConfigMap content. Inject the AWS SDK environment variables into the Pod, and do not pin `fs.s3a.aws.credentials.provider` to `SimpleAWSCredentialsProvider`. The current Hadoop AWS 3.1.4 default credential chain already reads AWS environment variables when no provider is configured explicitly. If you want to allow only environment-variable authentication, set the S3A provider to `com.amazonaws.auth.EnvironmentVariableCredentialsProvider`:

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

For temporary credentials, also inject `AWS_SESSION_TOKEN`. The AWS SDK also accepts `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`, but `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are recommended.

If MinIO, Ceph RGW, or another S3-compatible service is used, add the service endpoint to the S3 configuration:

```yaml
properties:
  fs.s3a.endpoint: https://s3-compatible-endpoint.example.com
  fs.s3a.path.style.access: true
```

### OSS

Alibaba Cloud OSS uses Hadoop OSS configuration and selects the target bucket with `oss.bucket`. Set `fs.oss.endpoint` to the actual endpoint of the bucket region, private cloud, or OSS-compatible service.

The default Hadoop Aliyun OSS credential provider reads `fs.oss.accessKeyId` and `fs.oss.accessKeySecret`. Temporary credentials can additionally set `fs.oss.securityToken`. It does not provide a default environment-variable credential chain like Hadoop S3A, so credentials injected from Kubernetes Secret as environment variables are not used automatically.

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

If you do not want to render OSS credentials into the final ConfigMap, use Hadoop credential provider for `fs.oss.accessKeyId` and `fs.oss.accessKeySecret`, or set `fs.oss.credentials.provider` to a custom provider. The custom provider must implement the Aliyun OSS SDK `com.aliyun.oss.common.auth.CredentialsProvider` interface and must be available in the SeaTunnel image.

When `backup-count: 1` is used, at least 2 Master replicas are required to store IMap backup replicas.

Do not commit real access keys or secret keys to ConfigMaps or Git repositories. Kubernetes does not resolve `secretKeyRef` inside ConfigMap file content. SeaTunnel and Hazelcast must read a final, usable YAML configuration. In production, generate the final ConfigMap through Helm, Kustomize, External Secrets, CI/CD, or the image build process, or use credential mechanisms actually supported by the underlying filesystem, such as Hadoop credential provider, AWS SDK environment variable provider, cloud-provider credential chains, or mounted credential files. Make sure every Master Pod has the same read/write permission to the object store.

When S3 or OSS is used, the SeaTunnel image must also contain the corresponding Hadoop filesystem implementation and dependencies, such as Hadoop AWS/AWS SDK or Hadoop Aliyun/OSS SDK. Checkpoint storage and IMap MapStore can use the same object storage service, but use different `namespace` prefixes, for example `/seatunnel/checkpoint/` and `/seatunnel/imap`.

## Plugin Loading

The base Master and Worker StatefulSets do not include custom plugin loading logic. Custom plugins are an additional deployment concern. Manage them separately according to cluster requirements and make sure Master and Worker use the same plugin versions.

Common plugin loading approaches:

| Approach | Use case | Description |
| --- | --- | --- |
| Build plugins into the SeaTunnel image | Stable production environments | Most repeatable and recommended for production |
| Inject plugins with initContainer overlay | Plugin set changes frequently | Add an initContainer to the StatefulSet as an overlay, copy plugins from a plugin image to `emptyDir`, and mount them |
| Mount from PersistentVolume or object storage | Large plugin sets | Requires extra version consistency management |

### Use a Custom Image

For stable production environments, build custom plugins into the SeaTunnel image:

```Dockerfile
FROM seatunnel:3.0.0

COPY plugins/ /opt/seatunnel/plugins/
```

Then use the same image tag in both Master and Worker StatefulSets.

### Use an initContainer Overlay

If plugins need to be released independently, build a plugin-only image and add the following fragment to both Master and Worker StatefulSets as an overlay. This fragment is not part of the base deployment manifest.

```yaml
initContainers:
  - name: plugin-loader
    image: seatunnel-plugin:v1.0.0
    command:
      - sh
      - -c
      - cp /opt/seatunnel/plugins/plugin.jar /mnt/lib/
    volumeMounts:
      - name: plugin-lib
        mountPath: /mnt/lib
containers:
  - name: app
    volumeMounts:
      - name: plugin-lib
        mountPath: /opt/seatunnel/lib/plugin.jar
        subPath: plugin.jar
volumes:
  - name: plugin-lib
    emptyDir: {}
```

If the plugin directory contains multiple jars, mount the whole plugin directory instead of mounting only one jar file.

## Logging

Keep console logs so Kubernetes log collectors can collect them. You can also use `log4j2.properties` to write local rolling logs. If logs are collected by sidecar or DaemonSet, make sure log paths, file names, and collection rules are consistent.

The default log path is usually:

```text
/opt/seatunnel/logs
```

For production environments, limit log retention time and file size to avoid filling Pod local disk.
