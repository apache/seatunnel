---
sidebar_position: 5
title: Session Cluster
---

# Session Cluster

A Session Cluster is a long-lived Zeta cluster shared by multiple jobs. Two masters provide
Hazelcast active/standby election and the active master creates the workers. Jobs are submitted
after the cluster is running and share the worker slots.

This is different from [Application Mode](../application/overview.md), which creates a dedicated
master and worker set for one job.

## Cluster shape

```text
kubectl apply -k deploy/kubernetes/session
  -> 2 Master Pods (StatefulSet)
  -> active master creates min-workers Worker Pods
  -> workers register slots
  -> submit jobs through REST or the native client
```

- Masters: StatefulSet, 2 replicas, `backup-count: 1`.
- Workers: bare Pods created by the active master. There is no Worker Deployment or StatefulSet.
- Worker owner: the session root ConfigMap. Workers survive active-master failover and the next
  active master adopts them.
- Slots: fixed slot mode only in this experimental release. `session.worker.slots` must equal the worker
  `seatunnel.yaml` `slot-service.slot-num`.
- Automatic scale-up: when a pending job needs more slots, the master creates
  `ceil(missingSlots / slotsPerWorker)` workers, capped by `max-workers`.
- Automatic scale-down: not supported in this experimental release.

### Multiple Sessions in one namespace

The manifests are plain external YAML. To run another Session in the same namespace, copy the
directory and replace every `seatunnel-session-cluster` occurrence that belongs to that Session:

```bash
cp -R deploy/kubernetes/session /tmp/payments-session
# Edit /tmp/payments-session:
#   resource names, labels, session.yaml, kustomization configMapGenerator,
#   and Hazelcast/client service DNS.
kubectl apply -k /tmp/payments-session
```

Every session-scoped resource name must be unique. There is no deployer script or Operator; users
own the manifests and may create Services, ConfigMaps, Secrets, or RBAC resources themselves and
keep only the objects they need.

## Requirements

Read [Kubernetes Deployment](../kubernetes/kubernetes.mdx) for cluster prerequisites and
[Kubernetes Configuration](../kubernetes/configuration.md) for namespace, ConfigMap/Secret,
image-pull, and Hazelcast discovery practices. Session-specific requirements are:

- A SeaTunnel image containing the session entrypoint and the Kubernetes resource-manager provider.
- A `conf/` directory containing `session.yaml`, `seatunnel.yaml`, both role Hazelcast configs,
  the client config, logging, and both JVM option files.
- The sample `kustomization.yaml` generates one `<session-name>-config` ConfigMap from `conf/`.
- Checkpoint and IMap storage reachable by every Pod. The sample uses `file:///tmp/` only for a
  local single-node test; use the storage guidance in [Separated Cluster Mode](../kubernetes/separated-cluster-mode.md)
  for production.

## Configure

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
    config-map: seatunnel-session-cluster-config
    jvm-options: "-Xms16g -Xmx16g"

  dynamic-worker:
    enabled: true
    startup-timeout-millis: 120000
    retry-interval-millis: 3000
```

The sample keeps this file as `deploy/kubernetes/session/conf/session.yaml`. `kustomization.yaml`
uses `configMapGenerator` to create one `<session-name>-config` ConfigMap from the whole `conf/`
directory. The master StatefulSet mounts the whole ConfigMap, and worker Pods mount their required
keys from the same object; `session.yaml` is not embedded in another ConfigMap.

`min-workers` defaults to `2`. `dynamic-worker.enabled` defaults to `false`; when enabled, workers
are created only from pending-job slot demand up to `max-workers`.

### Configuration reference

The master parses this file at startup. Unknown keys are ignored; invalid values fail master
startup before the coordinator is published.

#### Session and Kubernetes keys

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `session.name` | yes | — | DNS-1123 label, at most 40 characters. Used for Session labels and default worker/root names. It must be unique when several Sessions share a namespace. |
| `session.cluster-name` | no | `seatunnel-session-<name>` | Hazelcast cluster name shared by masters and workers. |
| `session.root-config-map` | no | `seatunnel-session-<name>-meta` | Existing root ConfigMap. It must exist before the master starts and has a server-assigned UID; worker Pods use it as their owner reference. |
| `session.master.port` | no | `5801` | Master Hazelcast port. Keep the Services, StatefulSet, and client configuration aligned when changing it. |
| `kubernetes.namespace` | no | `default` | Namespace for the Kubernetes client used by the master and the default worker/root names. It must contain the Session resources. |

The master StatefulSet, Services, PDB, and generated ConfigMap name are defined by the manifests,
not derived from `session.name` at runtime. When copying the sample for another Session, rename
those resources and their DNS references together.

#### Worker template keys

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `session.worker.image` | yes | — | Worker container image. The master image is configured separately in the StatefulSet. |
| `session.worker.min-workers` | no | `2` | Workers created when the active master initializes. Must be at least 1. |
| `session.worker.max-workers` | no | `32` | Upper bound for automatic worker creation. Must be at least `min-workers`. |
| `session.worker.slots` | no | `1` | Fixed slots per worker. Must equal the worker `seatunnel.yaml` `slot-service.slot-num`. |
| `session.worker.cpu-cores` | no | `1` | CPU resource requested and limited for each worker container. |
| `session.worker.memory-mb` | no | `1024` | Container memory request and limit, in MiB. |
| `session.worker.heap-mb` | no | `memory-mb * 3 / 4` | JVM heap in MiB. Must be greater than 0 and less than `memory-mb`. |
| `session.worker.jvm-options` | no | `-Xms<heap>m -Xmx<heap>m` | Additional JVM options passed by the worker entrypoint. |
| `session.worker.image-pull-policy` | no | `IfNotPresent` | Kubernetes image pull policy: `Always`, `IfNotPresent`, or `Never`. |
| `session.worker.service-account` | no | `default` | Service account for worker Pods. It does not need Kubernetes API permissions for normal jobs. |
| `session.worker.config-map` | no | `seatunnel-session-<name>-config` | Existing worker ConfigMap. It must contain `seatunnel.yaml`, `hazelcast-worker.yaml`, `log4j2.properties`, and `jvm_worker_options`. |
| `session.worker.seatunnel-home` | no | `/opt/seatunnel` | SeaTunnel installation directory inside the container. |
| `session.worker.labels` | no | empty | Additional worker Pod labels. The Session and role labels are always applied. |
| `session.worker.annotations` | no | empty | Additional worker Pod annotations. |
| `session.worker.env` | no | empty | Map of literal environment values or references using `secret:<name>/<key>` or `configmap:<name>/<key>`. |
| `session.worker.env-from` | no | empty | `secret:` or `configmap:` with comma-separated object names; every key becomes an environment variable. |
| `session.worker.secret-ref` | no | — | Existing Secret mounted into worker Pods. |
| `session.worker.secret-directory` | no | `/etc/seatunnel-secrets/session` | Mount directory for `secret-ref`. |

#### Dynamic worker keys

| Key | Required | Default | Description |
| --- | --- | --- | --- |
| `session.dynamic-worker.enabled` | no | `false` | Enables worker creation after a pending job cannot acquire enough slots. |
| `session.dynamic-worker.startup-timeout-millis` | no | `120000` | Maximum wait for requested workers to register. Must be greater than 0. |
| `session.dynamic-worker.retry-interval-millis` | no | `3000` | Delay between registration checks. Must be greater than 0. |

For example, `env` and `env-from` can be written as:

```yaml
session:
  worker:
    env:
      AWS_REGION: us-east-1
      S3_SECRET: secret:s3-credentials/secret-key
    env-from:
      secret: s3-credentials
      configmap: shared-settings
```

### Engine and Hazelcast configuration

Use the master and worker engine/Hazelcast configuration described in [Separated Cluster Mode](../kubernetes/separated-cluster-mode.md).
The Session-specific requirements are:

- Worker `seatunnel.yaml` must set `slot-service.dynamic-slot: false`; its `slot-num` must equal
  `session.worker.slots`.
- The master StatefulSet mounts the generated `<session-name>-config` ConfigMap read-only at
  `/opt/seatunnel/config`; workers mount only their four required keys from the same ConfigMap.
- The sample uses `file:///tmp/` for checkpoints and IMap persistence only for local validation.
  Follow the storage guidance in Separated Cluster Mode for production.

## Start

```bash
# The sample Session.
kubectl apply -k deploy/kubernetes/session

# Another Session copied and renamed as described above.
kubectl apply -k /tmp/payments-session

kubectl -n seatunnel get pods -w
```

The active master creates the minimum worker set. Wait until the master StatefulSet and worker
Pods are ready. For status, logs, and REST service access, see [Kubernetes Operations](../kubernetes/operations.md).

## Submit jobs

REST:

```bash
kubectl -n seatunnel port-forward svc/seatunnel-session-cluster-rest 8080:8080
curl -X POST http://127.0.0.1:8080/submit-job \
  -H 'Content-Type: application/json' \
  -d @job.json
```

Native client from inside the cluster:

```bash
/opt/seatunnel/bin/seatunnel.sh --config job.conf -m cluster
```

The master client configuration points at
`seatunnel-session-cluster-client.seatunnel.svc.cluster.local:5801`.

## Worker provisioning

The scheduler requests one slot per task group. If the current workers cannot satisfy the request:

1. `ResourceRequestHandler` asks the `SessionResourceManager` for capacity.
2. The manager computes `neededWorkers = ceil(missingSlots / slotsPerWorker)`.
3. It creates workers up to `max-workers`.
4. It waits for registration and retries slot allocation.

`REJECT` scheduling fails after the startup timeout when capacity is still insufficient. `WAIT`
keeps the job pending. A worker that registers after the timeout is still used for later requests.

## Stop

Stop jobs before deleting the session. Delete the master StatefulSet first so the active master
cannot replace workers while they are being removed. Do not use `kubectl delete -k` when the
namespace hosts multiple Sessions, because the kustomization contains the shared namespace.

```bash
SESSION_NAME=seatunnel-session-cluster  # or the name used in the copied manifests

kubectl -n seatunnel delete statefulset "${SESSION_NAME}-master"
kubectl -n seatunnel delete configmap \
  "${SESSION_NAME}-meta" "${SESSION_NAME}-config"
kubectl -n seatunnel delete service \
  "${SESSION_NAME}-headless" "${SESSION_NAME}-rest" "${SESSION_NAME}-client"
kubectl -n seatunnel delete pdb "${SESSION_NAME}-master"
```

User-owned checkpoints, PVCs, and referenced ConfigMaps/Secrets are not deleted.

## Limitations

- Worker template changes are not supported in place. Create a new session for a new image, slot
  count, CPU, or memory configuration.
- No automatic scale-down.
- No manual scale/spec API in this experimental release.
- No master failover beyond the Hazelcast active/standby pair and StatefulSet restart.
- No multi-tenant quotas, Kerberos, or CPU-aware slot scheduling.
