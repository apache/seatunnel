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
- Slots: fixed slot mode only in this MVP. `session.worker.slots` must equal the worker
  `seatunnel.yaml` `slot-service.slot-num`.
- Automatic scale-up: when a pending job needs more slots, the master creates
  `ceil(missingSlots / slotsPerWorker)` workers, capped by `max-workers`.
- Automatic scale-down: not supported in this MVP.

## Requirements

- A Kubernetes cluster and `kubectl`.
- A SeaTunnel image containing the session entrypoint and the Kubernetes resource-manager provider.
- A namespace for the session resources.
- A user-owned worker ConfigMap with `seatunnel.yaml`, `hazelcast-worker.yaml`,
  `log4j2.properties`, and `jvm_worker_options`.
- A user-owned master ConfigMap with `session.yaml`, `seatunnel.yaml`, `hazelcast-master.yaml`,
  `hazelcast-client.yaml`, `log4j2.properties`, and `jvm_master_options`.
- Checkpoint and IMap storage reachable by every Pod. The sample uses `file:///tmp/` for a local
  single-node test; use HDFS, S3, or a PVC in production.

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
    config-map: seatunnel-session-cluster-worker
    jvm-options: "-Xms16g -Xmx16g"

  dynamic-worker:
    enabled: true
    startup-timeout-millis: 120000
    retry-interval-millis: 3000
```

`min-workers` defaults to `2`. `dynamic-worker.enabled` defaults to `false`; when enabled, workers
are created only from pending-job slot demand up to `max-workers`.

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

The master Pods do not provide slots. They run the coordinator and REST service.

## Start

```bash
kubectl apply -k deploy/kubernetes/session
kubectl -n seatunnel get pods -w
```

The active master creates the minimum worker set. Wait until the master StatefulSet and the worker
Pods are ready.

```bash
kubectl -n seatunnel get statefulset
kubectl -n seatunnel get pods -l seatunnel.apache.org/role=worker
kubectl -n seatunnel logs -f statefulset/seatunnel-session-cluster-master
```

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
cannot replace workers while they are being removed:

```bash
kubectl -n seatunnel delete statefulset seatunnel-session-cluster-master
kubectl delete -k deploy/kubernetes/session
```

User-owned checkpoints, PVCs, and referenced ConfigMaps/Secrets are not deleted.

## Limitations

- Worker template changes are not supported in place. Create a new session for a new image, slot
  count, CPU, or memory configuration.
- No automatic scale-down.
- No manual scale/spec API in this MVP.
- No master failover beyond the Hazelcast active/standby pair and StatefulSet restart.
- No multi-tenant quotas, Kerberos, or CPU-aware slot scheduling.
