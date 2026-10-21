---
sidebar_position: 6
title: Checkpoint Recovery
---

# Kubernetes checkpoint recovery

Explicit recovery creates a new Kubernetes Job and a new Zeta job ID. With [application HA](architecture.md#opt-in-application-ha), individual master/worker Pod loss recovers within the existing application using the same native job ID. The instructions below describe explicit recovery after an application stops.

## Choose storage

Supported choices include:

- a caller-owned PVC mounted at `/opt/seatunnel/checkpoints` in the master;
- HDFS;
- object stores supported by native checkpoint storage, including OSS, S3, and COS.

:::note
Remote storage does not require `kubernetes.checkpoint-pvc`. The image must contain the
corresponding filesystem implementation and dependencies.
:::

## Use a PVC

Create a claim:

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

Add the deployment option:

```hocon
kubernetes.checkpoint-pvc = "seatunnel-checkpoints"
```

Configure native storage in the `seatunnel.yaml` stored in the runtime ConfigMap:

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

:::caution
The provider only mounts the PVC. Native checkpoint storage interprets the backend and namespace.
The master user must be able to write to the volume. The application never deletes this PVC.
:::

## Use remote storage

Configure the complete `seatunnel.engine.checkpoint.storage.plugin-config` in the runtime ConfigMap's `seatunnel.yaml`. Application Mode does not parse checkpoint URIs or maintain a backend allowlist. See [Checkpoint Storage](../../../engines/zeta/checkpoint-storage.md) for HDFS, OSS, S3, COS, endpoint, and authentication settings.

## Submit a recovery job

Enable periodic checkpoints and retain the Zeta job ID printed during the original submission:

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  checkpoint.retain-after-job-cancelled = true
}
```

After the original application has stopped and a completed checkpoint exists:

```bash
bin/seatunnel-application.sh submit --target kubernetes \
  --config job.conf --application-config kubernetes.conf \
  --restore-job-id PREVIOUS_JOB_ID
```

:::caution
The new submission uses a new job ID and loads the historical job's latest eligible checkpoint.
A missing checkpoint fails the job on the master (the `submit` command has already returned)
instead of silently starting over.
:::

Keep topology, connector versions, state format, and parallelism compatible. Replay after the checkpoint and sink delivery guarantees depend on connector semantics.

## Ordinary HA storage

For HA, every master must access the same durable checkpoint backend and namespace. Use a shared multi-node capable PVC for file-backed storage, or the native remote backend configuration. `--restore-job-id` uses checkpoint restore, fails when no eligible checkpoint exists, and assigns a new destination job ID. Do not run the historical and restored applications concurrently against the same sink without connector-specific coordination.
