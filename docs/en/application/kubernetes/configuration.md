---
sidebar_position: 4
title: Configuration
---

# Kubernetes configuration

## Kubernetes options

| Option | Default | Description |
| --- | --- | --- |
| `kubernetes.namespace` | `default` | Existing namespace containing all application resources. |
| `kubernetes.image` | Required | Image containing SeaTunnel, the provider, and job plugins. |
| `kubernetes.image-pull-policy` | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `kubernetes.image-pull-secrets` | Empty | Comma-separated existing Secret names used to pull images from private registries. |
| `kubernetes.service-account` | `default` | Existing ServiceAccount used by the master. |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | Absolute distribution path in the image, without `:`. |
| `kubernetes.config-map` | Unset | Existing ConfigMap mounted read-only at `<seatunnel-home>/config` in both master and worker Pods. |
| `kubernetes.kubeconfig` | SDK default | Local submitter kubeconfig; it is not sent to the master. Configure it in a permission-restricted deployment file rather than with `-D`. |
| `kubernetes.finished-job-retention-seconds` | `86400` | Positive retention period for completed or failed Jobs. |
| `kubernetes.checkpoint-pvc` | Unset | Existing PVC mounted at `/opt/seatunnel/checkpoints` in the master. |
| `kubernetes.master.labels` | Empty | Additional master Pod labels as comma-separated `key:value` pairs. SeaTunnel ownership labels are reserved. |
| `kubernetes.worker.labels` | Empty | Additional worker Pod labels as comma-separated `key:value` pairs. SeaTunnel ownership labels are reserved. |
| `kubernetes.master.annotations` | Empty | Master Pod annotations as comma-separated `key:value` pairs. |
| `kubernetes.worker.annotations` | Empty | Worker Pod annotations as comma-separated `key:value` pairs. |
| `kubernetes.master.node-selector` | Empty | Master Pod node selector as comma-separated `key:value` pairs. |
| `kubernetes.worker.node-selector` | Empty | Worker Pod node selector as comma-separated `key:value` pairs. |

## Shared application options

| Option | Default | Description |
| --- | --- | --- |
| `application.name` | `seatunnel` | Readable prefix of the unique Job name. |
| `application.job-id` | Generated | Positive native Zeta job ID. |
| `application.restore-job-id` | Unset | Historical job ID whose latest eligible checkpoint initializes this execution. |
| `application.worker-count` | `1` | Fixed number of worker Pods. |
| `application.worker.memory-mb` | `1024` | Worker memory request and limit in MiB. |
| `application.worker.cpu-cores` | `1` | Worker CPU request and limit. |
| `application.worker.slots` | `2` | Fixed execution slots per worker. |
| `application.master.memory-mb` | `1024` | Master memory request and limit in MiB. |
| `application.master.cpu-cores` | `1` | Master CPU request and limit. |
| `application.master.port` | `5801` | Master Hazelcast TCP port. |
| `application.startup-timeout-millis` | `120000` | Applied separately to master startup and worker creation and registration. |

Pod JVM heap uses 75% of the memory limit, leaving the remainder for off-heap allocations and JVM overhead.

`application.startup-timeout-millis` is not a job execution limit. Image pull, admission, or scheduling failures can exhaust the submitter's master-startup deadline. The master uses the same timeout separately for workers.

## Images and credentials

Use `kubernetes.image-pull-secrets` for existing private-registry Secrets. The provider does not create those credentials or expose an arbitrary Pod template. Local files and plugin JARs referenced by a job are not downloaded dynamically and must already be present in the image.

Set `kubernetes.config-map` to the name of an existing ConfigMap containing the SeaTunnel runtime configuration, including `seatunnel.yaml` and `log4j2_client.properties`. The submitter verifies that the ConfigMap exists before it creates the application Job. Kubernetes then mounts it read-only at `<seatunnel-home>/config` in both the master and worker Pods. SeaTunnel treats this ConfigMap as user-owned and does not delete it when the application ends.

The resolved job configuration is stored in a namespace-scoped Kubernetes Secret and mounted read-only only by the master. The Secret may contain connector credentials. Restrict Secret `get` and `list` permissions and enable Kubernetes encryption at rest when required; a Secret's base64 representation alone is not encryption. Prefer credential mechanisms supported by the connector, filesystem, or cluster.

Use command-line `-Dkey=value` only for non-sensitive overrides because command-line arguments may be visible to other users on the submission host. Put `kubernetes.kubeconfig` and other sensitive deployment values in a permission-restricted deployment configuration file, or rely on the Kubernetes SDK default credentials.

## Capacity planning

Pod requests equal limits. Plan worker count, worker slots, job parallelism, ResourceQuota, LimitRange, and node capacity together. The master does not provide task slots.
