---
sidebar_position: 1
title: Kubernetes Overview
---

# Kubernetes Application Mode

Kubernetes Application Mode creates an independent Kubernetes application for one SeaTunnel job. A `batch/v1` Job hosts the only Zeta master. The master requests a fixed number of worker Pods. Every process uses the same SeaTunnel image, and worker and application resources are cleaned up when the job terminates.

Use it when Kubernetes manages compute resources and jobs require isolated resources, dependencies, and failure boundaries. Execution uses the native Zeta Engine without Flink or Spark.

## Runtime layout

| Component | Kubernetes resource | Responsibility |
| --- | --- | --- |
| Application master | One Job Pod | Start the Zeta master, create workers, submit the job, and coordinate cleanup |
| Worker | N regular Pods | Join the isolated Hazelcast cluster and provide fixed task slots |
| Job configuration | Inline master environment; optional mounted `job.conf` | `kubernetes.job-config-file` selects a ConfigMap or Secret file; unset keeps the submitted `-c` file |
| Runtime configuration | Existing or created ConfigMap | Mounted read-only by the master and workers with `seatunnel.yaml`, Hazelcast, JVM option and logging files |
| Master discovery | Master Pod IP | Workers join the advertised Pod IP over TCP; the headless Service only selects the master Pod |
| Checkpoint | PVC or supported remote storage | Preserve recoverable state outside the application |

:::caution
Losing the master or any worker fails the current Job. Workers are not replaced and no standby
master is started. A new application can restore from a durable checkpoint.
:::

## Capabilities

| Area | Supported in this release |
| --- | --- |
| Submission | `submit`, `status`, `cancel`; submission returns as soon as the Job is started, `--wait` opts into waiting |
| Topology | One master and a fixed number of workers, each with its own CPU, memory and slots; one native job per application |
| Job configuration | Submitted `-c` file or a file mounted from a Secret/ConfigMap; `${VAR}` placeholders resolved inside the Pod |
| Secrets and ConfigMaps | Reference existing objects, which SeaTunnel never creates or deletes, or let it create owned ones; every mounted key declares its full path and uses `subPath` |
| Environment | Per-variable `kubernetes.env.*` and whole-object `envFrom`, sourced from literals, Secrets or ConfigMaps |
| Runtime files | Engine, Hazelcast, JVM-options and Log4j2 file selection, each of which can be disabled |
| Scheduling | Namespace, ServiceAccount, image pull Secrets, labels, annotations, node selectors and tolerations |
| Storage and recovery | Checkpoint PVC mount and recovery of an earlier job through `application.restore-job-id` |
| Lifecycle | Worker loss or master exit fails the application; `cancel` removes every owned resource and keeps referenced ones; finished Jobs are retained for `kubernetes.finished-job-retention-seconds` |

:::note
Not in this release: HA master failover, worker replacement or autoscaling, arbitrary Pod templates,
affinity/PriorityClass, Kerberos and REST/HTTPS. A failed application can be resumed from its last
durable checkpoint with `application.restore-job-id`.
:::

## Prerequisites

- Kubernetes 1.24 or later with `batch/v1` Jobs, suspended Jobs, and Job TTL support.
- `kubectl` and a submitter kubeconfig, or a suitable in-cluster identity.
- An existing namespace, ServiceAccount, and minimal RBAC.
- An existing ConfigMap with the SeaTunnel runtime configuration, or the
  `kubernetes.configmap.<entry>.data` route that has SeaTunnel create one.
- Hazelcast TCP connectivity between master and worker Pods.
- A cluster that can pull an image containing SeaTunnel, the Kubernetes provider, and job plugins.
- Submission permission to create and manage Jobs and Services, plus the Secret/ConfigMap entries
  the deployment creates; worker Pods are created by the application ServiceAccount.

## Recommended reading order

| Phase | Document | Content |
| --- | --- | --- |
| Understand deployment | [Cluster architecture](architecture.md) | Job, worker Pods, Service, Secret, ConfigMap, and storage relationships |
| Run the first job | [Quick start](quick-start.md) | Build an image, create RBAC, and submit a job |
| Operate an application | [Commands](commands.md) | Submit, inspect, wait for, and cancel an application |
| Tune deployment | [Configuration](configuration.md) | Complete Kubernetes and shared application options |
| Configure recovery | [Checkpoint recovery](checkpoint-recovery.md) | PVC, HDFS, object storage, and explicit recovery |
| Troubleshoot | [FAQ](faq.md) | Status, cancellation, logs, cleanup, and common failures |

See the shared [Application Mode overview](../overview.md) for common behavior.
