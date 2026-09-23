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
| Job configuration | ConfigMap | Mounted only by the master with resolved job and deployment information |
| Master discovery | Headless Service | Provide the current application's master address to workers |
| Checkpoint | PVC or supported remote storage | Preserve recoverable state outside the application |

Losing the master or any worker fails the current Job. Workers are not replaced and no standby master is started. A new application can restore from a durable checkpoint.

## Prerequisites

- Kubernetes 1.24 or later with `batch/v1` Jobs, suspended Jobs, and Job TTL support.
- `kubectl` and a submitter kubeconfig, or a suitable in-cluster identity.
- An existing namespace, ServiceAccount, and minimal RBAC.
- Hazelcast TCP connectivity between master and worker Pods.
- A cluster that can pull an image containing SeaTunnel, the Kubernetes provider, and job plugins.
- Submission permission to create and manage Jobs, ConfigMaps, Services, and Pods.

## Recommended reading order

| Phase | Document | Content |
| --- | --- | --- |
| Understand deployment | [Cluster architecture](architecture.md) | Job, worker Pods, Service, ConfigMap, and storage relationships |
| Run the first job | [Quick start](quick-start.md) | Build an image, create RBAC, and submit a job |
| Tune deployment | [Configuration](configuration.md) | Complete Kubernetes and shared application options |
| Configure recovery | [Checkpoint recovery](checkpoint-recovery.md) | PVC, HDFS, object storage, and explicit recovery |
| Troubleshoot | [FAQ](faq.md) | Status, cancellation, logs, cleanup, and common failures |

See the shared [Application Mode overview](../overview.md) and [architecture](../architecture.md) for common behavior.
