---
sidebar_position: 2
title: Architecture Overview
---

# Application Mode architecture overview

Application Mode creates an independent Zeta cluster for one SeaTunnel job. The platform starts and reclaims processes. Zeta continues to own worker registration, slot scheduling, task execution, and checkpoints.

```mermaid
flowchart LR
  client["SeaTunnel Application CLI"]
  platform["YARN / Kubernetes"]

  subgraph app["One application"]
    master["Application Master<br/>Zeta Master"]
    worker1["Zeta Worker 1"]
    workerN["Zeta Worker N"]
  end

  checkpoint[("Persistent checkpoint storage")]

  client -->|submit / status / cancel| platform
  platform --> master
  master -->|request fixed workers| platform
  platform --> worker1
  platform --> workerN
  worker1 -->|Hazelcast TCP| master
  workerN -->|Hazelcast TCP| master
  master --> checkpoint

  classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
  classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
  classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;
  class client layerCyan;
  class platform,master,worker1,workerN layerBlue;
  class checkpoint layerPurple;
  linkStyle default stroke:#5db8e2,stroke-width:2px;
```

## Roles and responsibilities

| Role | Responsibility |
| --- | --- |
| Submission client | Read job and deployment configuration; submit, inspect, or cancel an application |
| Resource platform | Start master and workers, record application state, and reclaim compute resources |
| Master | Create an isolated Zeta cluster, request workers, submit one job, and coordinate cleanup |
| Worker | Provide fixed slots and execute source, transform, and sink task groups |
| Checkpoint storage | Preserve job state required for recovery outside application processes |

One master can coordinate multiple workers. Fixed slot capacity is `application.worker-count × application.worker.slots`; effective parallelism also depends on the job topology.

## Runtime flow

```mermaid
sequenceDiagram
    participant CLI
    participant Platform as YARN / Kubernetes
    participant Master
    participant Worker
    participant Engine as Zeta Engine

    CLI->>Platform: submit job and deployment configuration
    Platform->>Master: start application master
    Master->>Platform: request a fixed number of workers
    Platform->>Worker: start N workers
    Worker->>Master: join cluster and register slots
    Master->>Engine: submit one native Zeta job
    Engine-->>Master: SUCCEEDED / FAILED / CANCELED
    Master->>Platform: release workers and exit
```

Closing the CLI does not cancel the remote application. Cancellation is explicit, which allows detached submission followed by status or cancellation using the application ID.

## Failure and recovery

This release has one master and sets `backup-count=0`. Losing the master or a worker fails the current application. There is no automatic takeover or worker replacement inside that application.

Persistent checkpoints and live HA are different capabilities. A job can write checkpoints to HDFS, OSS, S3, COS, or a Kubernetes persistent volume. After a failure, create a new application with the historical Zeta job ID. The new master loads the latest eligible checkpoint and restores source and task state.

```mermaid
sequenceDiagram
    participant Old as Application A / Job A
    participant Store as Persistent checkpoint storage
    participant CLI
    participant New as Application B / Job B

    Old->>Store: write completed checkpoint
    Old--xOld: master or worker failure
    CLI->>New: resubmit with Job A as restore-job-id
    New->>Store: load the latest eligible checkpoint for Job A
    Store-->>New: source and task state
    New->>New: resume with a new job ID
```

Storage must survive both applications. A YARN checkpoint directory must be outside staging. A Kubernetes PVC is caller-owned and is not deleted by the application. See the platform recovery guides for exact steps.

## Platform differences

| Resource | YARN | Kubernetes |
| --- | --- | --- |
| Master | ApplicationMaster container | Kubernetes Job Pod |
| Worker | YARN container | Worker Pod |
| Runtime files | Distribution localized from a shared filesystem | One image shared by master and workers |
| State | YARN application state | Kubernetes Job condition |
| Cleanup | Release containers and remove staging | Delete worker Pods and application resources |

Continue with [YARN Application Mode](yarn/overview.md) or [Kubernetes Application Mode](kubernetes/overview.md).
