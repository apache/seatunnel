---
sidebar_position: 2
title: Cluster Architecture
---

# Kubernetes cluster architecture

Each submission creates resources with a unique application ID in the target namespace. The Job Pod runs the master. Worker Pods do not mount Kubernetes API tokens and communicate with the master through Hazelcast TCP.

```mermaid
flowchart TB
  client["SeaTunnel Application CLI"]
  api["Kubernetes API Server"]
  image["SeaTunnel application image"]
  checkpoint[("PVC / HDFS / Object storage")]

  subgraph ns["Application Namespace"]
    job["batch/v1 Job"]
    config["ConfigMap<br/>job and deployment config"]
    service["Headless Service<br/>master discovery"]
    master["Master Pod<br/>Zeta Master"]
    worker1["Worker Pod 1"]
    workerN["Worker Pod N"]
  end

  client -->|create / status / cancel| api
  api --> job
  job --> master
  config -. mount .-> master
  service -. resolve master .-> worker1
  service -. resolve master .-> workerN
  master -->|create fixed workers| api
  api --> worker1
  api --> workerN
  image -. same immutable runtime .-> master
  image -. same immutable runtime .-> worker1
  image -. same immutable runtime .-> workerN
  worker1 -->|Hazelcast TCP| master
  workerN -->|Hazelcast TCP| master
  master --> checkpoint

  classDef layerBlue fill:#0f1d33,stroke:#5db8e2,stroke-width:2px,color:#f8fbff;
  classDef layerCyan fill:#0c2530,stroke:#2dd4bf,stroke-width:2px,color:#f8fbff;
  classDef layerPurple fill:#1f1a34,stroke:#8d7cf6,stroke-width:2px,color:#f8fbff;
  class client,api layerCyan;
  class ns,job,master,worker1,workerN layerBlue;
  class config,service,image,checkpoint layerPurple;
  linkStyle default stroke:#5db8e2,stroke-width:2px;
```

## Resource creation order

1. The submitter creates a suspended Job with `backoffLimit: 0`.
2. After receiving the Job UID, it creates an owner-referenced ConfigMap and headless Service.
3. It unsuspends the Job after localization is ready and waits for the master Pod to run.
4. The master starts an isolated Zeta cluster and creates a fixed number of worker Pods through the Kubernetes API.
5. Workers resolve the master through the headless Service, join the cluster, and register slots.
6. The master submits one Zeta job after every worker is ready.
7. After the job terminates, the master removes workers and exits. The Job records `Complete` or `Failed`.

## RBAC boundary

The submitter identity creates Jobs, ConfigMaps, and Services and performs status and cancellation operations. The application ServiceAccount only needs to read its Job and manage worker Pods. Only the master mounts the ServiceAccount token; workers do not access the Kubernetes API.

## Network and capacity

Hazelcast TCP must be allowed between master and workers. A NetworkPolicy must permit application Pods to reach the master port and required member traffic.

The master does not provide task slots. Fixed capacity is:

```text
application.worker-count × application.worker.slots
```

Pod requests equal limits. Namespace quota, LimitRange, admission policies, and node capacity can prevent scheduling.

## Ownership and durable data

The ConfigMap and Service are owned by the Job and are garbage-collected when it is deleted. Workers are explicitly cleaned by the application and carry application labels for fallback deletion.

A caller-owned checkpoint PVC has no Job owner reference, so success, failure, cancellation, or TTL expiry does not delete it. Remote checkpoint storage is also independent of the application lifecycle.
