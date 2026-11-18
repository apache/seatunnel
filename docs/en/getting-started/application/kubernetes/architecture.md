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
    appConfig["Created Secret / ConfigMap<br/>optional runtime files"]
    runtimeConfig["ConfigMap<br/>SeaTunnel runtime configuration"]
    service["Headless Service<br/>master Pod selector"]
    master["Master Pod<br/>Zeta Master"]
    worker1["Worker Pod 1"]
    workerN["Worker Pod N"]
  end

  client -->|create / status / cancel| api
  api --> job
  job --> master
  appConfig -. mount .-> master
  appConfig -. mount .-> worker1
  appConfig -. mount .-> workerN
  runtimeConfig -. read-only mount .-> master
  runtimeConfig -. read-only mount .-> worker1
  runtimeConfig -. read-only mount .-> workerN
  master -. pod IP .-> worker1
  master -. pod IP .-> workerN
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
  class appConfig,runtimeConfig,service,image,checkpoint layerPurple;
  linkStyle default stroke:#5db8e2,stroke-width:2px;
```

## Resource creation order

1. The submitter validates the user-owned runtime ConfigMap, then creates a suspended Job with `backoffLimit: 0`.
2. After receiving the Job UID, it creates the headless Service and, when `kubernetes.secret.*` or
   `kubernetes.configmap.<entry>.data` is configured, the owner-referenced application Secret and
   ConfigMap.
3. After the API confirms the dependent resources, it unsuspends the Job and returns the Job name without waiting for the master Pod. A TCP startup probe gives the Zeta master its configured startup window; readiness and liveness probes then check the Hazelcast master port. Use `status` or Pod events to observe slow image pulls, admission or scheduling problems.
4. The master starts an isolated Zeta cluster and creates a fixed number of worker Pods through the Kubernetes API. The master watches the application-labelled Pods through the Kubernetes watch API, so a deleted worker or a worker in a terminal phase is reported immediately; worker Pods carry no liveness probe.
5. Workers join the master over TCP at its Pod IP and register slots; the headless Service only selects the master Pod. Pod phase is used for failure detection; Zeta registration remains the source of worker readiness and slot capacity.
6. The master submits one Zeta job after every worker is ready.
7. After the job terminates, the master removes workers and exits. The Job records `Complete` or `Failed`.

## RBAC boundary

The submitter identity creates Jobs, Services, and the Secret/ConfigMap entries it declares, and
performs status and cancellation operations. The application ServiceAccount only needs to read its
Job and manage worker Pods. Only the master mounts the ServiceAccount token; workers do not access
the Kubernetes API.

## Network and capacity

Hazelcast TCP must be allowed between master and workers. A NetworkPolicy must permit application Pods to reach the master port and required member traffic.

The master does not provide task slots. Fixed capacity is:

```text
application.worker-count × application.worker.slots
```

Pod requests equal limits. Namespace quota, LimitRange, admission policies, and node capacity can prevent scheduling.

The Kubernetes API acknowledges Job, Secret, ConfigMap, and Service creation synchronously before the
Job is started. Runtime watches are therefore used for externally changing Pod lifecycle state. A
failed or ambiguous submission request compensates exactly the resources created by that submission,
so pre-existing resources with the same name are never deleted.

## Ownership and durable data

The created Secret/ConfigMap and the Service are owned by the Job and are garbage-collected when it
is deleted. Workers are explicitly cleaned by the application and carry application labels for
fallback deletion. Referenced ConfigMaps are user-owned, shared by the master and workers, and
retained after application cleanup.

A caller-owned checkpoint PVC has no Job owner reference, so success, failure, cancellation, or TTL expiry does not delete it. Remote checkpoint storage is also independent of the application lifecycle.
