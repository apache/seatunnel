---
sidebar_position: 3
---

# SeaTunnel Engine(Zeta) Deployment

SeaTunnel Engine(Zeta) supports three different deployment modes: local mode, hybrid cluster mode, and separated cluster mode.

Each deployment mode has different usage scenarios, advantages, and disadvantages. You should choose a deployment mode according to your needs and environment.

**Local mode:** Only used for testing, each task will start an independent process, and the process will exit after the task is completed.

**Hybrid cluster mode:** The Master service and Worker service of SeaTunnel Engine are mixed in the same process. All nodes can run jobs and participate in the election to become the master, that is, the master node is also running synchronous tasks simultaneously. In this mode, Imap (saving the state information of the task to provide support for the fault tolerance of the task) data will be distributed among all nodes.

**Separated cluster mode(experimental feature):** The Master service and Worker service of SeaTunnel Engine are separated, and each service is a single process. The Master node is only responsible for job scheduling, rest api, task submission, etc., and Imap data is only stored in the Master node. The Worker node is only responsible for the execution of the task, does not participate in the election to become the master, and does not store Imap data.

**Usage suggestion:** Although [Separated Cluster Mode](separated-cluster-deployment.md) is an experimental feature, the first recommended usage will be made in the future. In the hybrid cluster mode, the Master node needs to run tasks synchronously. When the task scale is large, it will affect the stability of the Master node. Once the Master node crashes or the heartbeat times out, it will lead to the switch of the Master node, and the switch of the Master node will cause fault tolerance of all running tasks, which will further increase the load of the cluster. Therefore, we recommend using the separated mode more.

[Local Mode Deployment](local-mode-deployment.md)

[Hybrid Cluster Mode Deployment](hybrid-cluster-deployment.md)

[Separated Cluster Mode Deployment](separated-cluster-deployment.md)
