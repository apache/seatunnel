# Web UI

## 从这里开始

建议把 [REST API 与 Web UI](./rest-api-and-web-ui.md) 作为运维入口页先读完。那一页会先解释什么时候启用 HTTP 服务、接下来该看哪些 REST API 页面，以及 Web UI 在日常运维里的位置。

本页只聚焦 Web UI 各个界面本身。

## 访问

在访问 web ui 之前我们需要开启 http rest api。首先需要在`seatunnel.yaml`配置文件中配置

```
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

然后访问 `http://ip:8080/#/overview`

## 概述

Apache SeaTunnel 的 Web UI 提供了一个友好的用户界面，用于监控和管理 SeaTunnel 作业。通过 Web UI，用户可以实时查看当前运行的作业、已完成的作业，以及集群中工作节点和管理节点的状态。主要功能模块包括 Jobs、Workers 和 Master，每个模块都提供了详细的状态信息和操作选项，帮助用户高效地管理和优化其数据处理流程。
![overview.png](../../../images/ui/overview.png)

## 作业

### 运行中的作业

“运行中的作业”模块列出了当前正在执行的所有 SeaTunnel 作业。用户可以查看每个作业的基本信息，包括作业 ID、提交时间、状态、执行时间等。点击具体作业可以查看更多详细信息，如任务分布、资源使用情况和日志输出，便于用户实时监控作业进度并及时处理潜在问题。
![running.png](../../../images/ui/running.png)
![detail.png](../../../images/ui/detail.png)

#### 实时可观测性（Realtime Observability）

在 Job Detail 页面中，DAG 图支持展示“最近 N 分钟”的实时指标（默认 3 分钟，最大 10 分钟）：

- **节点忙碌度**：Source/Transform/Sink 的忙闲比例（例如 Source Read/Idle，Transform Busy，Sink Busy）。
- **边的下游等待占比**：当作业在某些位置插入了队列（例如 async boundary 队列、sink 前拆分 IO 队列）时，边会根据下游等待占比与队列填充率进行着色/加粗。
- **交互**：点击节点或边可在右侧抽屉查看该对象的实时曲线与关键字段。

> 该能力需要作业侧开启 `env.engine.observability`（或满足默认开启条件），并按需配置 `async_boundaries`、`split_sink_io` 等。
> 详细配置与指标说明请参考：[实时可观测性](realtime-observability.md)。

### 已完成的作业

“已完成的作业”模块展示了所有已成功完成或失败的 SeaTunnel 作业。此部分提供了每个作业的执行结果、完成时间、耗时以及失败原因（如果有）。用户可以通过此模块回顾过去的作业记录，分析作业性能，进行故障排查或重复执行某些特定作业。
![finished.png](../../../images/ui/finished.png)

## 工作节点

### 工作节点信息

“工作节点”模块展示了集群中所有工作节点的详细信息，包括每个工作节点的地址、运行状态、CPU 和内存使用情况、正在执行的任务数量等。通过该模块，用户可以监控各个工作节点的健康状况，及时发现和处理资源瓶颈或节点故障，确保 SeaTunnel 集群的稳定运行。
![workers.png](../../../images/ui/workers.png)

## 管理节点

### 管理节点信息

“管理节点”模块提供了 SeaTunnel 集群中主节点的状态和配置信息。用户可以查看 Master 节点的地址、运行状态、负责的作业调度情况以及整体集群的资源分配情况。该模块帮助用户全面了解集群的核心管理部分，便于进行集群配置优化和故障排查。
![master.png](../../../images/ui/master.png)

## 下一步

- [REST API 与 Web UI](./rest-api-and-web-ui.md)
- [REST API V2](./rest-api-v2.md)
- [安全](./security.md)
