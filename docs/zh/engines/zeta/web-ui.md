# Web UI

## 从这里开始

建议把 [HTTP 运维能力：REST API 与 Web UI](./rest-api-and-web-ui.md) 作为运维入口页先读完。那一页会先解释什么时候启用 HTTP 服务、接下来该看哪些 REST API 页面，以及 Web UI 在日常运维里的位置。

本页聚焦 Web UI 各个界面本身，以及当前内置控制台的能力边界。

## 访问

访问 Web UI 前，需要先在 `seatunnel.yaml` 中开启 SeaTunnel Engine HTTP 服务：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

然后访问：

```text
http://<host>:8080/#/overview
```

如果配置了 `context-path`，需要把它放在 hash 路由之前：

```text
http://<host>:8080/<context-path>/#/overview
```

## 概述

Apache SeaTunnel 的 Web UI 是 SeaTunnel Engine 的可视化巡检控制台。它可以帮助运维人员查看集群概览、运行中和已完成作业、作业详情页、日志、实时 DAG 指标，以及 worker 和 master 节点状态。

当前 UI 的重点是巡检与观测。作业提交、停止、取消、savepoint、恢复和批量自动化等生命周期操作，请通过 [REST API](./rest-api-v2.md)、[作业生命周期 API](./rest-api-job-lifecycle.md) 或命令行完成。

![overview.png](../../../images/ui/overview.png)

## 能力总览

| UI 区域 | 当前能力 | 需要 REST API 或 CLI 完成 |
|---------|----------|---------------------------|
| Overview | 查看集群版本、slot 使用、worker 数量和作业数量 | 修改集群配置 |
| Jobs | 查看运行中和已完成作业、分页浏览作业列表、进入作业详情 | 提交、停止、取消、savepoint、恢复或批量操作作业 |
| Job Detail | 查看 DAG、作业指标、异常文本、作业配置、日志，以及开启后的实时可观测指标 | 编辑作业配置或改变作业生命周期状态 |
| Workers | 查看 worker 节点系统监控信息 | 更新 worker tag 或把 worker 状态接入自动化 |
| Master | 查看 master 节点系统监控信息 | 修改安全、HTTP 或集群级配置 |

## 作业

### 运行中的作业

“运行中的作业”模块列出当前正在执行的 SeaTunnel 作业。用户可以查看作业 ID、作业名称、创建时间、状态，并进入具体作业的详情页。

列表会周期性刷新，并支持分页。当前 UI 不提供作业提交、停止或取消按钮。

![running.png](../../../images/ui/running.png)
![detail.png](../../../images/ui/detail.png)

### 作业详情

作业详情页包含四个主要 tab：

- **Overview**：展示作业 DAG、source 和 sink 吞吐指标、flush signal 指标，以及开启可观测性后的 vertex 或 edge 实时指标。
- **Exception**：当作业失败或上报异常时，展示异常文本。
- **Configuration**：展示引擎暴露的运行时作业配置。
- **Log**：展示引擎日志 API 返回的作业日志文件。

#### 实时可观测性（Realtime Observability）

在 Job Detail 页面中，DAG 图支持展示“最近 N 分钟”的实时指标（默认 3 分钟，最大 10 分钟）：

- **节点忙碌度**：Source/Transform/Sink 的忙闲比例（例如 Source Read/Idle，Transform Busy，Sink Busy）。
- **边的下游等待占比**：当作业在某些位置插入了队列（例如 async boundary 队列、sink 前拆分 IO 队列）时，边会根据下游等待占比与队列填充率进行着色/加粗。
- **交互**：点击节点或边可在右侧抽屉查看该对象的实时曲线与关键字段。

> 该能力需要作业侧开启 `env.engine.observability`（或满足默认开启条件），并按需配置 `async_boundaries`、`split_sink_io` 等。
> 详细配置与指标说明请参考：[实时可观测性](realtime-observability.md)。

### 已完成的作业

“已完成的作业”模块展示已进入终态的作业，例如 finished、failed、cancelled 或 savepoint done。用户可以回看历史记录，并进入详情页查看配置、异常文本、引擎保留的指标和日志。

当前 UI 不提供重跑已完成作业的能力。如需从 checkpoint 或 savepoint 恢复，请使用 [作业生命周期 API](./rest-api-job-lifecycle.md) 或命令行。

![finished.png](../../../images/ui/finished.png)

## 工作节点

### 工作节点信息

“工作节点”模块展示 worker 节点的系统监控信息。可以用它查看 worker 地址、资源状态和引擎暴露的运行时健康信号。

当前 UI 对 worker 是只读视图。需要更新节点 tag 或把 worker 状态接入自动化时，请使用 [RESTful API V2](./rest-api-v2.md)。

![workers.png](../../../images/ui/workers.png)

## 管理节点

### 管理节点信息

“管理节点”模块展示 master 节点的系统监控信息。可以用它查看当前 master 侧运行状态和引擎暴露的资源信号。

当前 UI 不编辑 HTTP、安全、存储、调度或其他集群级配置。这些设置需要在 SeaTunnel 配置文件中维护，并通过相关 REST API 页面做运行时检查。

![master.png](../../../images/ui/master.png)

## 缺少的 UI 操作能力

从产品规划视角看，当前 Web UI 最应该补齐的能力包括：

- 通过配置文本或上传文件提交作业
- graceful stop、force cancel、stop-with-savepoint 和从 savepoint 恢复
- checkpoint overview 和 checkpoint history 视图
- connector option 元数据浏览，用于动态表单构建
- worker tag 更新操作
- 安全配置和 HTTP 配置状态检查

在这些控制能力进入 UI 之前，应把 REST API V2 和命令行视为操作控制面，把 Web UI 视为内置可视化巡检面。

## 下一步

- [HTTP 运维能力：REST API 与 Web UI](./rest-api-and-web-ui.md)
- [REST API V2](./rest-api-v2.md)
- [作业生命周期 API](./rest-api-job-lifecycle.md)
- [安全](./security.md)
