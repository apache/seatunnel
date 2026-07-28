---
sidebar_position: 1
---

# REST API 与 Web UI

SeaTunnel Engine 通过 REST API 和 Web UI 提供远程运维与可视化观测入口。本页说明两者的关系、如何开启共用的 HTTP 服务，以及下一步应该阅读哪些参考文档。

## 应该先看哪一页

| 需求 | 阅读入口 |
|------|----------|
| 开启 HTTP 服务、选择端口或配置 `context-path` | 本页，然后阅读 [RESTful API V2](./rest-api-v2.md) |
| 构建自动化脚本、运维平台或系统集成 | [RESTful API V2](./rest-api-v2.md) |
| 通过 HTTP 提交、停止、取消、savepoint 或恢复作业 | [作业生命周期 API](./rest-api-job-lifecycle.md) |
| 可视化查看集群健康、作业、DAG 指标和日志 | [Web UI](./web-ui.md) |
| 配置 HTTPS 或 HTTP Basic 认证 | [安全配置](./security.md) |
| 维护旧版 Hazelcast REST 客户端 | [RESTful API V1](./rest-api-v1.md) |

## REST API 与 Web UI 的关系

REST API 和 Web UI 不是两套独立服务。它们都依赖 SeaTunnel Engine 的同一套 HTTP 能力：

- **REST API** 是完整的 HTTP 接口，面向自动化和平台集成，覆盖元数据发现、作业提交、作业状态、生命周期操作、日志、checkpoint、worker 信息和实时指标。
- **Web UI** 是内置可视化控制台，面向人工巡检，适合查看 overview、运行中和已完成作业、作业详情、DAG 指标、日志、worker 状态和 master 状态。

需要快速查看运行态时使用 Web UI；需要提交、停止、取消、savepoint、恢复或批量自动化时，使用 REST API 或命令行。

## 开启 HTTP 服务

在使用这两类入口之前，需要先在 `seatunnel.yaml` 中开启 HTTP 服务：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

生产环境常用的可选配置包括：

- `context-path`：给所有 HTTP 接口加统一前缀
- `enable-dynamic-port`：当指定端口被占用时自动探测可用端口
- `enable-https`：开启 HTTPS
- `enable-basic-auth`：使用 HTTP Basic 认证保护接口

完整的 REST 参数说明请查看 [RESTful API V2](./rest-api-v2.md)。如果需要 HTTPS 和认证配置，请继续查看 [安全配置](./security.md)。

## 访问 Web UI

HTTP 开启后，可以通过下面的地址访问：

```text
http://<host>:<port>/#/overview
```

如果配置了 `context-path`，需要把 UI 路由放到该前缀下面：

```text
http://<host>:<port>/<context-path>/#/overview
```

## 当前 Web UI 能力

当前 Web UI 是一个可视化巡检控制台，支持：

| 区域 | 可以做什么 |
|------|------------|
| Overview | 查看项目版本、集群 slot、worker 数量和作业数量 |
| Jobs | 分页浏览运行中和已完成作业，并进入作业详情页 |
| Job Detail | 查看作业配置、DAG、source 和 sink 指标、作业日志，以及开启后的实时可观测指标 |
| Workers | 查看 worker 节点系统监控信息 |
| Master | 查看 master 节点系统监控信息 |

界面级说明请继续查看 [Web UI](./web-ui.md)。

## REST API 的典型用途

REST API 常见的使用方式包括：

- 获取连接器 `OptionRule` 元数据，用于动态表单
- 查询集群概览和作业状态
- 将 SeaTunnel Engine 集成到内部运维平台
- 向监控或调度系统暴露运行时状态

最常用的参考文档是：

- [RESTful API V2](./rest-api-v2.md)
- [作业生命周期 API](./rest-api-job-lifecycle.md)
- [RESTful API V1](./rest-api-v1.md)

如果是新接入系统，建议优先使用 **V2**；只有在兼容旧客户端时再考虑 V1。

## 推荐的运维使用顺序

### 1. 先开启 HTTP 能力

- 在 `seatunnel.yaml` 中配置 `seatunnel.engine.http`
- 提前决定是否需要 context path、动态端口、HTTPS 和基础认证

### 2. 先验证 REST 接口可达

- 先请求 overview、running jobs 等接口
- 确认从你的运维环境可以访问到 SeaTunnel Engine

### 3. 再打开 Web UI 做可视化检查

- 用 UI 验证集群健康情况和作业细节

### 4. 需要操作控制时使用 REST API

- 需要提交、停止、取消、savepoint 和恢复作业时，通过生命周期 API 完成
- Web UI 作为这些操作前后的可视化巡检入口

### 5. 生产环境补齐安全配置

- 如果接口会暴露给更广的内网或外部系统，建议开启 HTTPS 与认证

## 继续阅读

- [RESTful API V2](./rest-api-v2.md)
- [作业生命周期 API](./rest-api-job-lifecycle.md)
- [RESTful API V1](./rest-api-v1.md)
- [安全配置](./security.md)
- [Web UI](./web-ui.md)
