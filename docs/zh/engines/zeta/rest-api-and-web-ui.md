---
sidebar_position: 1
---

# REST API 与 Web UI

SeaTunnel Engine 在同一套 HTTP 服务之上提供了两类运维入口：

- 面向程序调用、自动化和平台集成的 **REST API**
- 面向人工排障与可视化观测的 **Web UI**

这页是运维侧的统一入口。它的目标是帮助你先理解什么时候需要开启 HTTP 能力、REST API 和 Web UI 的关系是什么，以及接下来该去看哪一页详细文档。

## 什么时候先看这页

如果你有以下需求，建议先从本页开始：

- 不依赖 CLI 查看运行中或已完成的作业
- 把 SeaTunnel Engine 接入外部平台或内部运维系统
- 为动态表单或自动化流程暴露连接器元数据
- 用基础认证或 HTTPS 保护运维接口

## REST API 与 Web UI 的关系

REST API 和 Web UI 不是两套独立系统，而是构建在同一个 HTTP 能力之上的两种访问方式：

- REST API 面向脚本、自动化和系统集成
- Web UI 面向人工巡检、排障和日常运维

在实际生产环境中，这两者通常会同时使用：

- 外部系统调用 REST 接口
- 运维人员通过 Web UI 查看任务和集群状态

## 开启 HTTP 能力

在使用这两类入口之前，需要先在 `seatunnel.yaml` 中开启 HTTP 服务：

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

通常还需要尽早关注这些配置项：

- `context-path`：给所有 HTTP 接口加统一前缀
- `enable-dynamic-port`：当指定端口被占用时自动探测可用端口
- `enable-https`：开启 HTTPS
- `enable-basic-auth`：使用 HTTP Basic 认证保护接口

完整的 REST 参数说明请查看 [RESTful API V2](./rest-api-v2.md)。如果需要 HTTPS 和认证配置，请继续查看 [安全配置](./security.md)。

## 如何访问 Web UI

HTTP 开启后，可以通过下面的地址访问：

```text
http://<host>:<port>/#/overview
```

Web UI 适合查看：

- 集群总览
- 运行中的作业
- 已完成的作业
- Worker 健康状态与资源使用
- Master 状态

详细界面说明请查看 [Web UI](./web-ui.md)。

## REST API 的典型用途

REST API 常见的使用方式包括：

- 获取连接器 `OptionRule` 元数据，用于动态表单
- 查询集群概览和作业状态
- 将 SeaTunnel Engine 集成到内部运维平台
- 向监控或调度系统暴露运行时状态

最常用的参考文档是：

- [RESTful API V2](./rest-api-v2.md)
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

### 4. 生产环境补齐安全配置

- 如果接口会暴露给更广的内网或外部系统，建议开启 HTTPS 与认证

## 继续阅读

- [RESTful API V2](./rest-api-v2.md)
- [RESTful API V1](./rest-api-v1.md)
- [安全配置](./security.md)
- [Web UI](./web-ui.md)
