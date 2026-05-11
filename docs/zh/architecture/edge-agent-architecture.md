---
title: Edge Agent 架构
---

# Edge Agent 架构

## 1. 概览

### 1.1 问题背景

在很多生产网络中，Zeta 集群无法直接访问边缘节点上的本地数据（主机文件、内网应用日志、本地事件通道）。因此需要一个专门的边缘采集进程，用于：

- 在数据源附近读取本地记录，
- 在网络波动时保持可恢复投递能力，
- 在不把 engine worker 部署到边缘节点的前提下，把数据送入运行中的 SeaTunnel 作业。

### 1.2 设计目标

SeaTunnel Edge Agent（Phase 1）遵循以下目标：

1. **独立部署**：在安装根目录下提供 `bin/`、`conf/`、`lib/` 布局的运行时与打包产物（见下文「打包与运维」），与引擎 worker 生命周期解耦。
2. **出站持久化**：基于 SQLite WAL 队列，具备明确的状态流转。
3. **协议与 Zeta 对齐**：复用 EdgeSocket 行协议与 commit 轮询语义。
4. **运维简单**：YAML 配置、start/stop/status 脚本、可预测主循环。
5. **可演进**：为后续并行发送、更强一致性语义预留边界。

### 1.3 架构定位对比

| 维度 | Edge Agent | 引擎内 Source |
|------|------------|---------------|
| 运行位置 | 边缘主机独立进程 | Zeta worker task |
| 输入访问 | 边缘本地文件/日志/事件 | worker 可达的数据源 |
| 出站耐久 | agent 本地 SQLite WAL | 引擎 checkpoint/状态机制 |
| 传输方式 | EdgeSocket TCP 行协议 | task 内部数据流 |
| 主要职责 | 边缘采集 + 转发 | 作业执行 |

## 2. 整体架构

### 2.1 逻辑拓扑

```
┌───────────────────────────────────────────────────────────────┐
│                          边缘主机                             │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                 Edge Agent 进程                        │  │
│  │  • AgentInput (file/log/event)                         │  │
│  │  • RecordBatchAccumulator                              │  │
│  │  • SqliteOutboundWal (PENDING/SENDING/ACKED)          │  │
│  │  • EdgeTransportClient                                 │  │
│  └─────────────────────────────────────────────────────────┘  │
└───────────────────────────────────────────────────────────────┘
                               │
                               │ (TCP 行协议)
                               ▼
┌───────────────────────────────────────────────────────────────┐
│                   SeaTunnel Engine (Zeta)                    │
│                                                               │
│  SeaTunnelClient 地址发现 (jobId -> task-group hosts)         │
│               +                                               │
│  EdgeSocket Source 入口 (__AUTH__/__BATCH__/__COMMIT__)      │
└───────────────────────────────────────────────────────────────┘
```

### 2.2 模块架构

| 模块 | Artifact | 职责 |
|------|----------|------|
| `seatunnel-edge-agent` | `seatunnel-edge-agent` (`pom`) | 父模块聚合与分发入口 |
| `seatunnel-edge-agent-core` | `seatunnel-edge-agent-core` | 启动、配置、主循环、WAL 生命周期 |
| `seatunnel-edge-agent-transport` | `seatunnel-edge-agent-transport` | EdgeSocket 客户端、发现与重连策略 |
| `seatunnel-edge-agent-connector` | `seatunnel-edge-agent-connector` | `AgentInput` 内置实现与 NDJSON 规范化 |
| `seatunnel-dist`（标准 assemblies） | `seatunnel-dist` (`pom`) | 通过 `assembly-bin.xml` / `assembly-bin-ci.xml` / `assembly-src.xml` 打包模块级 `bin/`、`conf` 与 jar |

## 3. 运行时执行模型

### 3.1 启动流程

```mermaid
sequenceDiagram
    participant Main as EdgeAgentMain
    participant Boot as EdgeAgentBootstrap
    participant WAL as SqliteOutboundWal
    participant ST as SeaTunnelClient
    participant ET as EdgeTransportClient

    Main->>Boot: 解析配置路径 + start()
    Boot->>WAL: open(); recoverStaleSending()
    Boot->>ST: 创建 SeaTunnelClient (cluster-name/addresses)
    Boot->>ET: open() (发现 + 认证)
    Boot->>Boot: 打开全部 AgentInput
    Boot->>Boot: 进入主循环
```

### 3.2 主循环

每轮循环执行：

1. 按 `queue.poll-batch-size` 从各输入轮询数据。
2. 在内存聚合，满足 `bulk-max-size` 或 `flush-interval-ms` 即触发 flush。
3. flush 后写入 WAL（状态 `PENDING`）。
4. 从 WAL 领取发送切片（`PENDING -> SENDING`）。
5. 合并 NDJSON 批次，发送并等待 commit ACK。
6. 成功则置 `ACKED`；失败则回滚 `PENDING` 且递增 `attempts`。

### 3.3 WAL 行状态机

```
PENDING
  │ claimSendingBatch()
  ▼
SENDING
  │ send + commit ACK success
  ├──────────────► ACKED
  │
  └ 发送失败 / 超时 / 重启恢复
                 ▼
               PENDING (attempts + 1)
```

## 4. 发现与协议模型

### 4.1 地址发现

`EdgeTransportClient` 不写死 worker 地址。它通过 `JobTaskGroupAddressesLookup` 调用 `SeaTunnelClient.getJobTaskGroupAddresses(jobId)`，解析 host 列表，再与 `output.port` 组合为 EdgeSocket 入口地址集合。

### 4.2 线路协议

| 阶段 | Agent -> Engine | Engine -> Agent |
|------|------------------|-----------------|
| 认证 | `__AUTH__:<token>` | `ACK` / `AUTH_FAILED` |
| 批次发送 | `__BATCH__:<batchId>:<payload>` | `RECEIVED` / `RETRY` |
| 提交轮询 | `__COMMIT__:<batchId>` | `PENDING` / `RETRY` / `ACK:<n>` |

ACK 完成条件：`n >= batchId`。

### 4.3 重连与重发现策略

I/O 或认证失败时：

1. 失效当前 socket 会话，
2. 重新发现 task-group 地址，
3. 轮换候选 endpoint，
4. 在有限循环内按退避策略重连并重新认证。

## 5. 耐久与故障处理

### 5.1 WAL 保障

- Agent 侧耐久队列存储在 SQLite（`outbound_records`）。
- `recoverStaleSending()` 负责处理领取后崩溃窗口。
- `retry.max-attempts` 防止无限重放。

### 5.2 典型故障场景

| 故障 | 行为 |
|------|------|
| Agent 进程崩溃 | 启动时 `SENDING` 复位为 `PENDING` |
| 短时网络中断 | 发送失败回滚 + 退避 + 重连/重发现 |
| Worker 地址漂移 | 下一轮重发现刷新 endpoint |
| 优雅停机 | 关闭前将 accumulator 刷入 WAL |

### 5.3 Phase 1 投递语义

Phase 1 语义由 ACK 边界限定：

- 只有收到覆盖该批次的 `ACK:<batchId>`，对应行才退出可重试集合；
- 在崩溃恢复与重试窗口内仍可能重复投递；
- 若下游要求强去重，建议依赖幂等键或业务去重策略。

## 6. 配置与输入模型（Phase 1）

### 6.1 `agent.yaml` 配置面

- `inputs`：有序输入定义（`file`/`log`/`event` + 逻辑 id）。
- `output`：集群引导参数（`cluster-name`、`cluster-addresses`）+ 投递身份参数（`job-id`、`auth-token`、`port`）+ 超时。
- `queue`：SQLite 路径与 poll 上限。
- `batch`：内存聚批阈值。
- `retry`：WAL 发送失败时的重试预算与退避。

示例见：[`conf/agent.yaml`](../../../seatunnel-edge-agent/conf/agent.yaml)。

### 6.2 输入类型行为

| 类型 | 行为 |
|------|------|
| `file` | 顺序读取配置文件中的非空 NDJSON 行 |
| `log` | 单文件 tail（或从头读取） |
| `event` | 文件预加载模式或内存注入模式（paths 为空） |

## 7. 打包与运维

### 7.1 安装根目录与路径约定

在源码仓库中，启动脚本与示例配置维护在 **`seatunnel-edge-agent/bin`** 与 **`seatunnel-edge-agent/conf`**。`seatunnel-dist` 的标准 `assembly-bin*.xml` 会将它们打进分发包。

文中诸如 `bin/seatunnel-edge-agent.sh`、`conf/agent.yaml` 的路径均相对于 **安装根目录**：运行脚本时，`bin/`、`conf/`、`lib/` 的公共父目录——通常是解压 `apache-seatunnel-edge-agent-*-bin.tar.gz` 后的顶层目录，或在开发时自行把 `bin/`、`conf/` 与构建产物 `lib/` 并排放置的目录。**不要**与 SeaTunnel 引擎在仓库根目录下的 `bin/`、`config/` 混淆。

### 7.2 构建产物

- 子模块 jar：`core`、`transport`、`connector`。
- `seatunnel-dist` 的 `assembly-bin*.xml` 产出二进制包，目录为：
  - `bin/`
  - `conf/`
  - `lib/`

### 7.3 脚本生命周期管理

- Unix: `bin/seatunnel-edge-agent.sh start|stop|status`
- Windows: `bin/seatunnel-edge-agent.cmd start|stop|status`

脚本默认相对于安装根目录（即 `bin/` 的父目录）解析路径，并支持通过环境变量覆盖配置路径、PID 文件、日志路径。

若不通过脚本而直接运行 `EdgeAgentMain`（例如 IDE），默认按 JVM **工作目录** 查找 `./conf/agent.yaml`；请统一工作目录或使用 `--config` / `EDGE_AGENT_CONFIG`。

## 8. 相关文档

- [架构总览](./overview.md)
- [Engine 架构](./engine/engine-architecture.md)
- [Checkpoint 机制](./fault-tolerance/checkpoint-mechanism.md)
