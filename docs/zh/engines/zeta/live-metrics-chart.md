# 实时指标图（Live Metrics Chart）

## 状态

本页是 [issue #11666](https://github.com/apache/seatunnel/issues/11666) 的设计约定。第一版应把 Job Detail 中已有的实时时序数据做成可选的实时图表，而不是新增指标管线，也不是照搬 Flink Metrics 页面布局。

设计边界：

- 复用现有 Job Detail Overview 与详情抽屉
- 复用 active master 内存中的实时指标（`/metrics/realtime/jobs/{jobId}/vertices|edges`）
- 对齐 Flink「挑选指标并持续观察」的能力，而不是对齐 Flink 的页面结构
- 第一版不做长期历史存储、告警、自定义派生表达式

## 问题

SeaTunnel 已通过 `windowMs` 返回窗口化时序，Job Detail 抽屉也已经用表格展示这些点。运维仍无法：

- 一眼看到趋势，而不是逐行读表
- 关闭节点/边抽屉后继续保留一条或多条曲线
- 并排对比多个 vertex/edge 的指标

## 现有基础

| 领域 | 现有契约 |
|---|---|
| Vertex 时序 | `GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=` |
| Edge 时序 | `GET /metrics/realtime/jobs/{jobId}/edges?windowMs=` |
| UI 轮询 | Job Detail Overview 在作业运行时每 2 秒轮询上述接口 |
| 窗口 | 默认 3 分钟，最大 10 分钟 |
| 抽屉 | 点击节点或边打开详情抽屉，展示关键字段与原始时序表 |

## 目标

1. 在抽屉中把实时时序渲染为折线图（可保留或替代原始表格）。
2. 支持 pin 一条或多条数值指标，关闭抽屉后图表仍可见。
3. 支持在同一张图上对比来自多个 vertex/edge 的 pin 指标。
4. 提供可被 [#11351](https://github.com/apache/seatunnel/issues/11351)、[#11352](https://github.com/apache/seatunnel/issues/11352) 复用的共享图表组件契约。
5. 无论 pin 多少条 series，客户端请求成本保持有界。

## 非目标

- 长期历史指标存储或导出
- 告警或阈值通知
- 任意自定义/派生指标表达式
- 把 Job Detail 改造成 Flink 风格 Metrics 页签布局
- 为每条 pin 指标增加额外轮询

## 交互落点

V1 仍落在当前 Job Detail Overview 布局：

```text
Overview
├─ DAG
├─ Pin 实时指标图面板   ← 新增
└─ 现有汇总指标表
```

- **抽屉**：分隔线上方的关键字段摘要保留；只改分隔线下方的时序表格为实时折线，并提供 pin 操作。不重做整个抽屉。
- **Pin 面板**：放在 DAG 与 Overview 现有汇总表之间，关闭抽屉后仍可继续观察。
- **布局**：保持 SeaTunnel Web UI 现有结构与组件风格。不把独立 Metrics 页签或 Flink 风格卡片网格作为硬性要求。

Pin 的含义是「把该指标保留在 Overview 图表面板中」，不是要求复制 Flink 外观。

## 共享图表组件契约

图表组件不负责发请求，由页面注入已拉取的 series。

```text
series item:
- id: 稳定 id，例如 vertex:12:sourceReadRatio
- name: 展示名
- points: [{ ts: number, value: number }, ...]

chart props:
- series: series item[]
- windowMs: number
- emptyText?: string
```

行为：

- 在既有有界窗口内为每条 series 画折线
- 按 `ts` 升序排序
- 容忍刷新间隔短于采集间隔带来的重复 bucket
- 无数据时展示空态
- 不自行请求指标

### 图表库

V1 在 `seatunnel-engine-ui` 中使用 **Apache ECharts**。

| 项 | 决定 |
|---|---|
| 库 | Apache ECharts |
| 协议 | Apache License 2.0（ASF 项目） |
| 范围 | 只负责渲染；轮询与 series 映射仍由页面完成 |

实现 PR 必须说明新增 ECharts npm 依赖，且不得再并行引入第二套图表库。

## Pin 模型

Pin 作用域为当前 Job Detail 会话：

| 事件 | 行为 |
|---|---|
| 从抽屉 pin | 加入 Overview pin 面板 |
| 关闭抽屉 | 保留已 pin series |
| 在同一作业页切换 Overview / Exception / Configuration / Log | 保留已 pin series |
| 离开 Job Detail | 清空 |
| 作业进入终态 | 清空 |
| 超过 pin 上限 | 拒绝新增并给出简短提示 |

默认 pin 上限：**6** 条 series。

已 pin series 只消费 Overview 已在轮询的同一份 realtime 响应，不得额外增加 REST 流量。

## 刷新、保留与成本

V1 保持现有刷新与保留模型：

- Overview 打开且作业运行时，每个作业每 2 秒轮询 vertices/edges 各一次
- 默认查询窗口 3 分钟，最大 10 分钟
- 图表数据不落盘
- 多用户各自轮询，但单个客户端无论 pin 多少条，每个轮询周期仍是这两次请求

因此实时图成本与现有 realtime observability 成正比。

## 验收标准

- 在 Job Detail 中至少能 pin 一条指标，并在不保持抽屉打开的情况下看到实时更新的图。
- 可在同一张图上对比多个指标或 vertex。
- 文档写明 pin 上限与共享轮询，保证内存与请求成本有界。
- 同步更新中英文文档。

## 相关链接

- Issue: [#11666](https://github.com/apache/seatunnel/issues/11666)
- Umbrella: [#11668](https://github.com/apache/seatunnel/issues/11668)
- Related: [#11351](https://github.com/apache/seatunnel/issues/11351), [#11352](https://github.com/apache/seatunnel/issues/11352)
- 现有文档：[实时可观测性](realtime-observability.md)、[Web UI](web-ui.md)
