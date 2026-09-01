# 实时指标图（Live Metrics Chart）

在 Job Detail 的 Overview 页，运维可以从节点或边抽屉中 Pin 数值型实时指标，关闭抽屉后仍能在 Overview 上看实时折线，并对比多个 vertex/edge。

本页描述已落地的 Job Detail 行为。它复用现有 realtime metrics 接口和 Overview 轮询，不新增指标管线、Metrics 页签或长期历史存储。

## 出现位置

```text
Overview
├─ DAG
├─ Pin 实时指标图面板
└─ 现有汇总指标表
```

- 点击 DAG 上的节点或边打开详情抽屉。
- 分隔线上方的关键字段摘要保持不变。
- 分隔线下方由原来的时序表改为实时折线图，并提供 Pin / Unpin。
- 关闭抽屉后，已 Pin 的 series 仍留在 Overview 面板。
- 图表按量纲拆开，避免混轴压扁：**占比**（0–100%）、**耗时**（毫秒/条）、**条数**。同量纲叠在一张图上；Overview 上不同量纲并排成一行（最多三列）。图例使用短名，例如 `Source[0]`。

## Pin 行为

Pin 只作用于当前这次 Job Detail 访问，不会写入 `localStorage`。

| 事件 | 行为 |
|---|---|
| 从抽屉 Pin | 加入 Overview pin 面板 |
| 关闭抽屉 | 保留已 Pin series |
| 在同一作业页切换 Overview / Exception / Configuration / Log | 保留已 Pin series |
| 离开 Job Detail | 清空 |
| 作业进入终态 | 清空 |
| 超过 Pin 上限 | 拒绝新增并给出简短提示 |

默认 Pin 上限：**6** 条 series。

## 刷新、窗口与成本

已 Pin series 只消费 Overview 已经在给 DAG 使用的同一份 realtime 响应：

- `GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=`
- `GET /metrics/realtime/jobs/{jobId}/edges?windowMs=`

Overview 打开且作业运行时，页面每 2 秒轮询上述两个接口。默认窗口 3 分钟，最大 10 分钟。Pin 不会增加额外 REST 流量：单个客户端无论 Pin 多少条，每个轮询周期仍是这两次请求。

图表数据不落盘。

## 共享拉取与图表合约

后续可观测页面（例如运行时执行图 [#11351](https://github.com/apache/seatunnel/issues/11351)、反压诊断 [#11352](https://github.com/apache/seatunnel/issues/11352)）应复用下列积木，不要再开一条轮询或第二套图库。

**可复用**

| 积木 | 位置 | 合约 |
|---|---|---|
| Job 级拉取 | `seatunnel-engine-ui` 中的 `fetchJobRealtimeMetrics` | 按一个 `windowMs` 拉取 vertices 与 edges。调用方自己管轮询循环，helper 不启动定时器。 |
| 实时折线 | `LiveLineChart` / `LiveMetricsBoard` | 请求无关。页面注入已取好的 series：`{ id, name, unit?, points: [{ ts, value }] }`，以及 `windowMs` 和可选 `emptyText`。组件不发 HTTP。 |

同量纲叠线；不同量纲（占比、耗时、条数）拆图，避免混轴。

**不要复用**：Job Detail 的 Pin store、6 条上限、Overview pin 面板布局。这些只服务本页的会话交互。

ECharts 只作为 `seatunnel-engine-ui` 的渲染依赖。不要再引入第二套图库。

## 限制

第一版不包含：

- 长期历史指标存储或导出
- 告警或阈值通知
- 自定义或派生指标表达式
- 独立的 Flink 风格 Metrics 页签

指标语义见 [实时可观测性](realtime-observability.md)。Job Detail 页面说明见 [Web UI](web-ui.md)。
