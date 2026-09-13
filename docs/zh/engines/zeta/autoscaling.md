# 自动扩缩容推荐

SeaTunnel Engine 可以在 Active Master 上运行 Phase 1 自动扩缩容循环。本阶段会采集调度压力、
Worker CPU、Worker JVM 内存以及固定 Slot 利用率，并通过 REST API 和 OpenMetrics 输出扩缩容推荐。
本阶段不会实际增加或删除 Worker。

自动扩缩容默认关闭。

```yaml
seatunnel:
  engine:
    autoscaler:
      enabled: false
      evaluation-interval-seconds: 30
      metrics-freshness-seconds: 120
      max-future-skew-seconds: 5
      scale-out-stabilization-seconds: 300
      scale-in-stabilization-seconds: 600
      scale-out-cpu-threshold: 0.8
      scale-out-jvm-memory-threshold: 0.8
      scale-in-cpu-threshold: 0.3
      scale-in-jvm-memory-threshold: 0.3
      fixed-slot-scale-out-threshold: 0.8
      fixed-slot-scale-in-threshold: 0.3
      scale-step: 1
      min-workers: 1
      max-workers: 2147483647
      history-size: 20
```

## 决策信号

当出现调度资源短缺信号时，例如 Worker 等待 Slot 或拒绝资源请求，或者 Worker CPU/JVM 内存
在稳定窗口内持续高于扩容阈值时，Autoscaler 可以给出扩容推荐。

Slot pressure 不能单独触发扩容。在固定 Slot 模式下，Slot 利用率只作为辅助的调度容量信号。
在动态 Slot 模式下，Slot 利用率会报告为 `UNKNOWN`。

缩容判断更保守。CPU 和 JVM 内存必须同时低于配置的缩容阈值，Worker 指标必须新鲜且完整；
固定 Slot 模式还必须满足低 Slot 利用率阈值。

## REST API

以下接口会从 Active Master 返回推荐状态：

| Endpoint | 说明 |
| --- | --- |
| `/autoscaler/status` | 当前 autoscaler 视图，包括 enabled/running 状态、最新推荐、当前快照和有限历史 |
| `/autoscaler/metrics` | 最新评估使用的当前指标快照 |
| `/autoscaler/history` | 最近的推荐历史 |

## OpenMetrics

启用 engine metrics 后，`/openmetrics` 会包含以下 autoscaler 指标：

- `seatunnel_autoscaler_enabled`
- `seatunnel_autoscaler_running`
- `seatunnel_autoscaler_current_workers`
- `seatunnel_autoscaler_recommended_workers`
- `seatunnel_autoscaler_recommended_delta`
- `seatunnel_autoscaler_recommendations_total`
- `seatunnel_autoscaler_metrics_valid`
- `seatunnel_autoscaler_input_cpu_utilization`
- `seatunnel_autoscaler_input_jvm_memory_utilization`
- `seatunnel_autoscaler_input_slot_utilization`
- `seatunnel_autoscaler_worker_samples`
- `seatunnel_autoscaler_resource_shortages_total`
- `seatunnel_autoscaler_stabilization_seconds`
- `seatunnel_autoscaler_info`
