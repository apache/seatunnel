# Autoscaling Recommendation

SeaTunnel Engine can run a Phase 1 autoscaling loop on the active master. In this phase the engine
collects scheduling pressure, worker CPU, worker JVM memory, and fixed-slot utilization, then emits
scaling recommendations through REST APIs and OpenMetrics. It does not add or remove workers.

Autoscaling is disabled by default.

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

## Decision Signals

Scale-out can be recommended when scheduler shortage signals are observed, such as workers waiting
for slots or rejecting resource requests, or when worker CPU or JVM memory remains above the
configured scale-out threshold for the stabilization window.

Slot pressure cannot trigger scale-out by itself. In fixed-slot mode, slot utilization is used only
as an auxiliary scheduling-capacity signal. In dynamic-slot mode, slot utilization is reported as
`UNKNOWN`.

Scale-in is conservative. CPU and JVM memory must both stay below the configured scale-in
thresholds, worker metrics must be fresh and complete, and fixed-slot mode must also satisfy the
low slot-utilization threshold.

## REST APIs

The following endpoints return the recommendation state from the active master:

| Endpoint | Description |
| --- | --- |
| `/autoscaler/status` | Current autoscaler view, including enabled/running state, latest recommendation, current snapshot, and bounded history |
| `/autoscaler/metrics` | Current metrics snapshot used by the latest evaluation |
| `/autoscaler/history` | Recent recommendation history |

## OpenMetrics

When engine metrics are enabled, `/openmetrics` includes autoscaler metrics such as:

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
