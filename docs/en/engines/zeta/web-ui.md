# Web UI

## Start Here

Use [REST API and Web UI](./rest-api-and-web-ui.md) as the main operations entry point. That page explains when to enable the HTTP service, which REST API pages to read next, and how Web UI fits into day-to-day operations.

This page focuses on the Web UI screens themselves and the current capability boundary of the built-in console.

## Access

Before accessing Web UI, enable the SeaTunnel Engine HTTP service in `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

Then visit:

```text
http://<host>:8080/#/overview
```

If `context-path` is configured, include it before the hash route:

```text
http://<host>:8080/<context-path>/#/overview
```

## Overview

The Web UI of Apache SeaTunnel is a visual inspection console for SeaTunnel Engine. It helps operators view cluster overview data, running and finished jobs, job detail pages, logs, realtime DAG metrics, and the status of worker and master nodes.

The Web UI does not submit jobs or provide lifecycle control actions such as cancel, stop, savepoint, or restore. Use the REST API or CLI when you need those operations.
![overview.png](../../../images/ui/overview.png)

## Capability Summary

| UI area | Current capability |
|---------|--------------------|
| Overview | View cluster version, slot usage, worker count, and job counts |
| Jobs | View running and finished jobs, paginate job lists, and open job details |
| Job Detail | View DAG, job metrics, exception text, job configuration, logs, and realtime observability data |
| Workers | View worker-node system monitoring information |
| Master | View master-node system monitoring information |

## Jobs

### Running Jobs

The "Running Jobs" section lists SeaTunnel jobs that are currently in execution. Users can view job ID, job name, creation time, status, and open a detail page for a specific job.

The list refreshes periodically and supports pagination.

![running.png](../../../images/ui/running.png)
![detail.png](../../../images/ui/detail.png)

### Job Detail

The Job Detail page contains four main tabs:

- **Overview**: shows the job DAG, source and sink throughput metrics, flush-signal metrics, and realtime vertex or edge metrics when observability is enabled.
- **Exception**: shows the job error message when the job has failed or reported an exception.
- **Configuration**: shows the runtime job configuration exposed by the engine.
- **Log**: shows job log files returned by the engine log API.

#### Realtime Observability

On the Job Detail page, the DAG view can display realtime metrics for the recent window (3 minutes by default, up to 10 minutes):

- **Vertex busyness**: busy and idle ratios for Source, Transform, and Sink vertices.
- **Edge downstream wait ratio**: when the job inserts queues at async boundaries or before Sink IO, edges are colored and thickened by downstream wait ratio and queue fill ratio.
- **Interaction**: click a vertex or edge to open the detail drawer and view realtime curves and key fields.
- **Pinned live chart**: pin one or more numeric metrics from the drawer so live charts remain visible on Overview after the drawer closes. Series are split by unit (ratio, duration, records) so mixed scales stay readable; same-unit metrics overlay for comparison. See [Live Metrics Chart](live-metrics-chart.md) for pin lifecycle, the 6-series limit, and shared polling cost.

This capability requires the job to enable `env.engine.observability` or configure an option that auto-enables it, such as `async_boundaries` or `split_sink_io`. See [Realtime Observability](realtime-observability.md) for configuration and metric semantics.

For the runtime graph design boundary and large-DAG fallback rules, see [Runtime Execution Graph](runtime-execution-graph.md).

### Finished Jobs

The "Finished Jobs" section displays jobs that have reached a terminal state, such as finished, failed, cancelled, or savepoint done. Users can review historical records and open the detail page to inspect configuration, exception text, metrics retained by the engine, and logs.

![finished.png](../../../images/ui/finished.png)

## Workers

### Workers Information

The "Workers" section displays system monitoring information for worker nodes. Use it to inspect worker address, resource status, and runtime health signals exposed by the engine.

![workers.png](../../../images/ui/workers.png)

## Master

### Master Information

The "Master" section displays system monitoring information for master nodes. Use it to inspect the current master-side runtime state and resource signals exposed by the engine.

![master.png](../../../images/ui/master.png)

## Next Steps

- [REST API and Web UI](./rest-api-and-web-ui.md)
- [REST API V2](./rest-api-v2.md)
- [Runtime Execution Graph](./runtime-execution-graph.md)
- [Live Metrics Chart](./live-metrics-chart.md)
- [Job Lifecycle API](./rest-api-job-lifecycle.md)
- [Task Failure History Design](./task-failure-history.md)
- [Security](./security.md)
