# Live Metrics Chart

On the Job Detail Overview page, operators can pin numeric realtime metrics and watch them as a live line chart without keeping the vertex or edge drawer open.

This page describes the shipped Job Detail behavior. It reuses the existing realtime metrics endpoints and Overview poll. It does not add a metrics pipeline, a Metrics tab, or long-term history.

## Where it appears

```text
Overview
├─ DAG
├─ Pinned live metrics chart panel
└─ Existing summary metrics table
```

- Click a vertex or edge on the DAG to open the detail drawer.
- The drawer keeps the key-field summary above the divider.
- Below the divider, the previous series table is a live line chart with Pin / Unpin controls.
- Pinned series stay on the Overview panel after the drawer closes.
- Charts are split by unit so mixed metrics stay readable: **ratio** (0–100%), **duration** (ms/record), and **records**. Same-unit series overlay on one chart; Overview places different units in one compact row (up to three columns). Legend labels use short operator ids such as `Source[0]`.

## Pin behavior

Pins are scoped to the current Job Detail visit. They are not written to `localStorage`.

| Event | Behavior |
|---|---|
| Pin from the drawer | Add the series to the Overview pinned panel |
| Close the drawer | Keep pinned series |
| Switch Overview / Exception / Configuration / Log on the same job page | Keep pinned series |
| Leave Job Detail | Clear pinned series |
| Job reaches a terminal state | Clear pinned series |
| Exceed the pin limit | Reject the new pin and show a short message |

Default pin limit: **6** series.

## Refresh, window, and cost

Pinned series consume the same Overview realtime response already used by the DAG:

- `GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=`
- `GET /metrics/realtime/jobs/{jobId}/edges?windowMs=`

While Overview is open and the job is running, the page polls both endpoints every 2 seconds. The default window is 3 minutes, capped at 10 minutes. Pinning does not create extra REST traffic: one client still issues the same two requests per poll interval regardless of pin count.

No chart data is written to disk.

## Shared fetch and chart contract

Follow-up observability views such as the runtime graph ([#11351](https://github.com/apache/seatunnel/issues/11351)) and backpressure diagnosis ([#11352](https://github.com/apache/seatunnel/issues/11352)) should reuse these building blocks instead of adding another poll or chart library.

**Reuse**

| Piece | Location | Contract |
|---|---|---|
| Job-level fetch | `fetchJobRealtimeMetrics` in `seatunnel-engine-ui` | Loads vertices and edges for one `windowMs`. Callers own the poll loop. The helper does not start a timer. |
| Live chart | `LiveLineChart` / `LiveMetricsBoard` | Request-agnostic. Pages inject already-fetched series: `{ id, name, unit?, points: [{ ts, value }] }`, plus `windowMs` and optional `emptyText`. The component does not fetch. |

Same-unit series overlay on one chart. Mixed units (ratio, duration, records) are split so scales stay readable.

**Do not reuse** from this feature: the Job Detail pin store, the six-series pin limit, or the Overview pin-panel layout. Those are session UX for this page only.

ECharts is a rendering dependency of `seatunnel-engine-ui` only. Do not add a second chart library.

## Limitations

The first version does not include:

- long-term historical metrics storage or export
- alerting or threshold notifications
- custom or derived metric expressions
- a separate Flink-style Metrics tab

For connector and engine metric semantics, see [Realtime Observability](realtime-observability.md). For the Job Detail screens, see [Web UI](web-ui.md).
