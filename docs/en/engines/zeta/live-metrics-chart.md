# Live Metrics Chart

## Status

This page is the proposed design contract for [issue #11666](https://github.com/apache/seatunnel/issues/11666). The first delivery should turn the existing Job Detail realtime time-series into a selectable live chart without adding another metrics pipeline or copying the Flink Metrics page layout.

The design is intentionally bounded:

- reuse the existing Job Detail Overview page and detail drawer
- reuse active-master in-memory realtime metrics (`/metrics/realtime/jobs/{jobId}/vertices|edges`)
- align capability with Flink-style "pick metrics and keep watching", not Flink page structure
- avoid persistent metric history, alerting, and custom derived expressions in the first version

## Problem

SeaTunnel already returns windowed realtime series through `windowMs`, and the Job Detail drawer already shows those points as a table. Operators still cannot:

- see trend at a glance instead of reading rows
- keep one or more series visible after closing the vertex or edge drawer
- compare metrics from more than one vertex or edge side by side

## Existing Foundation

| Area | Existing contract |
|---|---|
| Vertex series | `GET /metrics/realtime/jobs/{jobId}/vertices?windowMs=` |
| Edge series | `GET /metrics/realtime/jobs/{jobId}/edges?windowMs=` |
| UI polling | Job Detail Overview already polls both endpoints every 2 seconds while the job is running |
| Window | Default 3 minutes, capped at 10 minutes |
| Drawer | Clicking a vertex or edge opens the existing detail drawer with key fields and a raw series table |

## Goals

1. Render realtime series as a line chart in addition to, or instead of, the raw table in the drawer.
2. Let users pin one or more numeric metrics so the chart remains visible after the drawer closes.
3. Allow comparing pinned metrics from multiple vertices or edges on one chart.
4. Expose a shared chart component contract that later features such as [#11351](https://github.com/apache/seatunnel/issues/11351) and [#11352](https://github.com/apache/seatunnel/issues/11352) can reuse.
5. Keep client cost bounded regardless of how many series a user pins.

## Non-Goals

- long-term historical metrics storage or export
- alerting or threshold notifications
- arbitrary custom or derived metric expressions
- redesigning Job Detail into a Flink-like Metrics tab layout
- adding a second polling loop per pinned metric

## UX Placement

V1 stays on the current Job Detail Overview layout:

```text
Overview
├─ DAG
├─ Pinned live metrics chart panel   ← new
└─ Existing summary metrics table
```

- **Drawer**: keep the existing key-field summary above the divider. Replace or augment only the bottom series table with a live line chart and pin controls. Do not redesign the whole drawer.
- **Pinned panel**: place it between the DAG and the existing Overview summary table so operators can keep watching after the drawer closes.
- **Layout**: keep SeaTunnel Web UI structure and components. Do not introduce a separate Metrics tab or Flink-style card grid as a hard requirement.

Pin means "keep this metric visible in the Overview chart panel", not a requirement to copy Flink chrome.

## Shared Chart Component Contract

The chart component must be request-agnostic. Pages inject already-fetched series.

```text
Input series item:
- id: stable series id, for example vertex:12:sourceReadRatio
- name: display label
- points: [{ ts: number, value: number }, ...]

Input chart props:
- series: series item[]
- windowMs: number
- emptyText?: string
```

Behavior:

- draw one line per series over the existing bounded window
- sort points by ascending `ts`
- tolerate repeated buckets from a refresh interval shorter than the collector interval
- show an empty state when there are no points
- do not fetch metrics itself

### Chart library

V1 uses **Apache ECharts** in `seatunnel-engine-ui` only.

| Item | Decision |
|---|---|
| Library | Apache ECharts |
| License | Apache License 2.0 (ASF project) |
| Scope | Chart rendering only; pages still own polling and series mapping |

The implementation PR must call out the ECharts npm dependency and must not add a second chart library.

## Pin Model

Pinned series are session-scoped to the current Job Detail visit:

| Event | Behavior |
|---|---|
| Pin from drawer | Add series to the Overview pinned panel |
| Close drawer | Keep pinned series |
| Switch Overview / Exception / Configuration / Log | Keep pinned series while staying on the same job page |
| Leave Job Detail | Clear pinned series |
| Job reaches a terminal state | Clear pinned series |
| Exceed pin limit | Reject the new pin and show a short message |

Default pin limit: **6** series.

Pinned series consume the same Overview realtime response already polled for the DAG. Pinning must not create additional REST traffic.

## Refresh, Retention, and Cost

V1 keeps the existing refresh and retention model:

- one job-level poll for vertices and edges every 2 seconds while Overview is open and the job is running
- default query window 3 minutes, maximum 10 minutes
- no chart data written to disk
- concurrent browser users each poll independently, but each client still issues the same two requests per poll interval regardless of pin count

This keeps live-chart cost proportional to today's realtime observability feature.

## Acceptance Criteria

- From Job Detail, a user can pin at least one metric and see it as a live-updating chart without keeping the drawer open.
- Multiple metrics or vertices can be compared on the same chart.
- Documented pin limit and shared polling keep memory and request cost bounded.
- English and Chinese docs describe the behavior.

## Related

- Issue: [#11666](https://github.com/apache/seatunnel/issues/11666)
- Umbrella: [#11668](https://github.com/apache/seatunnel/issues/11668)
- Related: [#11351](https://github.com/apache/seatunnel/issues/11351), [#11352](https://github.com/apache/seatunnel/issues/11352)
- Existing docs: [Realtime Observability](realtime-observability.md), [Web UI](web-ui.md)
