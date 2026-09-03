---
sidebar_position: 1
---

# REST API and Web UI

SeaTunnel Engine provides REST API and Web UI as the primary interfaces for remote operations and visual monitoring. This page explains how the two interfaces relate, how to enable the shared HTTP service, and where to find detailed reference pages.

## Which Page Should I Read?

| Need | Go to |
|------|-------|
| Enable the HTTP service, choose a port, or use `context-path` | This page, then [RESTful API V2](./rest-api-v2.md) |
| Build automation, scripts, an operations portal, or a platform integration | [RESTful API V2](./rest-api-v2.md) |
| Submit, stop, cancel, savepoint, or restore jobs through HTTP | [Job Lifecycle API](./rest-api-job-lifecycle.md) |
| Visually inspect cluster health, jobs, DAG metrics, and logs | [Web UI](./web-ui.md) |
| Configure HTTPS or HTTP Basic authentication | [Security](./security.md) |
| Maintain an old Hazelcast REST client | [RESTful API V1](./rest-api-v1.md) |

## How REST API and Web UI Fit Together

REST API and Web UI are not separate services. Both depend on the same SeaTunnel Engine HTTP capability:

- **REST API** is the complete HTTP interface for automation and integration. It covers metadata discovery, job submission, job status, job lifecycle operations, logs, checkpoints, worker information, and realtime metrics.
- **Web UI** is the built-in visual console. It is optimized for human inspection of overview data, running and finished jobs, job details, DAG metrics, logs, worker status, and master status.

Use the Web UI when you need a quick operational view. Use REST API or the command line when you need lifecycle control such as job submission, stop, cancel, savepoint, restore, or batch automation.

## Enable the HTTP Service

Before using either interface, enable the HTTP service in `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
```

Optional settings commonly used in production:

- `context-path`: prefix all HTTP endpoints under a custom path
- `enable-dynamic-port`: scan for an available port when the configured one is occupied
- `enable-https`: expose HTTPS instead of plain HTTP
- `enable-basic-auth`: protect the endpoints with HTTP Basic authentication

For the full REST parameter details, see [RESTful API V2](./rest-api-v2.md). For HTTPS and authentication, see [Security](./security.md).

## Access the Web UI

Once HTTP is enabled, open:

```text
http://<host>:<port>/#/overview
```

If `context-path` is configured, put the UI route under that prefix:

```text
http://<host>:<port>/<context-path>/#/overview
```

## Current Web UI Capabilities

The current Web UI is an inspection console. It provides:

| Area | What you can do |
|------|-----------------|
| Overview | View project version, cluster slots, worker count, and job counts |
| Jobs | Browse running and finished jobs with pagination and open a job detail page |
| Job Detail | Inspect job configuration, DAG, source and sink metrics, job logs, and realtime observability data when enabled |
| Workers | View worker-node system monitoring information |
| Master | View master-node system monitoring information |

See the screen-level walkthrough in [Web UI](./web-ui.md).

## Common REST API Scenarios

The REST API is commonly used for:

- retrieving connector `OptionRule` metadata for dynamic forms
- fetching cluster overview and job status
- integrating SeaTunnel Engine into an internal operations portal
- exposing runtime state to monitoring or orchestration layers

The most commonly referenced pages are:

- [RESTful API V2](./rest-api-v2.md)
- [Job Lifecycle API](./rest-api-job-lifecycle.md)
- [RESTful API V1](./rest-api-v1.md)

If you are building new integrations, prefer **V2** unless you have to maintain compatibility with an older client.

## Typical Operational Workflow

### 1. Enable HTTP

- configure `seatunnel.engine.http` in `seatunnel.yaml`
- decide whether you need a context path, dynamic ports, HTTPS, or basic authentication

### 2. Verify REST access

- query overview and running jobs endpoints
- confirm that the service is reachable from your operational environment

### 3. Open Web UI

- use the UI to verify cluster health and inspect job details

### 4. Use REST API for operational control

- submit, stop, cancel, savepoint, and restore jobs through the lifecycle API when automation is required
- use the Web UI as the visual inspection layer during or after those operations

### 5. Lock down production access

- enable HTTPS and authentication when exposing the endpoints beyond a trusted internal network

## Related Pages

- [RESTful API V2](./rest-api-v2.md)
- [Job Lifecycle API](./rest-api-job-lifecycle.md)
- [RESTful API V1](./rest-api-v1.md)
- [Security](./security.md)
- [Web UI](./web-ui.md)
