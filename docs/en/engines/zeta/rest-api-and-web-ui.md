---
sidebar_position: 1
---

# REST API And Web UI

SeaTunnel Engine exposes two operational interfaces on top of the same HTTP service:

- a **REST API** for programmatic integration, automation, and inspection
- a **Web UI** for visual monitoring of jobs, workers, and master status

This page is the entry point for operators. Use it to understand when to enable HTTP access, how the REST API and Web UI relate to each other, and where to find the detailed references.

## When To Use This Page

Read this page if you need to:

- monitor running and finished jobs without using the CLI
- integrate SeaTunnel Engine with an external platform or internal tooling
- expose connector metadata to a UI or automation workflow
- secure operational endpoints with basic authentication or HTTPS

## How The Two Interfaces Fit Together

The REST API and Web UI are not separate products. They are two interfaces built on the same HTTP capability of SeaTunnel Engine:

- the REST API is designed for scripts, automation, and system integration
- the Web UI is designed for visual inspection and day-to-day operations

In practice, many production environments use both:

- external systems call REST endpoints
- operators troubleshoot and observe jobs through the Web UI

## Enable HTTP Access

Before using either interface, enable the HTTP service in `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

Optional settings you may care about early:

- `context-path`: prefix all HTTP endpoints under a custom path
- `enable-dynamic-port`: scan for an available port when the configured one is occupied
- `enable-https`: expose HTTPS instead of plain HTTP
- `enable-basic-auth`: protect the endpoints with HTTP Basic authentication

For the full REST parameter details, see [RESTful API V2](./rest-api-v2.md). For HTTPS and authentication, see [Security](./security.md).

## Access The Web UI

Once HTTP is enabled, open:

```text
http://<host>:<port>/#/overview
```

The Web UI helps you inspect:

- cluster overview
- running jobs
- finished jobs
- worker health and resource usage
- master status

See the detailed walkthrough in [Web UI](./web-ui.md).

## Common REST API Scenarios

The REST API is commonly used for:

- retrieving connector `OptionRule` metadata for dynamic forms
- fetching cluster overview and job status
- integrating SeaTunnel Engine into an internal operations portal
- exposing runtime state to monitoring or orchestration layers

The most commonly referenced pages are:

- [RESTful API V2](./rest-api-v2.md)
- [RESTful API V1](./rest-api-v1.md)

If you are building new integrations, prefer **V2** unless you have to maintain compatibility with an older client.

## Typical Operational Workflow

### 1. Enable HTTP

- configure `seatunnel.engine.http` in `seatunnel.yaml`
- decide whether you need a context path, dynamic ports, HTTPS, or basic authentication

### 2. Verify REST access

- query overview and running jobs endpoints
- confirm that the service is reachable from your operational environment

### 3. Open the Web UI

- use the UI to verify cluster health and inspect job details

### 4. Lock down production access

- enable HTTPS and authentication when exposing the endpoints beyond a trusted internal network

## Related Pages

- [RESTful API V2](./rest-api-v2.md)
- [RESTful API V1](./rest-api-v1.md)
- [Security](./security.md)
- [Web UI](./web-ui.md)
