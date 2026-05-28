---
sidebar_position: 1
---

# Getting Started Overview

This page is the fastest way to understand how to approach SeaTunnel for the first time. Use it to choose an engine, find the right quick start, and follow a short reading path instead of jumping between unrelated pages.

## What SeaTunnel Helps You Do

SeaTunnel is a distributed data integration platform built for moving data between heterogeneous systems with a unified connector model. In practice, teams use it for:

- batch synchronization between databases, files, and warehouses
- CDC and real-time synchronization
- multi-table or full-database migration
- multimodal ingestion for structured, unstructured, and binary data

If you are evaluating SeaTunnel for the first time, start with the built-in **SeaTunnel Engine (Zeta)**. It has the shortest setup path and is the default engine for new deployments.

## Choose An Engine

| Engine | Best for | Start here |
| --- | --- | --- |
| SeaTunnel Engine (Zeta) | New projects, CDC, low-resource environments, quick local validation | [Quick Start With SeaTunnel Engine](./locally/quick-start-seatunnel-engine.md) |
| Flink | Teams that already operate Flink clusters | [Quick Start With Flink](./locally/quick-start-flink.md) |
| Spark | Teams that already operate Spark clusters | [Quick Start With Spark](./locally/quick-start-spark.md) |

For a broader comparison, see [Engine Overview](../engines/overview.md).

## Recommended First Run

If you want to validate your installation in the shortest path:

1. Read [Deployment](./locally/deployment.md) and install the binary package.
2. Install the sample plugins needed for the first job.
3. Run the local SeaTunnel Engine quick start with `FakeSource -> FieldMapper -> Console`.
4. After the sample succeeds, replace the demo source and sink with real connectors.

## Recommended Reading Path

### Path A: I just want to run my first job

- [Deployment](./locally/deployment.md)
- [Quick Start With SeaTunnel Engine](./locally/quick-start-seatunnel-engine.md)
- [Job Configuration Guide](./job-configuration-guide.md)

### Path B: I already know the job I want to build

- [Job Configuration Guide](./job-configuration-guide.md)
- [Source Connectors](../connectors/source)
- [Sink Connectors](../connectors/sink)
- [Transforms](../transforms)

### Path C: I need to understand architecture first

- [About SeaTunnel](../introduction/about.md)
- [How It Works](../introduction/how-it-works.md)
- [Architecture Overview](../architecture/overview.md)

## What You Need Before Using SeaTunnel

- Java 8 or Java 11 with `JAVA_HOME` configured
- A SeaTunnel binary package from the [download page](https://seatunnel.apache.org/download)
- Required connector plugins installed under `${SEATUNNEL_HOME}/connectors/`
- Any third-party driver jars required by your chosen connectors

If you only want to run the sample job, the required plugins are usually `connector-fake` and `connector-console`.

## After The First Successful Run

Once the sample job is working, the next step is usually one of these:

- replace `FakeSource` and `Console` with real connectors
- switch from local validation to a cluster deployment
- expose REST API and Web UI for operational visibility

Use these pages next:

- [Job Configuration Guide](./job-configuration-guide.md)
- [SeaTunnel Engine(Zeta) Deployment](../engines/zeta/deployment.md)
- [REST API And Web UI](../engines/zeta/rest-api-and-web-ui.md)
