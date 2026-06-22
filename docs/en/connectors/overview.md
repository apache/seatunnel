---
slug: /connectors
---

# Connectors Overview

This page is the shortest path for choosing the right SeaTunnel connector entry point. Start by answering three questions: where does data come from, where does it go, and do you need CDC or special formats.

## Choose An Entry Point

| What you need right now | Start here |
| --- | --- |
| Read data from an external system | [Source Connectors](./source-overview.md) |
| Write data into a target system | [Sink Connectors](./sink-overview.md) |
| Follow a real source-to-sink example | [Scenario Recipes](../getting-started/recipes/overview.md) |
| Understand shared connector parameters | [Source Common Options](./common-options/source-common-options.md) and [Sink Common Options](./common-options/sink-common-options.md) |
| Build a CDC pipeline | [CDC Production Cookbook](./cdc-production-cookbook.md) |
| Troubleshoot plugin installation or dependency conflicts | [Connector FAQ](./connector-faq.md) and [Connector Isolated Dependency Loading](./connector-isolated-dependency.md) |

## Recommended Reading Order For New Users

1. Run one local job first, then come back to choose real connectors.
2. Pick the source and sink before comparing transform or format details.
3. Check plugin installation and third-party driver requirements before copying connector examples.
4. Read CDC and recovery details only when your pipeline actually needs them.

## What To Verify Before You Commit To A Connector

- whether the connector supports your runtime engine
- whether extra drivers or plugin jars are required
- whether the connector supports batch, streaming, CDC, or exactly-once semantics
- whether the parameter names and examples match your SeaTunnel version

## Useful Next Pages

- [Job Configuration Guide](../getting-started/job-configuration-guide.md)
- [Scenario Recipes](../getting-started/recipes/overview.md)
- [Transforms Overview](../transforms)
- [Quick Start With SeaTunnel Engine](../getting-started/locally/quick-start-seatunnel-engine.md)
