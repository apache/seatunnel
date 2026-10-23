# About SeaTunnel

<img src="https://seatunnel.apache.org/image/logo.png" alt="seatunnel logo" width="200px" height="200px" align="right" />

[![Slack](../../images/seatunnel-slack.svg)](https://s.apache.org/seatunnel-slack)
[![Twitter Follow](../../images/ASFSeaTunnel.svg)](https://x.com/ASFSeaTunnel)

SeaTunnel is a multimodal, high-performance, distributed data integration platform.
It helps teams move and synchronize data across databases, files, data lakes, and streaming systems with one unified job model.

## Start Here

If this is your first time using SeaTunnel, follow this reading path:

<div
  style={{
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
    gap: "16px",
    margin: "16px 0 24px",
  }}>
  <a
    href="../getting-started/locally/run-your-first-job"
    style={{
      display: "block",
      padding: "18px",
      border: "1px solid var(--ifm-color-emphasis-300)",
      borderRadius: "12px",
      textDecoration: "none",
      color: "inherit",
      background: "var(--ifm-background-surface-color)",
      boxShadow: "var(--ifm-global-shadow-lw)",
    }}>
    <strong>Run your first job</strong><br/>
    <span>Start with a local FakeSource -&gt; FieldMapper -&gt; Console pipeline, then continue with practical source-to-sink recipes.</span>
  </a>
</div>

- [Getting Started Overview](../getting-started/overview.md) for the shortest path into the docs
- [Quick Start With SeaTunnel Engine](../getting-started/locally/quick-start-seatunnel-engine.md) for the first local run
- [Job Configuration Guide](../getting-started/job-configuration-guide.md) for writing real jobs
- [How It Works](how-it-works.md) for the execution model before going deeper

If you already operate Flink or Spark clusters, you can also jump directly to
[Quick Start With Flink](../getting-started/locally/quick-start-flink.md) or
[Quick Start With Spark](../getting-started/locally/quick-start-spark.md).

## What SeaTunnel Helps You Do

SeaTunnel is designed for the jobs data teams usually need to deliver first:

- **Move data between many systems**: databases, message queues, file systems, object storage, data lakes, and SaaS systems
- **Handle both batch and streaming workloads**: one connector model can serve full loads, incremental loads, CDC, and real-time synchronization
- **Keep job definitions understandable**: a SeaTunnel job is still mainly `env`, `source`, `transform`, and `sink`
- **Reduce operational cost**: SeaTunnel focuses on high throughput, lower dependency overhead, and practical observability

## Why Teams Choose SeaTunnel

- **Connector-first design**: SeaTunnel provides a unified Connector API, so Source, Transform, and Sink plugins can be reused across engines
- **Flexible engine choice**: start with SeaTunnel Engine (Zeta), or run on Flink or Spark when that better fits your environment
- **Built for data synchronization**: multi-table sync, CDC scenarios, and large-scale job execution are first-class use cases
- **Operational visibility**: jobs expose runtime metrics and task information that help you understand throughput and stability
- **Room to grow**: teams can begin with a single local job and later move to larger clusters and more advanced deployments

## Understand SeaTunnel In One Picture

![SeaTunnel Work Flowchart](../../images/architecture_diagram.png)

You can understand the runtime flow in three ideas:

### 1. A SeaTunnel job is a pipeline

You describe the job in a config file, then SeaTunnel runs a pipeline from **Source** to **Transform** to **Sink**.

### 2. Connectors define what you read and write

SeaTunnel supports a broad set of [source connectors](../connectors/source),
[sink connectors](../connectors/sink), and [transforms](../transforms).
If you need custom behavior, you can also extend these plugin types.

### 3. The engine defines where the job runs

[SeaTunnel Engine (Zeta)](../engines/zeta/about.md) is the default choice and the recommended starting point for most new users.
If you already rely on Flink or Spark, SeaTunnel can submit the same connector-based job model there as well.

## Choose An Engine

| Engine | Best starting point | When to use it |
| --- | --- | --- |
| [SeaTunnel Engine (Zeta)](../engines/zeta/about.md) | Recommended for most new users | You want the simplest path to run SeaTunnel jobs end to end |
| [Apache Flink](../engines/flink.md) | Good for existing Flink users | You already operate Flink and want SeaTunnel to fit that platform |
| [Apache Spark](../engines/spark.md) | Good for existing Spark users | You already run Spark for batch workloads and want to reuse that stack |

## Continue Learning

- [How It Works](how-it-works.md) for the runtime model without the full architecture deep dive
- [Intro To Config File](concepts/config.md) for how to write real jobs
- [Connector documentation](../connectors/source) for choosing the systems you want to read from and write to
- [Architecture Overview](../architecture/overview.md) for the deeper system design
- [FAQ](../faq.md) for common usage, CDC, and configuration questions

## Get Help And Join The Community

- [Developer Setup](../developer/setup.md) if you want to build or debug SeaTunnel locally
- [Contribution Path](../developer/contribution-path.md) if you want to start contributing with the smallest reasonable scope
- [Contribute Plugin](../developer/contribute-plugin.md) if you want to contribute a connector or transform
- [GitHub Issues](https://github.com/apache/seatunnel/issues), [Slack](https://s.apache.org/seatunnel-slack), and the [dev mailing list](https://lists.apache.org/list.html?dev@seatunnel.apache.org) if you need community help

## Who Uses SeaTunnel

SeaTunnel has lots of users. You can find more information about them in [Users](https://seatunnel.apache.org/user).
