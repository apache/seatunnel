---
sidebar_position: 4
title: Deployment Guide
---

# Edge Agent Deployment Guide

Install and run Edge Agent on an edge host: one long-lived process per install root that reads local input, buffers outbound data in SQLite WAL, and pushes batches to a configured EdgeSocket endpoint.

There is no Edge Agent cluster manager. Scale out by deploying one agent per edge host (or per data domain on a host), each with its own agent.yaml and WAL path. Engine-side ingestion is separate: run a SeaTunnel job whose source is [EdgeSocket](../connectors/source/EdgeSocket.md) and whose listen address matches output.endpoint.

First time? Complete [Quick Start](quick-start.md) (console, then production setup) before this checklist.

## Before you deploy

- [Download or build](download.md) the package on each edge host
- input.paths and output (transport endpoint and token) match your Engine job — see [Output Configuration](output-configuration.md)
- Network path from the edge host to the EdgeSocket listen address
- After start: BOOTSTRAP_READY in logs; monitor WAL size and RECEIVED / retry — see [Operations](operations.md)

For foreground debugging without bin/seatunnel-edge-agent.sh, see [Operations](operations.md).

## 1. Download

[Download And Build Edge Agent Package](download.md)

## 2. Configure EDGE_AGENT_HOME

Unpack the tarball and point your shell or service unit at the install root:

```shell
export EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent-<version>
export PATH=$PATH:$EDGE_AGENT_HOME/bin
```

Add the same exports to /etc/profile.d/edge-agent.sh if you want them for all users.

## 3. Configure agent.yaml

Default config path: $EDGE_AGENT_HOME/config/agent.yaml. Override with EDGE_AGENT_CONFIG if needed.

Minimal production-oriented example:

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"

output:
  type: transport
  endpoint: "seatunnel-engine-host:9876"
  auth-type: token
  token: "<shared-secret-with-edgesocket-source>"
```

This matches the current default layout: no queue or retry blocks (sqlite-path defaults to data/wal.db). See the [configuration reference](configuration.md) when you need to tune WAL or retry behavior.

## 4. Configure file input

File collection is the most variable part of agent.yaml: glob patterns, tail vs backfill, multiline stack traces, and NDJSON lines. Use the dedicated guide for examples:

[File Input Configuration](input-configuration.md) — path globs, multiline / match: after|before, rotation, and eight scenario YAML snippets.

Quick reference:


| Topic                                                             | Where                                              |
| ----------------------------------------------------------------- | -------------------------------------------------- |
| Parameter table (paths, encoding, glob-scan-interval-ms, …) | [Configuration — input](configuration.md#input)  |
| Scenario YAML (NDJSON, Java logs, date-stamped files, …)          | [File Input Configuration](input-configuration.md) |


## 5. Engine-side prerequisite

Submit a job with EdgeSocket Source on the Engine. The listen address and token must match the agent output block:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9876
    token = "your-token"
  }
}

sink {
  Console {}
}
```

Save as edgesocket-prod.conf and submit:

```shell
./bin/seatunnel.sh --config ./config/edgesocket-prod.conf
```

:::caution

Confirm the Engine listener port is reachable from the edge host before starting the agent. For RAW/PACKET modes and Sink replacement, see [Output Configuration](output-configuration.md). Wire protocol: [EdgeSocket Source](../connectors/source/EdgeSocket.md).

:::

## 6. Start the agent

```shell
cd "$EDGE_AGENT_HOME"
sh bin/seatunnel-edge-agent.sh start
```

Check status:

```shell
sh bin/seatunnel-edge-agent.sh status
```

Expected application log marker:

```text
BOOTSTRAP_READY edge-agent started agentId=..., inputId=..., inputType=file, outputType=transport
```

Logs default to $EDGE_AGENT_HOME/log/edge-agent.log. Startup control output goes to $EDGE_AGENT_HOME/edge-agent.out unless overridden.

## 7. Stop and upgrade

```shell
sh bin/seatunnel-edge-agent.sh stop
```

To upgrade:

1. Stop the agent.
2. Replace starter/ jars and bin/ scripts from the new package (keep config/agent.yaml and WAL data).
3. Start again.

Preserve edge-agent.id (install root) and the WAL database (queue.sqlite-path, default data/wal.db under data/) if you rely on auto-generated IDs and existing source positions.

## 8. Multiple agents on one host

Run separate install roots (or separate EDGE_AGENT_CONFIG, EDGE_AGENT_PID_FILE, EDGE_AGENT_ID_FILE, and queue.sqlite-path) per agent instance. Do not share one WAL file or edge-agent.id between processes.

## 9. JVM options

The launcher invokes java without a dedicated jvm_options file. Pass extra JVM flags by wrapping the start command or editing bin/seatunnel-edge-agent.sh in your fork. Typical edge settings:

```text
-Xms256m -Xmx512m
```

For foreground debugging, run the starter JAR from the install root with --config pointing at your agent.yaml and Log4j2 config under config/. See bin/seatunnel-edge-agent.sh help and [Operations](operations.md).

