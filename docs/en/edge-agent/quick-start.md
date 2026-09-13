---
sidebar_position: 2
title: Quick Start
---

# Edge Agent Quick Start

Two validation paths:


| Path                                                            | Engine required | Purpose                                    |
| --------------------------------------------------------------- | --------------- | ------------------------------------------ |
| [Console local test](#console-local-test-no-engine)             | No              | Install, collect, local WAL without Engine |
| [Production setup (with Engine)](#production-setup-with-engine) | Yes             | Agent → Engine job                         |


For production hardening, see [Deployment Guide](deployment-guide.md) and [Operations](operations.md).

## Step 1: Install Edge Agent on the edge host

Follow [Download And Build Edge Agent Package](download.md), then:

```shell
export EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent-<version>
cd "$EDGE_AGENT_HOME"
```

The install root contains bin/, config/, and starter/.

## Console local test

### Prerequisites

- Java 11 or 17 on the edge host, with JAVA_HOME set.
- No SeaTunnel Engine and no EdgeSocket connectivity required.
- Writable install root (default WAL data/wal.db, logs, edge-agent.id).

### Configure the agent

Edit $EDGE_AGENT_HOME/config/agent.yaml:

```yaml
input:
  paths:
    - "/tmp/edge-agent-quickstart.log"

output:
  type: console
```

When output.type is omitted, the default is also console. You may omit queue (default sqlite-path: data/wal.db). See [Output Configuration — console](output-configuration.md).

Create the sample log and append a line:

```shell
echo '{"event":"hello","ts":1}' >> /tmp/edge-agent-quickstart.log
```

### Start and verify

```shell
sh bin/seatunnel-edge-agent.sh start
sh bin/seatunnel-edge-agent.sh status
```

Confirm BOOTSTRAP_READY in log/edge-agent.log, then append another line:

```shell
echo '{"event":"world","ts":2}' >> /tmp/edge-agent-quickstart.log
```

Search log/edge-agent.log for EDGE_CONSOLE_OUTPUT (console writes via the app logger, not edge-agent.out). You should see serialized payloads.

:::tip No EDGE_CONSOLE_OUTPUT?

1. Confirm files in input.paths exist and are readable (ls -l /tmp/edge-agent-quickstart.log).
2. Append a new line after BOOTSTRAP_READY (tail mode does not re-read old content on first open).
3. Check log/edge-agent.log for input or WAL errors.

:::

The agent still creates edge-agent.id and the WAL persistence files under data/ (wal.db, wal.db-wal, wal.db-shm), even without Engine. See [Configuration — WAL persistence files](configuration.md#sqlite-persistence-files).

### Stop

```shell
sh bin/seatunnel-edge-agent.sh stop
```

To continue with Engine, see [Production setup](#production-setup-with-engine).

## Production setup

### Prerequisites

- [Step 1](#step-1-install-edge-agent-on-the-edge-host) completed.
- Network: the edge host can reach the Engine EdgeSocket port (example 9876).
- SeaTunnel Engine: cluster or local Zeta where you can submit a job.
- Writable install root.

### Start an Engine job

Submit a job with EdgeSocket Source (HOCON):

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9876
    token = "quick-start-secret"
  }
}

sink {
  Console {}
}
```

:::caution

Note the listen port and token. The agent output.endpoint must be reachable from the edge machine (typically the Engine node IP or load balancer, not 0.0.0.0).

:::

Save as edgesocket-quickstart.conf and submit:

```shell
./bin/seatunnel.sh --config ./config/edgesocket-quickstart.conf
```

### Configure the agent

Edit config/agent.yaml:

```yaml
input:
  paths:
    - "/tmp/edge-agent-quickstart.log"

output:
  type: transport
  endpoint: "<engine-host>:9876"
  auth-type: token
  token: "quick-start-secret"
  packet-mode: RAW
```

Replace `<engine-host>` with the Engine node IP or hostname. If you ran the console test, you can reuse the same log file.

```shell
echo '{"event":"hello-engine","ts":1}' >> /tmp/edge-agent-quickstart.log
```

### Start and verify

```shell
sh bin/seatunnel-edge-agent.sh start
```

Confirm BOOTSTRAP_READY in log/edge-agent.log. After appending log lines, the Engine Console sink should print data after RECEIVED. For REJECTED, see [Operations — Common issues](operations.md#common-issues).

Use this minimal verification chain:

1. Agent log contains BOOTSTRAP_READY.
2. Agent log does not contain AUTH_FAILED / REJECTED.
3. Engine job log shows incoming EdgeSocket batches and Console sink output.
4. If auth fails: AUTH_FAILED usually means output.token vs Engine token mismatch; REJECTED usually means duplicate collector policy conflict.

### Stop

```shell
sh bin/seatunnel-edge-agent.sh stop
```

Keep edge-agent.id and the data/ directory (default wal.db plus -wal/-shm) when migrating or continuing tests. See [FAQ](faq.md).

## Next steps

Once you have completed the quick start, continue with the [Deployment Guide](deployment-guide.md) for production setup, or see the [Configuration Reference](configuration.md) for full parameters. For a complete documentation map, visit [About Edge Agent](about.md#recommended-reading-order).

