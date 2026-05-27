---
sidebar_position: 8
title: Operations
---

# Edge Agent Operations

Day-two operations for Edge Agent: lifecycle scripts, logging, health checks, and troubleshooting.

## Lifecycle commands

From EDGE_AGENT_HOME (install root):

```bash
# Start agent in background
sh bin/seatunnel-edge-agent.sh start

# Stop
sh bin/seatunnel-edge-agent.sh stop

# Check process status
sh bin/seatunnel-edge-agent.sh status

# SQLite WAL / source-position operations
sh bin/seatunnel-edge-agent.sh db [subcommand]

# Help
sh bin/seatunnel-edge-agent.sh help
```

:::note

Windows: use bin\seatunnel-edge-agent.cmd with the same subcommands.

:::

## SQLite operations

Inspect or maintain the database at queue.sqlite-path without writing raw SQL. Implementation uses the agent JAR and SQLite JDBC (no system sqlite3 required).

```bash
# Overview (safe while agent is running)
sh bin/seatunnel-edge-agent.sh db info
sh bin/seatunnel-edge-agent.sh db wal-summary
sh bin/seatunnel-edge-agent.sh db wal-list --status DEAD --limit 20
sh bin/seatunnel-edge-agent.sh db wal-show --id 42

# Source file positions
sh bin/seatunnel-edge-agent.sh db positions

# Writes: stop the agent first, then use --yes (or --dry-run to preview)
sh bin/seatunnel-edge-agent.sh stop
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --dry-run
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --yes
sh bin/seatunnel-edge-agent.sh db wal-retry-dead --yes    # may duplicate sends (WAL retry under BEST_EFFORT)
sh bin/seatunnel-edge-agent.sh db wal-unstick-sending --yes
sh bin/seatunnel-edge-agent.sh db wal-purge-acked --older-than-ms 86400000 --yes
```


| Subcommand            | Read / write | Description                                                                         |
| --------------------- | ------------ | ----------------------------------------------------------------------------------- |
| info                | Read         | DB path, -wal/-shm sizes, whether agent PID is alive                            |
| wal-summary         | Read         | Row counts and oldest updated_at per WAL status                                   |
| wal-list            | Read         | List WAL rows; first column id(pk) is the row primary key for wal-show      |
| wal-show            | Read         | One row detail; requires --id from wal-list (not source_id or batch_id) |
| positions           | Read         | File tail offsets (--source-id can be used to scope a source)                     |
| wal-purge-dead      | Write        | Delete DEAD rows                                                                  |
| wal-retry-dead      | Write        | Reset DEAD → PENDING                                                            |
| wal-unstick-sending | Write        | Reset SENDING → PENDING                                                         |
| wal-purge-acked     | Write        | Delete old ACKED rows (--older-than-ms)                                         |


Run sh bin/seatunnel-edge-agent.sh db help for all flags.

:::caution SQLite path for db commands

The file used by db (highest priority first): --sqlite-path → EDGE_AGENT_SQLITE_PATH → install-root/data/wal.db (default; -wal/-shm sit under data/).

db does not read queue.sqlite-path from agent.yaml. If the running agent uses a custom path, pass the same path with --sqlite-path.

:::

## Environment variables


| Variable                             | Default                                     | Purpose                                           |
| ------------------------------------ | ------------------------------------------- | ------------------------------------------------- |
| EDGE_AGENT_CONFIG                  | $EDGE_AGENT_HOME/config/agent.yaml        | Agent YAML path                                   |
| EDGE_AGENT_SQLITE_PATH             | install-root/data/wal.db when unset         | SQLite file for db (below --sqlite-path)      |
| EDGE_AGENT_PID_FILE                | $EDGE_AGENT_HOME/edge-agent.pid           | PID file                                          |
| EDGE_AGENT_ID_FILE                 | $EDGE_AGENT_HOME/edge-agent.id            | Install-root identity file path                   |
| EDGE_AGENT_LOG_FILE                | $EDGE_AGENT_HOME/edge-agent.out           | Startup script log                                |
| EDGE_AGENT_LOG_CONFIG              | $EDGE_AGENT_HOME/config/log4j2.properties | Log4j2 config                                     |
| EDGE_AGENT_LOG_DIR                 | $EDGE_AGENT_HOME/log                      | Application log directory                         |
| EDGE_AGENT_APP_LOG_NAME            | edge-agent.log                            | Application log file name                         |
| EDGE_AGENT_STARTUP_READY_TIMEOUT_S | 10                                        | Seconds to wait for BOOTSTRAP_READY after start |


## Log files


| File                 | Content                               |
| -------------------- | ------------------------------------- |
| log/edge-agent.log | Main application log (Log4j2 rolling) |
| edge-agent.out     | Script-level start/stop messages      |


:::note Key log keywords

- BOOTSTRAP_READY — successful startup (also used by the start script).
- BOOTSTRAP_FAILED — fatal startup error.
- Shutdown signal received — graceful shutdown hook.

Config path is logged at DEBUG only.

:::

## Health and monitoring

- Process: status subcommand or `kill -0 <pid>`.
- Functional: engine EdgeSocket source returns RECEIVED for batches and shows ingest.
- WAL: monitor the queue.sqlite-path database file and its -wal/-shm sidecars; watch disk free space.
- Backpressure: collector may respond with QUEUE_FULL or RETRY — see [EdgeSocket Source](../connectors/source/EdgeSocket.md).

## Common issues

### AUTH_FAILED

Token authentication failed. Typical cause:

- output.token does not match engine EdgeSocket token.

Fix token alignment and restart the agent. Do not expect WAL retry to recover a bad token configuration.

### REJECTED

Collector was rejected by source policy. Typical cause:

- Duplicate agent instances using the same identity against one listener.

Keep only one active collector per listener policy. Do not expect auto-reconnect after REJECTED.

### BOOTSTRAP_READY not seen within timeout

- Check log/edge-agent.log for stack traces (config validation, SQLite path, missing paths).
- Increase EDGE_AGENT_STARTUP_READY_TIMEOUT_S on slow disks.
- Verify starter/seatunnel-edge-agent-starter.jar exists.

### Source positions reset after reinstall

If edge-agent.id is deleted and input.id is not set in YAML, positions no longer match (treated as a new source). When migrating or upgrading, copy edge-agent.id with the SQLite files at queue.sqlite-path (default data/wal.db under data/, including wal.db-wal and wal.db-shm), or set explicit IDs in YAML. See [FAQ — Identity and recovery](faq.md#identity-and-recovery).

### WAL rows stuck in DEAD or SENDING

Use db wal-list, db wal-show, and write subcommands documented above. For when to purge vs retry and duplicate-send risk, see [FAQ — DEAD rows](faq.md#what-does-wal-status-dead-mean).

## systemd example

```ini
[Unit]
Description=SeaTunnel Edge Agent
After=network.target

[Service]
Type=forking
Environment=EDGE_AGENT_HOME=/opt/apache-seatunnel-edge-agent
Environment=JAVA_HOME=/usr/lib/jvm/java-11
ExecStart=/bin/sh ${EDGE_AGENT_HOME}/bin/seatunnel-edge-agent.sh start
ExecStop=/bin/sh ${EDGE_AGENT_HOME}/bin/seatunnel-edge-agent.sh stop
PIDFile=${EDGE_AGENT_HOME}/edge-agent.pid
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

Adjust paths and user. Validate ExecStart behavior on your distribution (some setups prefer Type=simple with foreground Java).

