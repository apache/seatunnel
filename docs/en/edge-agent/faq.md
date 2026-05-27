---
sidebar_position: 9
title: FAQ
---

# FAQ

High-frequency questions about identity, migration, WAL DEAD rows, and multiple agents on one host. Commands and troubleshooting trees: [Operations](operations.md). Full parameter tables: [Configuration Reference](configuration.md).

## Identity and recovery

### What is edge-agent.id?

An identity file at the install root (next to edge-agent.pid). When agent.id, input.id, or output.id are omitted from YAML, the agent reads or writes them here; explicit YAML IDs win. See [Identity file](configuration.md#identity-file-edge-agentid).

### What do agent.id, input.id, and output.id do?

| Key / YAML | Role |
|------------|------|
| agent.id | Agent instance identity (logs, multiple agents on one host) |
| input.id | Source identity; WAL / position sourceId for resume after restart |
| output.id | Logical outbound identity (migration and logs; not on the wire in this release) |

### Which ID is used to resume file positions after restart?

input.id. Positions live in edge_agent_source_position, keyed by source_id (= input.id) and file path. agent.id and output.id are not used for position lookup.

### What must I keep when migrating or upgrading?

1. edge-agent.id (especially input.id)
2. The SQLite file at queue.sqlite-path, plus -wal and -shm sibling files (do not copy only the main file)

For migration or upgrade, you must keep config/agent.yaml (IDs can be omitted there if already recorded in the identity file).

### What happens if I delete edge-agent.id?

If input.id is not set in YAML, a new input.id is generated and existing positions no longer match (treated as a new source).

### Multiple agents on one host?

Use separate install roots (or separate edge-agent.id and queue.sqlite-path). Do not share one database file.

### What is EDGE_AGENT_ID_FILE?

Launcher environment variable; default $EDGE_AGENT_HOME/edge-agent.id. See [Operations — Environment variables](operations.md#environment-variables).

## SQLite persistence

Default path, on-disk files (data, -wal, -shm), and what each table stores: [Configuration — SQLite persistence files](configuration.md#sqlite-persistence-files). Console mode still uses this database.

### What does WAL status DEAD mean?

A row exceeded retry.max-attempts (attempt_count on claim). The scheduler no longer sends it. Inspect with:

```bash
sh bin/seatunnel-edge-agent.sh db wal-list --status DEAD
sh bin/seatunnel-edge-agent.sh db wal-show --id <row-id>
```

After fixing the root cause (endpoint, token, payload), stop the agent and purge or retry:

```bash
sh bin/seatunnel-edge-agent.sh stop
sh bin/seatunnel-edge-agent.sh db wal-purge-dead --yes
# or: db wal-retry-dead --yes  (may duplicate sends under BEST_EFFORT WAL retry)
```

There is no automatic purge of DEAD rows in this release. See [Operations](operations.md) for SQLite db commands.

### How does the agent choose EdgeSocket batchId?

Each outbound WAL row stores a batch_id allocated from the edge_agent_meta table (next_batch_id). The scheduler sends `__BATCH__:<batch_id>:...` on the wire. Retries for the same WAL row reuse the same batch_id.

The WAL row id (auto-increment) is only for local ack/cleanup; it is not the wire batchId.

Keep the database file at queue.sqlite-path (and -wal / -shm) across agent restarts so next_batch_id keeps increasing. If you delete or recreate that file, counters restart from 1, which can conflict with [EdgeSocket Source](../connectors/source/EdgeSocket.md) expectations for globally monotonic batch ids after an engine checkpoint.

This release does not send `__COMMIT__`; the agent does not read engine checkpoint watermarks to pick the next batchId.

### Wrong token or AUTH_FAILED?

Authentication failures are fatal (the agent exits after logging). Fix output.token and the engine EdgeSocket token so they match, then restart; do not expect WAL retry to recover a misconfigured token. (secret_key is for PACKET encryption, not auth token matching.)

