---
sidebar_position: 5
title: Configuration Reference
---

# Edge Agent Configuration Reference

Edge Agent reads a single YAML file (default config/agent.yaml). This page is the authoritative reference for every supported key, its type, default, and user-visible behavior.

Example file: [seatunnel-edge-agent/config/agent.yaml](../../../seatunnel-edge-agent/config/agent.yaml). For guided setup, start with [Quick Start](quick-start.md); for scenario YAML, see [File Input Configuration](input-configuration.md) and [Output Configuration](output-configuration.md).

For terminology (WAL, BEST_EFFORT, WAL row states, Engine response codes), see [Glossary](about.md#glossary).

## Top-level layout

```yaml
agent:    # defaults apply when omitted
input:    # required — at least paths (file collector)
# queue:  # when omitted, sqlite-path defaults to data/wal.db
# retry:  # when omitted, retry table defaults apply
output:   # production: type transport + endpoint (+ token)
```

:::note Layout

agent.yaml uses top-level sections only: agent, input, queue, retry, output. Omit queue to use default sqlite-path: data/wal.db. Omit retry to use built-in retry defaults. For local debugging, output.type: console writes EDGE_CONSOLE_OUTPUT entries to log/edge-agent.log instead of sending EdgeSocket traffic.

:::

## agent

Process-wide settings.


| Key                  | Type   | Required | Default       | Description                                                                                                                                                                                                                                                                                                                                                |
| -------------------- | ------ | -------- | ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| id                 | string | No       | auto          | Agent instance identity (logs, ops). Auto: see [Identity file](#identity-file-edge-agentid).                                                                                                                                                                                                                                                               |
| delivery-guarantee | string | No       | BEST_EFFORT | Outbound delivery mode. Only BEST_EFFORT is supported in this release (aliases: best-effort, best_effort). Unacknowledged sends are persisted and retried, so the same record may reach output more than once—design downstream consumers to be idempotent. Details: [Delivery mode](./architecture-overview.md#62-delivery-mode). |
| idle-sleep-ms      | long    | No       | 200         | Sleep (ms) when a scheduler loop iteration makes no progress. Must be > 0.       |
| bulk-max-size      | integer | No       | 256         | Flush in-memory reader buffer to WAL when this many events are pending.           |
| flush-interval-ms  | long    | No       | 1000        | Flush in-memory reader buffer to WAL after this many ms since the buffer became non-empty. |


:::caution About BEST_EFFORT

- Durable local WAL with automatic retry until the engine returns RECEIVED (or the row becomes DEAD).
- The same WAL row may be sent more than once after failures, crashes, or resurrectSending.
- Use idempotent sinks or deduplication keys when you need strict uniqueness.

:::

## input

File collector (type: file, default). One input source per agent instance.

For glob patterns, multiline log assembly, and scenario-based YAML examples, see [File Input Configuration](input-configuration.md).


| Key                     | Type           | Required | Default  | Description                                                                                                                                   |
| ----------------------- | -------------- | -------- | -------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| id                    | string         | No       | auto     | Source identity; WAL/position sourceId. Auto: see [Identity file](#identity-file-edge-agentid).                                             |
| type                  | string         | No       | file   | Input plugin id. Only file is implemented.                                                                                                  |
| paths                 | list of string | Yes  | —        | Glob patterns for files to tail (e.g. /var/log/*.log). Must be non-empty; blank entries are invalid.                                        |
| encoding              | string         | No       | UTF-8  | File character encoding.                                                                                                                      |
| read-from-beginning   | boolean        | No       | false  | true: read from file start on first open. false: tail from EOF when no saved position exists. Saved byte offset still applies on restart. |
| glob-scan-interval-ms | long           | No       | 5000   | How often (ms) to scan globs for new files.                                                                                                   |
| close-inactive-ms     | long           | No       | 300000 | Close idle file handles after this many ms without reads.                                                                                     |
| on-error              | string         | No       | skip   | Per-file IO error handling: skip removes the file cursor and continues; fail aborts the agent.                                            |


### input.multiline

Multiline log assembly support. Omit multiline (or omit pattern) for one physical line = one event.


| Key         | Type    | Required | Default | Description                                                                                                                                |
| ----------- | ------- | -------- | ------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| pattern   | string  | No*      | —       | Regex for event boundaries. Setting a non-empty pattern enables multiline mode.                                                            |
| match     | string  | No       | after | after: matching line starts a new event; prior buffer is flushed. before: matching line is the last line of the current event. |
| negate    | boolean | No       | false | Invert regex match semantics.                                                                                                              |
| max-lines | integer | No       | 500   | Maximum physical lines buffered into one multiline event.                                                                                  |
| flush-idle-timeout-ms | long | No | 5000 | Force-flush the multiline buffer when idle longer than this (ms). Must be > 0 when multiline is enabled. |


:::caution

pattern is required in practice when multiline mode is enabled.

:::

### input.output-format

How each logical event is serialized before WAL / transport (not the EdgeSocket wire format).


| Key    | Type   | Required | Default | Description                                                                                                 |
| ------ | ------ | -------- | ------- | ----------------------------------------------------------------------------------------------------------- |
| type | string | No       | line  | line: JSON wrapper per event with _file, _line, _offset, payload. json: structured JSON output. |


### Nested input.file block

Use this nested block when you need to override overlapping top-level fields; it has the same keys as the flat input fields above (paths, encoding, multiline, output-format, etc.). When both flat keys and input.file are set, nested file wins for overlapping fields.

## queue

SQLite WAL outbound buffer and in-process batching before WAL append.


| Key                     | Type    | Required | Default       | Description                                                                                                                                                                                                                    |
| ----------------------- | ------- | -------- | ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| sqlite-path           | string  | No       | data/wal.db | Path to the SQLite database file (WAL + source positions). Parent directory data/ is created automatically. Relative paths resolve from the agent working directory (install root when using bin/seatunnel-edge-agent.sh). |
| poll-batch-size       | integer | No       | 128         | Max WAL rows claimed per scheduler iteration; also caps the input poll batch size for that iteration.                                                                                                                          |
| cleanup-batch-size    | integer | No       | 128         | Max ACKED WAL rows deleted per cleanup pass.                                                                                                                                                                                   |
| acked-retention-ms    | long    | No       | 0           | Retain ACKED rows for this many ms before cleanup; 0 means delete as soon as cleanup runs (subject to batch size).                                                                                                           |
| resurrect-batch-size  | integer | No       | 100         | Max SENDING rows reset to PENDING per resurrection pass (crash recovery).                                                                                                                                                  |
| resurrect-interval-ms | long    | No       | 60000       | Interval (ms) between resurrection passes; doubles as the staleness threshold for SENDING rows. Must be > 0. |


### SQLite persistence files

:::note SQLite file details

sqlite-path is a single database file path (not a directory). Default data/wal.db under install root data/, with -wal and -shm sidecars in the same directory.

:::

| Item      | Description                                                                                                  |
| --------- | ------------------------------------------------------------------------------------------------------------ |
| Path      | Default data/wal.db; custom paths such as state/agent.db are allowed                  |
| On disk   | wal.db plus -wal and -shm sidecars                                 |
| In the DB | Outbound queue edge_agent_wal; source positions edge_agent_source_position (source_id = input.id) |
| Migration | Copy with edge-agent.id; do not share one sqlite-path across agents                                      |

See also [FAQ](faq.md).

## retry

WAL row send retry policy (scheduler / WAL layer). When retry is absent from YAML, the defaults in the table below apply.


| Key              | Type    | Required | Default  | Description                                                                                                        |
| ---------------- | ------- | -------- | -------- | ------------------------------------------------------------------------------------------------------------------ |
| max-attempts   | integer | No       | 16     | Max send attempts per WAL row (attempt_count); rows at or above this limit are marked DEAD and no longer sent. |
| backoff-ms     | long    | No       | 250    | Scheduler base backoff (ms) after a failed WAL batch send (distinct from transport reconnect backoff).             |
| backoff-max-ms | long    | No       | 300000 | Upper bound (ms) for scheduler send backoff; must be ≥ backoff-ms.                                               |


## output

Outbound destination. Default plugin is console when type is omitted.

For endpoint/token alignment, RAW vs PACKET, and scenario YAML, see [Output Configuration](output-configuration.md).


| Key    | Type   | Required | Default   | Description                                                                                                             |
| ------ | ------ | -------- | --------- | ----------------------------------------------------------------------------------------------------------------------- |
| id   | string | No       | auto      | Outbound logical identity (logs, migration; not on the wire). Auto: see [Identity file](#identity-file-edge-agentid).   |
| type | string | No       | console | transport: EdgeSocket client. console: write payload logs as EDGE_CONSOLE_OUTPUT to log/edge-agent.log (debug). |


### output when type: transport

EdgeSocket collector client settings.


| Key                       | Type    | Required | Default | Description                                                                                                                         |
| ------------------------- | ------- | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| endpoint                | string  | Yes  | —       | Collector address host:port. Must match the SeaTunnel job [EdgeSocket Source](../connectors/source/EdgeSocket.md) listen address. |
| auth-type               | string  | No       | token | Does not support auth-type none; must match EdgeSocket source authentication.                                                     |
| token                   | string  | Yes  | —       | Shared secret for `__AUTH__`; must match EdgeSocket token in the engine job.                                                      |
| connect-timeout-ms      | integer | No       | 5000  | TCP connect timeout (ms).                                                                                                           |
| read-timeout-ms         | integer | No       | 30000 | TCP read timeout (ms).                                                                                                              |
| max-batch-send-attempts | integer | No       | 64    | Send attempts per batch before transport reconnect logic.                                                                           |
| initial-backoff-ms      | long    | No       | 100   | Initial reconnect backoff (ms).                                                                                                     |
| max-backoff-ms          | long    | No       | 30000 | Maximum reconnect backoff (ms).                                                                                                     |
| max-reconnect-cycles    | integer | No       | 16    | Max reconnect cycles for a failing batch.                                                                                           |
| packet-mode             | string  | No       | RAW   | RAW: line payloads as-is. PACKET: framed packets with compression/encryption support.                                          |
| compression             | string  | No       | gzip  | PACKET mode only (ignored when packet-mode is RAW): none, gzip, zlib, or deflate.                                   |
| encryption              | string  | No       | none  | PACKET mode: none or aes_gcm.                                                                                               |
| aes-secret-key-base64   | string  | Cond.    | —       | Base64 AES key; required when encryption is aes_gcm.                                                                            |


### output when type: console

No additional keys. Payloads are logged as EDGE_CONSOLE_OUTPUT in log/edge-agent.log for local debugging.

## Identity file

When agent.id, input.id, or output.id are omitted from YAML, the agent reads or writes them in install-root/edge-agent.id (same directory as edge-agent.pid). Explicit IDs in YAML always win.


| Key / YAML  | Role                                                                                                              |
| ----------- | ----------------------------------------------------------------------------------------------------------------- |
| agent.id  | Agent instance identity (logs, distinguishing multiple agents)                                                    |
| input.id  | Source identity; WAL / source-position sourceId — restarts resume offsets instead of treating the source as new |
| output.id | Outbound logical identity (migration and logs; not sent on the wire in this release)                              |


Example file:

```text
agent.id=<uuid>
input.id=<uuid>
output.id=<uuid>
```

:::caution

Keep edge-agent.id and SQLite files (default data/wal.db and sidecars) when migrating or upgrading. If lost and input.id is not set in YAML, a new ID is generated and existing positions no longer apply.

:::

See also [FAQ](faq.md).

## Environment variables

:::note

Launcher overrides (EDGE_AGENT_CONFIG, EDGE_AGENT_PID_FILE, EDGE_AGENT_ID_FILE, etc.) are documented in [Operations — Environment variables](operations.md#environment-variables). They are not part of agent.yaml.

:::

## Related

- [Quick Start](quick-start.md)
- [Deployment Guide](deployment-guide.md)
- [File Input Configuration](input-configuration.md)
- [Output Configuration](output-configuration.md)
- [Architecture Overview](./architecture-overview.md)
- [EdgeSocket Source](../connectors/source/EdgeSocket.md)

