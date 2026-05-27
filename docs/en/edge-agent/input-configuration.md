---
sidebar_position: 6
title: Input Configuration
---

# File Input Configuration

Configure local file collection in agent.yaml under input. Parameter types and defaults are only in [Configuration Reference — input](configuration.md#input); this page provides glob paths, line vs multiline logical events, and ready-to-use YAML.

The Edge Agent input section configures the file collector (type: file, default). It tails local files, supports multiline log merging, serializes each logical event, and hands records to the outbound queue.

## Paths and file discovery

### paths

paths is a list of glob patterns or concrete file paths. The collector:

- Resolves all patterns at startup and opens matching regular files (not directories).
- Re-scans on glob-scan-interval-ms (default 5000) to pick up new files that match the same patterns (for example after log rotation creates app.log.2025-05-19).
- Sorts matched files by last-modified time (oldest first) when first discovered.

:::note

Patterns use the Java NIO glob: syntax (*, **, ?, [...]). Use forward slashes even on Windows.

:::

| Pattern example | Matches |
|-----------------|---------|
| /var/log/myapp/*.log | All .log files directly under /var/log/myapp/ |
| /var/log/myapp/** | All regular files under that tree (recursive) |
| /var/log/nginx/access.log | One file |
| /data/events/app-*.json | Files like app-001.json, app-prod.json |

If a path has no glob characters and points to a directory, the agent collects all regular files under it (equivalent to appending **/*).

### Tail vs full read

| Key                 | Value           | Typical use |
|---------------------|-----------------|-------------|
| read-from-beginning | false (default) | Production tail: on first open with no saved position, start at end of file (only new writes). |
| read-from-beginning | true            | Backfill or one-shot catch-up: read from byte 0 on first open. After restart, saved positions still apply. |

:::note

Positions are stored in the same SQLite database as the outbound queue. Restart continues from the last persisted flush offset (written with WAL append, not engine ACK).

:::

### Rotation and many files

| Key | Role |
|-----|------|
| glob-scan-interval-ms | How often to run glob again and attach new paths. |
| close-inactive-ms | Close file handles idle longer than this (default 5 minutes). |
| on-error: skip | Skip a broken file and continue (fail stops the agent). |

---

## One line per event vs multiline

Default (no multiline, or no pattern): each physical line is one event. Best for NDJSON (one JSON object per line) or plain text logs where stack traces are rare.

Multiline (multiline.pattern set): physical lines are buffered until an event boundary is detected.

| multiline.match | Meaning |
|-------------------|---------|
| after (default) | A line matching the regex starts a new event; buffered lines before it are flushed as the previous event. Typical for logs that start with a timestamp on each new record. |
| before | A line matching the regex is the last line of the current event; buffer flushes immediately after appending it. |
| negate: true | Invert match (advanced; use when boundaries are “lines that do not match”). |

| Key | Role |
|-----|------|
| max-lines | Safety cap on lines per event (default 500). |
| flush-idle-timeout-ms | Force-flush idle multiline buffer after this interval (default 5000 ms). Must be > 0. |

:::note

In YAML, regex backslashes must be escaped in double-quoted strings (`"^\\d{4}-\\d{2}-\\d{2}"`).

:::

### Serialized shape

| output-format.type | Downstream receives |
|----------------------|---------------------|
| line (default) | One JSON object per event with _file, _line, _offset, payload (line text or multiline array). |
| json | Structured JSON from the line payload when parseable. |

This is the agent event envelope, not the EdgeSocket wire encoding (see [EdgeSocket Source](../connectors/source/EdgeSocket.md)).

---

## Scenario examples

Each example shows only input; add output / queue as in [Deployment Guide](deployment-guide.md).

### 1. Single service — wildcard under one directory

Collect all *.log files for one application. Tail new data only.

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"
  read-from-beginning: false
  glob-scan-interval-ms: 5000
```

### 2. Multiple directories or name patterns

Nginx access + error, plus a JSON event directory:

```yaml
input:
  paths:
    - "/var/log/nginx/access.log"
    - "/var/log/nginx/error.log"
    - "/data/myapp/events/**/*.json"
  encoding: UTF-8
```

### 3. NDJSON files

No multiline block. Each line becomes one event; payload holds the raw line.

```yaml
input:
  paths:
    - "/data/ingest/*.ndjson"
  output-format:
    type: line
```

Example file content:

```text
{"user":"a","action":"login"}
{"user":"b","action":"logout"}
```

### 4. Java / Spring logs — timestamp starts a new event

Log lines that begin with 2025-05-19 start a new event; stack trace lines stay with the previous header.

```yaml
input:
  paths:
    - "/var/log/spring-boot/application.log"
  multiline:
    pattern: "^\\d{4}-\\d{2}-\\d{2}"
    match: after
    max-lines: 500
    flush-idle-timeout-ms: 5000
  output-format:
    type: line
```

Example on disk:

```text
2025-05-19 10:00:00 ERROR com.example.Main - failed
java.lang.RuntimeException: boom
    at com.example.Main.run(Main.java:1)
2025-05-19 10:00:01 INFO  com.example.Main - recovered
```

→ Two events: first event = 3 lines; second = 1 line.

### 5. Block ending with a delimiter line

Use when a fixed trailer line marks the end of a record (for example ---END---).

```yaml
input:
  paths:
    - "/opt/batch/output/*.txt"
  multiline:
    pattern: "^---END---$"
    match: before
```

### 6. Backfill after install, then tail

First deployment: read existing content from file start. Later restarts resume from saved offsets.

```yaml
input:
  paths:
    - "/var/log/myapp/*.log"
  read-from-beginning: true
```

:::tip

After the first run, you can set read-from-beginning: false — saved positions take precedence on restart.

:::

### 7. Date-stamped rotating files

Daily files like app-2025-05-19.log appear over time. Glob + periodic discovery picks them up.

```yaml
input:
  paths:
    - "/var/log/myapp/app-*.log"
  glob-scan-interval-ms: 3000
  close-inactive-ms: 600000
```

### 8. Stable input.id across reinstalls

Omitting id uses a stable source identity as the WAL / position sourceId (see [Identity file](configuration.md#identity-file-edge-agentid)). Set an explicit id for multiple agents or WAL restore; keep edge-agent.id with the WAL database:

```yaml
input:
  id: "edge-host-01-nginx"
  paths:
    - "/var/log/nginx/*.log"
```

---

## Nested input.file block

You may put the same keys under input.file instead of flat under input. If both are present, file wins for overlapping keys. Prefer flat input unless you need a clear split for tooling.

```yaml
input:
  id: my-source
  file:
    paths:
      - "/var/log/*.log"
    multiline:
      pattern: "^\\["
      match: after
```

---

