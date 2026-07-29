---
sidebar_position: 5
---

# REST API Job Lifecycle Cookbook

## Overview

This guide supplements the [REST API v2 reference](rest-api-v2.md) with practical recipes for
managing the complete job lifecycle: submission, monitoring, stopping, cancelling, savepoint,
and recovery. It also covers authentication, performance considerations, and common errors.

---

## 1. Prerequisites

Enable the REST API in `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    http:
      enable-http: true
      port: 8080
      enable-dynamic-port: true
      port-range: 100
```

All examples below use `http://<master>:8080`. Replace with your actual master host and port.

---

## 2. Job Submission

### 2.1 Submit a job from a config file (JSON body)

```bash
curl -X POST http://<master>:8080/submit-job \
  -H "Content-Type: application/json" \
  -d @job.json
```

Minimal `job.json` structure:

```json
{
  "env": {
    "job.name": "my-cdc-job",
    "job.mode": "STREAMING",
    "checkpoint.interval": 30000
  },
  "source": [
    {
      "plugin_name": "MySQL-CDC",
      "plugin_output": "mysql_cdc_result",
      "base-url": "jdbc:mysql://localhost:3306/mydb",
      "username": "cdc_user",
      "password": "password",
      "database-names": ["mydb"],
      "table-names": ["mydb.orders"],
      "startup.mode": "initial",
      "server-id": "5400-5404"
    }
  ],
  "transform": [],
  "sink": [
    {
      "plugin_name": "Console",
      "plugin_input": ["mysql_cdc_result"]
    }
  ]
}
```

### 2.2 Submit a job with multiple transforms (JSON format)

```json
{
  "env": {
    "job.name": "etl-with-transforms",
    "job.mode": "BATCH"
  },
  "source": [
    {
      "plugin_name": "FakeSource",
      "plugin_output": "fake",
      "row.num": 100,
      "schema": {
        "fields": {
          "id": "int",
          "name": "string",
          "amount": "double"
        }
      }
    }
  ],
  "transform": [
    {
      "plugin_name": "FieldMapper",
      "plugin_input": ["fake"],
      "plugin_output": "after_field_map",
      "field_mapper": {
        "id": "user_id",
        "name": "user_name"
      }
    },
    {
      "plugin_name": "Filter",
      "plugin_input": ["after_field_map"],
      "plugin_output": "filtered",
      "fields": ["user_id", "user_name", "amount"]
    }
  ],
  "sink": [
    {
      "plugin_name": "Console",
      "plugin_input": ["filtered"]
    }
  ]
}
```

### 2.3 Submission response

A successful submission returns:

```json
{
  "jobId": "733584788375093248",
  "jobName": "my-cdc-job"
}
```

Save the `jobId` for all subsequent lifecycle operations.

---

## 3. Job Status Query

### 3.1 Query a single running job

```bash
curl http://<master>:8080/job-info/<jobId>
```

Response fields:

| Field | Description |
|---|---|
| `jobId` | Unique job identifier |
| `jobName` | Human-readable job name |
| `jobStatus` | `RUNNING`, `FINISHED`, `FAILED`, `CANCELLED` |
| `envOptions` | Env configuration applied |
| `createTime` | Job creation timestamp |
| `jobDag` | DAG structure |
| `metrics` | Source/sink throughput counters |

### 3.2 Query all running jobs

```bash
curl "http://<master>:8080/running-jobs?page=1&rows=10"
```

### 3.3 Query finished jobs

```bash
curl "http://<master>:8080/finished-jobs/FINISHED?page=1&rows=10"
```

Use the `state` path parameter to filter finished jobs, for example `FINISHED`, `FAILED`,
`CANCELED`, or `SAVEPOINT_DONE`.

### 3.4 Query job metrics only

```bash
curl http://<master>:8080/job-info/<jobId>
```

Key metric fields:

| Metric | Meaning |
|---|---|
| `SourceReceivedCount` | Total rows read from source |
| `SinkWriteCount` | Total rows written to sink |
| `SourceReceivedQPS` | Current read throughput (rows/sec) |
| `SinkWriteQPS` | Current write throughput (rows/sec) |

---

## 4. Querying Job Logs

```bash
# Get the last N lines of a running job's log
curl "http://<master>:8080/logs/<jobId>"
```

For large deployments where log files are on individual workers, use the worker's REST port
directly, or configure centralized logging (see [Logging](logging.md)).

---

## 5. Stop, Cancel, and Savepoint Semantics

### Semantics comparison

| Operation | What happens | State preserved | Can resume |
|---|---|---|---|
| `stop` (graceful) | Waits for in-flight data to flush | Checkpoint at stop point | Yes, via `--restore` |
| `stop-with-savepoint` | Graceful stop + explicit savepoint written | Full savepoint | Yes, via `--restore` |
| `cancel` (force kill) | Immediate termination | No new state written | Only from last checkpoint |

### 5.1 Graceful stop (no savepoint)

```bash
curl -X POST "http://<master>:8080/stop-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248", "isStopWithSavePoint": false}'
```

### 5.2 Stop with savepoint

```bash
curl -X POST "http://<master>:8080/stop-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248", "isStopWithSavePoint": true}'
```

The savepoint path is printed in the job log and returned in the job final state:

```bash
curl http://<master>:8080/job-info/733584788375093248 | \
  python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('savepointPath', 'N/A'))"
```

### 5.3 Cancel (force)

```bash
curl -X POST "http://<master>:8080/stop-job" \
  -H "Content-Type: application/json" \
  -d '{"jobId": "733584788375093248", "isStopWithSavePoint": false, "force": true}'
```

---

## 6. Job Recovery and Restart

### 6.1 Restart from the latest checkpoint

Submit the job again with the `jobId` parameter to restore from the last successful checkpoint:

```bash
curl -X POST http://<master>:8080/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": {
      "job.id": "733584788375093248",
      "job.name": "my-cdc-job",
      "job.mode": "STREAMING",
      "checkpoint.interval": 30000
    },
    "source": [ ... ],
    "sink": [ ... ]
  }'
```

Providing the same `job.id` instructs the engine to restore state from the existing checkpoint
directory for that job.

### 6.2 Restart from a specific savepoint path

```bash
curl -X POST http://<master>:8080/submit-job \
  -H "Content-Type: application/json" \
  -d '{
    "env": {
      "job.name": "my-cdc-job-restored",
      "job.mode": "STREAMING",
      "checkpoint.interval": 30000,
      "restore.mode": "savepoint",
      "savepoint.path": "/seatunnel/checkpoint/savepoint/733584788375093248/1748595600000"
    },
    "source": [ ... ],
    "sink": [ ... ]
  }'
```

---

## 7. Authentication and Authorization

When basic authentication is enabled (see [Security](security.md)), all REST API calls must
include the configured username and password.

### Basic auth example

```bash
curl -u admin:password "http://<master>:8080/running-jobs?page=1&rows=10"
```

---

## 8. REST API Performance Considerations

### `job-info` slowness with many finished jobs

When `finished-job-state` IMap grows large (thousands of entries), the
`/running-jobs` and `/finished-jobs/:state` endpoints may become slow because they scan all entries.

**Mitigations:**
1. Reduce `history-job-expire-minutes` to shorten the retention window
2. Avoid polling finished-jobs endpoints at high frequency; cache the result in your monitoring layer
3. For dashboards, query specific `jobId` directly instead of listing all jobs

### Concurrent submission rate

The REST API processes submissions synchronously in the Hazelcast executor pool. For bulk
submission scenarios (importing hundreds of jobs), throttle submissions to 10–20 per second
to avoid overwhelming the master node.

### Dynamic port allocation

If `enable-dynamic-port: true`, different master nodes may use different ports. Use the
REST API overview endpoint to inspect the active cluster state from a reachable master:

```bash
# Inspect the cluster state from a reachable master
curl http://<master>:8080/overview | \
  python3 -c "import sys,json; print(json.load(sys.stdin))"
```

---

## 9. Common Errors and Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `HTTP 404` on any endpoint | REST API not enabled or wrong port | Set `enable-http: true` and check port |
| `Connection refused` | Master not started or firewall blocking port | Verify master process is running; check firewall |
| `jobId not found` in `job-info` | Job has already finished or was never started | Query `/finished-jobs/:state` with the expected final state |
| Submit returns `400 Bad Request` | Malformed JSON or missing required fields | Validate JSON; check `plugin_name` spelling |
| `Job already exists with same job.id` | Submitting duplicate `job.id` without stopping first | Cancel or stop the existing job, then resubmit |
| `Unauthorized 401` | Basic authentication is enabled but no credentials were provided | Include `-u user:pass` |
| `Savepoint path not found` | Savepoint was deleted or path is wrong | Check checkpoint storage and provide correct path |

---

## See Also

- [REST API v2 Reference](rest-api-v2.md)
- [REST API v1 Reference](rest-api-v1.md)
- [Security Configuration](security.md)
- [Checkpoint Storage](checkpoint-storage.md)
- [CDC Pipeline Architecture](../../architecture/cdc-pipeline-architecture.md)
