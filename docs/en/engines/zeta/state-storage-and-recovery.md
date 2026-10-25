---
sidebar_position: 8
---

# State Storage and Recovery

## Overview

SeaTunnel Engine (Zeta) persists several categories of state data during job execution. Understanding what
each category stores, where it lives, and how to manage it is critical for running production-grade CDC or
long-running streaming jobs.

| Storage Category | Purpose | Default Location |
|---|---|---|
| Checkpoint | Fault-tolerant snapshots of pipeline operator state | `seatunnel.yaml` `checkpoint.storage` |
| Savepoint | User-triggered named checkpoint for planned stop/restart | Same path as checkpoint, different directory |
| IMap / MapStore | Distributed in-memory state (job metadata, running job data, metrics) | `hazelcast.yaml` `map` MapStore configuration (optional; not persisted by default) |
| WAL (Write-Ahead Log) | Append-only log files that back the file-based IMap storage plugin | `<namespace>/<clusterName>/<businessName>/` (IMap storage) |
| History Job Metadata | Completed/failed job records | Same IMap storage location |

---

## 1. Checkpoint Storage

### What is stored

A checkpoint captures a consistent snapshot of all pipeline operator states at a given point in time.
For a CDC job this includes:

- Binlog / WAL offset (MySQL binlog position, PostgreSQL LSN, Oracle SCN)
- Split-level progress for parallel readers
- 2PC sink transaction state (Doris, StarRocks, Kafka transaction IDs)
- SeaTunnelRow buffers in-flight through transforms

### Storage path layout

```
<namespace>/                          # configured namespace, default /seatunnel/checkpoint/
  <job-id>/
    <pipeline-id>/
      <checkpoint-id>/
        <task-location>/state-data
```

### Configuration reference

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 10000              # milliseconds between checkpoints (default 300000)
      timeout: 60000               # checkpoint completion timeout (ms, default 30000)
      min-pause: -1                # minimum pause (ms) between consecutive checkpoints, -1 disables
      monitor-history-size: 32     # checkpoint history entries kept per pipeline (default 32)
      retain-after-job-cancelled: false   # keep checkpoint data after a job is cancelled
      storage:
        type: hdfs                 # checkpoint storage plugin: hdfs | localfile (default localfile,
                                   # deprecated - use the hdfs plugin instead)
        max-retained: 20           # maximum number of retained checkpoints (default 20)
        plugin-config:
          namespace: /seatunnel/checkpoint/    # must end with /
          storage.type: hdfs       # backend filesystem of the hdfs plugin:
                                   # hdfs (default) | local | s3 | oss | cos | gcs
          fs.defaultFS: hdfs://localhost:9000  # required when storage.type is hdfs
          # For S3 (storage.type: s3):
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
          # Environment credentials:
          # fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
```

### Checkpoint retention and cleanup

- Checkpoints are **not** automatically deleted by `history-job-expire-minutes`. They must be
  cleaned up manually or via the retention policy below.
- Only the latest `checkpoint.storage.max-retained` checkpoints (default 20) are kept; older
  checkpoint data is removed. Directories of jobs killed unexpectedly may remain on disk.
- By default, canceled jobs still clean up existing checkpoint data. If you want to retain the
  checkpoints generated during job execution for later recovery, set
  `checkpoint.retain-after-job-cancelled: true` in the job `env`, or set
  `seatunnel.engine.checkpoint.retain-after-job-cancelled: true` in `seatunnel.yaml`.
- **Safe cleanup rule**: A checkpoint directory for a job ID can be deleted only after the job has
  been cancelled and you have confirmed that you do not intend to restore it.

---

## 2. Savepoint

### Savepoint vs Checkpoint

| Aspect | Checkpoint | Savepoint |
|---|---|---|
| Trigger | Periodic / automatic | Manual (`seatunnel.sh -s <jobId>`) |
| Purpose | Fault tolerance | Planned stop, upgrade, migration |
| Lifecycle | Managed by engine | Managed by operator |
| Retention | Auto-rotated | Kept until manually deleted |

### Triggering a savepoint

```bash
# Trigger a savepoint for a running job (the job stops after the savepoint completes)
$SEATUNNEL_HOME/bin/seatunnel.sh -s <job-id>
# Long forms: --savepoint <job-id> or --savepoint-job <job-id>

# Or via REST API v2
curl -X POST http://<master>:8080/stop-job \
  -H 'Content-Type: application/json' \
  -d '{
    "jobId": <job-id>,
    "isStopWithSavePoint": true,
    "force": false
  }'
```

### Restoring from a savepoint

```bash
# Submit with --restore to resume from the latest savepoint
$SEATUNNEL_HOME/bin/seatunnel.sh --config job.conf --restore <job-id>
```

### Restoring from the latest completed checkpoint

```bash
# Submit with --restore-with-checkpoint to resume from the latest completed checkpoint
# The new run gets a new runtime job ID.
$SEATUNNEL_HOME/bin/seatunnel.sh --config job.conf --restore-with-checkpoint <job-id>
```

### Savepoint path layout

```
<namespace>/
  savepoint/
    <job-id>/
      <savepoint-timestamp>/
        <pipeline-id>/
          <task-location>/state-data
```

### Safe cleanup

A savepoint can be deleted only if you are certain you will never restore that job from it.
Deleting an active savepoint mid-restore causes the job to fail with a "state not found" error.

---

## 3. IMap and MapStore (Hazelcast Distributed State)

### What IMap stores

SeaTunnel Engine uses Hazelcast IMap as its distributed in-memory key-value store. The main
logical maps are:

| IMap Name | Content |
|---|---|
| `engine_runningJobInfo` | Metadata of running jobs (job id, job state, etc.) |
| `engine_runningJobState` | Current state machine status of each running job and its pipelines |
| `engine_runningJobMetrics` | Real-time throughput, latency, and record count metrics |
| `engine_finishedJobState` | Terminal state for completed, cancelled, or failed jobs |
| `engine_finishedJobMetrics` | Final metrics snapshot after job termination |
| `engine_finishedJobVertexInfo` | Vertex-level information of finished jobs |
| `engine_stateTimestamps` | State transition timestamps |

### MapStore (disk-backed persistence)

By default the IMap data lives only in cluster memory. Hazelcast MapStore can write IMap entries
to a filesystem so they survive a full cluster restart. This is **separate** from checkpoint
storage, and it is **not** enabled in the shipped `config/hazelcast.yaml`.

To enable it, add a `map` entry to `hazelcast.yaml`. The entry name can use a Hazelcast `*`
wildcard to cover all engine maps:

```yaml
# hazelcast.yaml
map:
  engine_*:
    map-store:
      enabled: true
      initial-mode: EAGER
      class-name: org.apache.seatunnel.engine.server.persistence.FileMapStore
      properties:
        type: hdfs                  # backend filesystem: hdfs (default) | s3 | oss | gcs
        namespace: /seatunnel-imap  # root path for IMap files (default /seatunnel-imap)
        clusterName: seatunnel      # required, used for cluster isolation in the path
        businessName: seatunnel     # required, used to distinguish business data
        fs.defaultFS: hdfs://localhost:9000   # required when type is hdfs
```

Note that `engine_runningJobMetrics` is intentionally not persisted: runtime metrics snapshots
are auxiliary observability state.

IMap files are written under `<namespace>/<clusterName>/<businessName>/`.

### Relationship between IMap, MapStore, and Checkpoint

```
Checkpoint storage  <──────────────────────────────────────────>  Operator state (offsets, splits)
IMap / MapStore     <──────────────────────────────────────────>  Job/pipeline lifecycle state
```

They are **independent**. Deleting checkpoint storage does not affect IMap, and vice versa.
A job can be restarted from a checkpoint even if the IMap MapStore is wiped, **but the job ID
and pipeline mapping must be re-submitted** because `engine_runningJobState` is lost.

---

## 4. Write-Ahead Log (WAL)

The file-based IMap storage plugin (`imap-storage-file`) writes IMap updates to append-only WAL
files under `<namespace>/<clusterName>/<businessName>/` and periodically archives them into
compact ORC files. The WAL guarantees IMap durability against process crashes; there is no
user-tunable compaction or flush-interval option for it.

WAL files for **finished jobs** can be removed once the job's history has expired and you are
certain the job will not be restored. Do not manually delete IMap storage files while jobs are
running.

---

## 5. History Job Expiration

### What `history-job-expire-minutes` does and does NOT do

```yaml
seatunnel:
  engine:
    history-job-expire-minutes: 1440   # 24 hours
```

| Action | Covered by expire? |
|---|---|
| Remove from `finished-job-state` IMap | Yes |
| Remove from `finished-job-metrics` IMap | Yes |
| Keep the job removed after a cluster restart (IMap persistence enabled) | Yes (the expiration time is persisted with the entry, so an expired job is not loaded again and a job that has not expired yet keeps its remaining time) |
| Delete MapStore persistence files for that job | **No** (the expired records stay in the IMap storage files) |
| Delete checkpoint storage directories | **No** |
| Delete savepoint directories | **No** |
| Delete WAL files | **No** |

**Key takeaway**: `history-job-expire-minutes` only cleans up the job metadata stored in
`engine_finishedJobState`. Checkpoint and savepoint directories on HDFS / S3 / local are **never**
touched by this setting. You must manage them separately.

---

## 6. Capacity Planning for Long-Running CDC Jobs

### Checkpoint size estimation

| Source | Typical per-checkpoint size |
|---|---|
| MySQL CDC (1 table, low volume) | 1–10 KB (binlog offset + split state) |
| MySQL CDC (multi-table, 100 splits) | 100 KB – 1 MB |
| MySQL CDC (full snapshot phase) | 10–500 MB (snapshot split states) |
| PostgreSQL CDC (logical replication) | 1–50 KB per table |
| Oracle CDC (LogMiner) | 50 KB – 2 MB |

Formula: `checkpoint_size ≈ N_tables × avg_splits × state_per_split × max_concurrent_checkpoints`

### Storage sizing rule of thumb

Retain **at least 3 checkpoints** per job at all times. Allocate:

```
storage_needed = checkpoint_size × 3 × safety_factor(1.5)
```

### IMap / WAL sizing

Each running CDC job stores approximately:

- 50–200 bytes per pipeline in `engine_runningJobState`
- 1–2 KB per pipeline per metric flush in `engine_runningJobMetrics`

For a cluster running 100 CDC jobs:

```
imap_memory ≈ 100 × 200B ≈ 20 KB (state)
imap_memory ≈ 100 × 1KB × flush_rate ≈ manageable
```

WAL disk: plan 2–5 GB per node for a busy CDC cluster. Mount a dedicated disk partition and monitor
the IMap storage directory with:

```bash
du -sh <namespace>/<clusterName>/<businessName>/
watch -n 60 'du -sh /seatunnel-imap/'
```

---

## 7. Troubleshooting

### Checkpoint directory growing unboundedly

**Symptom**: HDFS / S3 usage grows continuously even after jobs complete.

**Diagnosis**:

```bash
# List checkpoint directories by job ID
hadoop fs -ls /seatunnel/checkpoint/
# Or for local storage
ls -lh /tmp/seatunnel/checkpoint/
```

**Root cause**: SeaTunnel does not have a built-in TTL on checkpoint directories. Each new
checkpoint adds directories; old ones are only removed if the job completes cleanly and its
state is rotated out.

**Fix**:

1. Query finished job metadata through REST API v2:
   ```bash
   curl "http://<master>:8080/finished-jobs/FINISHED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/CANCELED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/FAILED?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/SAVEPOINT_DONE?page=1&rows=100"
   curl "http://<master>:8080/finished-jobs/UNKNOWABLE?page=1&rows=100"
   ```
2. For job IDs not in the response, their checkpoint directories are orphaned and safe to delete.

---

### IMap / WAL directory growing

**Symptom**: The IMap storage directory (e.g. `/seatunnel-imap/`) fills the disk on master/worker
nodes.

**Diagnosis**:

```bash
du -sh <namespace>/<clusterName>/<businessName>/
```

**Root cause candidates**:

- `engine_runningJobMetrics` and finished-job entries accumulate faster than
  `history-job-expire-minutes` removes them
- IMap storage files are append-only between archive cycles, so frequent small updates of
  long-running jobs increase disk usage

**Fix**:

```yaml
# seatunnel.yaml — shorten how long finished-job history is kept
seatunnel:
  engine:
    history-job-expire-minutes: 1440
```

After jobs expire, the corresponding IMap entries are removed and the storage files can be
compacted or removed during a maintenance window (restart the engine after manual deletion to
trigger a clean reload).

---

### "State not found" error on job restart

**Symptom**: Job fails immediately on restart with `checkpoint state not found` or
`restore pipeline state failed`.

**Cause**: The checkpoint directory was deleted or the storage path was changed.

**Fix**:

1. Verify the checkpoint path in `seatunnel.yaml` matches the actual storage location.
2. If the checkpoint is gone, submit the job fresh (no `--restore` flag). For CDC jobs, decide
   whether to restart from `startup.mode=initial` (re-snapshot) or `startup.mode=latest` (skip
   missed data).

---

### Safe cleanup checklist

Before deleting any state directory:

- [ ] Confirm the job is in `FINISHED`, `CANCELLED`, or `FAILED` terminal state
- [ ] Confirm you will not restore from checkpoint or savepoint
- [ ] Confirm the job ID is not referenced in any monitoring or alerting rule
- [ ] For checkpoint storage: delete `<namespace>/<job-id>/` recursively
- [ ] For savepoint: delete `<namespace>/savepoint/<job-id>/` recursively
- [ ] For MapStore/WAL: restart the engine after manual deletion to trigger reload

---

## See Also

- [Checkpoint Storage Configuration](checkpoint-storage.md)
- [REST API v2](rest-api-v2.md) — query job state and metrics via API
