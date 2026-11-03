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
| IMap / MapStore | Distributed in-memory state (job metadata, running job data, metrics) | `hazelcast.yaml` MapStore base-dir |
| WAL (Write-Ahead Log) | Durability log for IMap persistence | Same MapStore base-dir |
| History Job Metadata | Completed/failed job records | MapStore base-dir |

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
      interval: 300000             # milliseconds between checkpoints
      timeout: 60000               # checkpoint completion timeout (ms)
      min-pause: 100               # minimum pause (ms) between the end of one checkpoint and the start of the next
      monitor-history-size: 10     # number of recent checkpoints kept for monitoring (default 32)
      storage:
        type: hdfs                 # hdfs | localfile (deprecated)
        plugin-config:
          namespace: /seatunnel/checkpoint/    # must end with /
          # For S3:
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
          # Environment credentials:
          # fs.s3a.aws.credentials.provider: com.amazonaws.auth.EnvironmentVariableCredentialsProvider
```

### Checkpoint retention and cleanup

- Checkpoints are **not** automatically deleted by `history-job-expire-minutes`. They must be
  cleaned up manually or via a configurable retention policy.
- Only the **latest N checkpoints** are retained per pipeline (controlled by Hazelcast in-memory
  references). Old checkpoint directories may remain on disk if the job was killed unexpectedly.
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
| Trigger | Periodic / automatic | Manual (`seatunnel.sh -r <jobId> --savepoint`) |
| Purpose | Fault tolerance | Planned stop, upgrade, migration |
| Lifecycle | Managed by engine | Managed by operator |
| Retention | Auto-rotated | Kept until manually deleted |

### Triggering a savepoint

```bash
# Stop a running job and create a savepoint
$SEATUNNEL_HOME/bin/seatunnel.sh --stop-job <job-id> --savepoint

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

SeaTunnel Engine uses Hazelcast IMap as its distributed in-memory key-value store. The following
logical maps are the main persisted IMaps (names as defined in the engine code):

| IMap Name | Content |
|---|---|
| `engine_runningJobInfo` | Job information (job DAG) of each running job |
| `engine_runningJobState` | Current state machine status of each running job |
| `engine_finishedJobState` | Terminal state for completed, cancelled, or failed jobs |
| `engine_finishedJobMetrics` | Final metrics snapshot after job termination |
| `engine_checkpoint-id-map` | Checkpoint IDs (engine checkpoint id mapping) |

### MapStore (disk-backed persistence)

Hazelcast MapStore writes IMap entries to disk so they survive process restarts. This is
**separate** from checkpoint storage.

IMap persistence is configured in `hazelcast.yaml` through the `map.engine*` `map-store` block,
backed by `FileMapStoreFactory`. For example, with HDFS:

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap
        clusterName: seatunnel-cluster
        storage.type: hdfs
        fs.defaultFS: hdfs://localhost:9000
```

For a single node without HDFS, set `fs.defaultFS: file:///` to store the data under the local
file system. The `namespace` property defines the root directory where the IMap data and its
write-ahead log files are written. See
[Separated Cluster Deployment](separated-cluster-deployment.md) for the full description of each
property. Note that `engine_runningJobMetrics` is intentionally excluded from persistent IMap
storage to avoid excessive WAL growth.

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

Hazelcast uses a write-ahead log for MapStore durability. WAL files are written by the
`imap-storage-file` plugin under the configured map-store `namespace` directory.

### WAL growth in long-running CDC jobs

Every IMap update that the MapStore persists generates a WAL write. Over time (days or weeks),
WAL files can grow into several GB when jobs run at very high throughput.

**Mitigation:**

- Keep the high-frequency metrics IMap out of persistence. `engine_runningJobMetrics` is already
  excluded by the engine's MapStore; do not rename maps to work around this.
- Remove jobs you no longer need so their entries expire (`history-job-expire-minutes`) and the
  corresponding IMap entries stop being written.

WAL files for **finished jobs** can be safely compacted or removed after the corresponding IMap
entries have been flushed to MapStore files. Do not delete WAL files for running jobs.

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
| Remove from `engine_finishedJobState` IMap | Yes |
| Remove from `engine_finishedJobMetrics` IMap | Yes |
| Keep the job removed after a cluster restart (IMap persistence enabled) | Yes (the expiration time is persisted with the entry, so an expired job is not loaded again and a job that has not expired yet keeps its remaining time) |
| Delete MapStore persistence files for that job | **No** (the expired records stay in the IMap storage files) |
| Delete checkpoint storage directories | **No** |
| Delete savepoint directories | **No** |
| Delete WAL files | **No** (only indirectly via compaction) |

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
with:

```bash
du -sh $SEATUNNEL_HOME/imap/wal/
watch -n 60 'du -sh /tmp/seatunnel/imap/'
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

**Symptom**: the map-store `namespace` directory (e.g. `/tmp/seatunnel/imap/`) fills the disk on
master/worker nodes.

**Diagnosis**:

```bash
du -sh /tmp/seatunnel/imap/
```

**Root cause candidates**:

- `engine_runningJobMetrics` updates at every checkpoint for every running job (this IMap is
  excluded from persistence by the engine's MapStore; check for custom map configurations that
  persist it)
- Many finished jobs whose IMap entries have not yet expired (`history-job-expire-minutes`)
- WAL files from long-running jobs are not being compacted

**Fix**:

- Adjust `history-job-expire-minutes` in `seatunnel.yaml` so finished job state is cleaned up
  earlier.
- Move the map-store `namespace` to a larger disk, or to HDFS (`storage.type: hdfs` with the
  corresponding `fs.defaultFS`), and restart the cluster.

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
