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
| Savepoint | User-triggered named checkpoint for planned stop/restart | Same checkpoint storage, under the job's own directory |
| IMap / MapStore | Distributed in-memory state (job metadata, job state, history) | In memory; optionally persisted through the Hazelcast MapStore configured in `hazelcast.yaml` |

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
      interval: 10000                 # milliseconds between checkpoints
      timeout: 60000                  # checkpoint completion timeout (ms)
      storage:
        type: hdfs                    # hdfs (also supports S3 / local file via the HDFS API) | localfile (deprecated)
        plugin-config:
          namespace: /seatunnel/checkpoint/    # must end with /
          # For S3:
          # fs.s3a.endpoint: https://s3.amazonaws.com
          # fs.s3a.access.key: <your-access-key>
          # fs.s3a.secret.key: <your-secret-key>
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
| Trigger | Periodic / automatic | Manual (`$SEATUNNEL_HOME/bin/seatunnel.sh --savepoint <jobId>`) |
| Purpose | Fault tolerance | Planned stop, upgrade, migration |
| Lifecycle | Managed by engine | Managed by operator |
| Retention | Auto-rotated | Kept until manually deleted |

### Triggering a savepoint

```bash
# Stop a running job and create a savepoint
$SEATUNNEL_HOME/bin/seatunnel.sh --savepoint <job-id>

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

A savepoint is a checkpoint of savepoint type: it is written to the same checkpoint storage
configured above, under the directory of the job it belongs to (`<namespace>/<job-id>/`). There is
no separate savepoint root directory.

### Safe cleanup

A savepoint can be deleted only if you are certain you will never restore that job from it.
Deleting an active savepoint mid-restore causes the job to fail with a "state not found" error.

---

## 3. IMap and MapStore (Hazelcast Distributed State)

### What IMap stores

SeaTunnel Engine uses Hazelcast IMap as its distributed in-memory key-value store. The main
logical maps used by the engine are:

| IMap Name | Content |
|---|---|
| `engine_runningJobInfo` | Submitted running job information (job id, job name, metrics snapshot) |
| `engine_runningJobState` | Current state machine status of running jobs and pipelines |
| `engine_stateTimestamps` | Timestamps of job/pipeline state transitions |
| `engine_finishedJobState` | Terminal state for completed, canceled, or failed jobs |
| `engine_finishedJobMetrics` | Final metrics snapshot after job termination |
| `engine_finishedJobVertexInfo` | Execution vertex information of finished jobs |

### MapStore (disk-backed persistence)

By default IMap data is memory-only (replicated across nodes according to the backup count). If all
nodes stop, the data is lost unless the MapStore persistence is enabled. When enabled, the Hazelcast
MapStore writes IMap entries to an external file system (HDFS, S3, or local files through the HDFS
API) so they survive full cluster restarts. This is **separate** from checkpoint storage.

The persistence is configured in `hazelcast.yaml` (see
[Separated Zeta Cluster Deployment](separated-cluster-deployment.md) for details):

```yaml
map:
  engine*:
    map-store:
      enabled: true
      initial-mode: EAGER
      factory-class-name: org.apache.seatunnel.engine.server.persistence.FileMapStoreFactory
      properties:
        type: hdfs
        namespace: /tmp/seatunnel/imap     # storage namespace (default /seatunnel-imap when unset)
        clusterName: seatunnel-cluster
        storage.type: hdfs
        fs.defaultFS: hdfs://localhost:9000
```

In separated cluster mode only the Master node stores IMap data, so this configuration takes effect
on the Master node.

### Relationship between IMap, MapStore, and Checkpoint

```
Checkpoint storage  <──────────────────────────────────────────>  Operator state (offsets, splits)
IMap / MapStore     <──────────────────────────────────────────>  Job/pipeline lifecycle state
```

They are **independent**. Deleting checkpoint storage does not affect IMap, and vice versa.
A job can be restarted from a checkpoint even if the IMap MapStore data is wiped, **but the job ID
and pipeline mapping must be re-submitted** because the running job state is lost.

---

## 4. MapStore File Maintenance

The IMap persistence writes each map as files under the configured `namespace`. In long-running
clusters these files keep growing while jobs keep running, because every state change of a running
job is written through the MapStore.

**Mitigation:**

- Let finished jobs expire: `history-job-expire-minutes` removes finished-job records from the
  IMaps, which stops them from being persisted again.
- Monitor the namespace directory on the master node:

```bash
du -sh /tmp/seatunnel/imap/   # replace with the namespace you configured in hazelcast.yaml
```

Do not delete MapStore files of running jobs. Files of finished jobs whose records have already
expired from the IMaps can be removed while the cluster is stopped.

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
| Remove records from the finished-job IMaps (`engine_finishedJobState`, `engine_finishedJobMetrics`) | Yes |
| Delete checkpoint storage directories | **No** |
| Delete savepoint data | **No** |

**Key takeaway**: `history-job-expire-minutes` only cleans up the job metadata stored in the
finished-job IMaps. Checkpoint and savepoint directories on HDFS / S3 / local are **never**
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

### IMap storage sizing

Each running CDC job stores approximately:

- 50–200 bytes per pipeline of job state
- 1–2 KB per pipeline per metrics flush

For a cluster running 100 CDC jobs this is in the order of a few MB of IMap data. The MapStore
namespace disk usage grows with the state transition history; plan a few GB per long-running
cluster and monitor it with:

```bash
du -sh /tmp/seatunnel/imap/   # your configured MapStore namespace
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

### MapStore namespace directory growing

**Symptom**: The configured MapStore `namespace` directory fills the disk on the master node.

**Diagnosis**:

```bash
du -sh /tmp/seatunnel/imap/   # replace with the namespace you configured in hazelcast.yaml
```

**Root cause candidates**:

- Finished jobs are not expiring (`history-job-expire-minutes` is not set or too large)
- Too many running jobs continuously updating their state and metrics

**Fix**:

Enable/tune `history-job-expire-minutes` in `seatunnel.yaml`, and clean up files of expired jobs
while the cluster is stopped if needed.

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

- [ ] Confirm the job is in `FINISHED`, `CANCELED`, or `FAILED` terminal state
- [ ] Confirm you will not restore from checkpoint or savepoint
- [ ] Confirm the job ID is not referenced in any monitoring or alerting rule
- [ ] For checkpoint storage: delete `<namespace>/<job-id>/` recursively
- [ ] For MapStore data: stop the cluster before deleting files of expired jobs

---

## See Also

- [Checkpoint Storage Configuration](checkpoint-storage.md)
- [REST API v2](rest-api-v2.md) — query job state and metrics via API
