---
sidebar_position: 5
title: Checkpoint Recovery
---

# YARN checkpoint recovery

Application Mode recovers by explicitly submitting a new YARN application. It does not automatically restart a failed ApplicationMaster or provide live master takeover.

## 1. Configure durable storage

Before the first submission, configure native checkpoint storage in the distribution's `config/seatunnel.yaml`. The checkpoint directory must be outside `yarn.staging-dir`.

HDFS example:

```yaml
seatunnel:
  engine:
    checkpoint:
      interval: 5000
      timeout: 60000
      storage:
        type: hdfs
        max-retained: 3
        plugin-config:
          storage.type: hdfs
          fs.defaultFS: hdfs://namenode:8020
          namespace: /seatunnel/checkpoints
```

Application Mode preserves the complete `plugin-config`. See [Checkpoint Storage](../../engines/zeta/checkpoint-storage.md) for OSS, S3, COS, HDFS nameservice, endpoint, and authentication settings. Include the required filesystem implementation and dependencies in the distribution.

## 2. Enable checkpoints in the job

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  checkpoint.retain-after-job-cancelled = true
}
```

Application Mode retains checkpoints after cancellation by default. Explicit `checkpoint.retain-after-job-cancelled = false` overrides that behavior and can prevent recovery.

## 3. Record the native job ID

Record the Zeta job ID printed during submission, or assign one explicitly:

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config streaming-job.conf --deployment-config yarn-deployment.conf \
  --job-id 10001
```

Wait for at least one completed checkpoint before stopping the original application or attempting recovery after failure.

## 4. Submit the recovery application

Use a new job ID and the historical ID as the restore source:

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config streaming-job.conf --deployment-config yarn-deployment.conf \
  --job-id 10002 --restore-from-checkpoint 10001 --wait
```

The new application loads the latest eligible checkpoint for Job `10001` and writes later checkpoints below Job `10002`. Missing checkpoints fail submission instead of silently starting over.

## Recovery requirements

- Stop the original application to avoid two executions writing to the same systems.
- Give the new application access to the same checkpoint storage and Hadoop configuration.
- Keep topology, connector versions, state serialization, and parallelism compatible.
- Use connectors that support checkpointing; replay and sink guarantees still depend on connector semantics.
- Keep checkpoints outside application staging.
