---
sidebar_position: 4
title: Configuration
---

# YARN configuration

Deployment configuration uses HOCON. Resource values and timeouts must be positive. The master port must be between 1 and 65535.

## YARN options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `yarn.deployment-target` | Enum | `APPLICATION` | Deployment topology. The first release supports one isolated application per job. |
| `yarn.distribution` | String | Required | Local `.tar.gz`, `.tgz`, or `.zip` distribution readable by the submitter. |
| `yarn.config-dir` | String | Empty | Hadoop configuration directory. Empty uses `HADOOP_CONF_DIR`, then the Hadoop classpath. |
| `yarn.staging-dir` | String | `.seatunnel/applications` | Shared-filesystem staging root. A relative path normally resolves below the submitting user's HDFS home. |
| `yarn.queue` | String | `default` | YARN submission queue. |
| `yarn.priority` | Integer | `-1` | Application priority. A negative value keeps the cluster default. |
| `yarn.tags` | String | Empty | Comma-separated YARN application tags. |
| `yarn.master.node-label` | String | Empty | Node-label expression for the ApplicationMaster. |
| `yarn.worker.node-label` | String | Empty | Node-label expression for workers; empty inherits `yarn.master.node-label`. |

## Shared application options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `application.name` | String | `seatunnel` | YARN application display name. |
| `application.job-id` | Long | Generated | Positive native Zeta job ID. |
| `application.restore-job-id` | Long | Unset | Historical job ID whose latest eligible checkpoint initializes this execution. |
| `application.worker-count` | Integer | `1` | Fixed number of worker containers. |
| `application.worker.memory-mb` | Integer | `1024` | Container memory per worker in MiB. |
| `application.worker.cpu-cores` | Integer | `1` | Virtual CPU cores per worker. |
| `application.worker.slots` | Integer | `2` | Fixed execution slots per worker. |
| `application.master.memory-mb` | Integer | `1024` | ApplicationMaster container memory in MiB. |
| `application.master.cpu-cores` | Integer | `1` | ApplicationMaster virtual CPU cores. |
| `application.master.port` | Integer | `5801` | Starting Hazelcast port; a later free port is selected on collision. |
| `application.startup-timeout-millis` | Long | `120000` | Applied separately to AM startup and worker allocation and registration. |

Container JVM heap uses 75% of requested memory, leaving the remainder for off-heap memory and JVM overhead. The minimum request is 64 MiB.

`application.startup-timeout-millis` is not a job execution timeout. A streaming job may run indefinitely. The setting limits only AM startup and worker allocation and registration.

## Capacity planning

Plan worker count, slots per worker, and job parallelism together. Sources, transforms, and sinks may all create task groups, so `env.parallelism` alone is not a complete slot estimate. Requests must also fit YARN queue quotas and maximum container capabilities.

## Command-line overrides

Command-line `-Dkey=value` takes precedence:

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config job.conf --deployment-config yarn-deployment.conf \
  -Dapplication.worker-count=3 \
  -Dapplication.worker.slots=4
```
