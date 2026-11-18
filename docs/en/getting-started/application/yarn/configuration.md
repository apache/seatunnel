---
sidebar_position: 5
title: Configuration
---

# YARN configuration

The deployment file (`-a`) configures the application and the YARN cluster. The job can come from `-c` or a local, HDFS or inline file selected with `yarn.job-config-file`. For a first submission, see the [quick start](quick-start.md).

## Configuration sources

| Source | Used by | Purpose |
| --- | --- | --- |
| `-a yarn.conf` | Submitter | HOCON deployment settings. Optional when overrides supply the required options. |
| `-Dkey=value` | Submitter | Overrides the deployment file. Each argument supplies one option. |
| `-c job.conf` or `yarn.job-config-file` | AM | Job content, including placeholders resolved from the AM environment. A configured `yarn.job-config-file` takes precedence. |
| `yarn.config-dir` or `HADOOP_CONF_DIR` | Hadoop clients | Cluster endpoints, HDFS nameservices and the authentication mode. The resolved Hadoop configuration is localized to both roles. |

## Option reference

### YARN options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `yarn.deployment-target` | Enum | No | `APPLICATION` | Deployment topology. The first release supports one isolated application per job. |
| `yarn.distribution` | String | On submit | — | Local path, `file:///` URI, or `hdfs://` URI to a readable `.tar.gz`, `.tgz`, or `.zip` distribution. |
| `yarn.config-dir` | String | No | Empty | Hadoop configuration directory. Empty uses `HADOOP_CONF_DIR`, then the Hadoop classpath. |
| `yarn.staging-dir` | String | No | `.seatunnel/applications` | Shared-filesystem staging root. A relative path normally resolves below the submitting user's HDFS home. |
| `yarn.queue` | String | No | `default` | YARN submission queue. |
| `yarn.priority` | Integer | No | `-1` | `-1` uses the cluster default. Set a nonnegative integer to request an application priority. |
| `yarn.tags` | String | No | Empty | Comma-separated YARN application tags. |
| `yarn.master.node-label` | String | No | Empty | Node-label expression for the ApplicationMaster. |
| `yarn.master.http-port` | Integer | No | Unset | Enables the AM Web UI and REST API on this exact port (`1`–`65535`). Omit for a headless application. |
| `yarn.worker.node-label` | String | No | Empty | Node-label expression for workers; empty inherits `yarn.master.node-label`. |
| `yarn.hadoop-user-name` | String | No | Effective Hadoop user's short name | User for YARN requests, HDFS staging, the AM and workers in simple mode. |
| `yarn.active-deadline-seconds` | Long | No | Queue policy | Positive application lifetime in seconds from submission, enforced by the ResourceManager. The queue can cap this value. |
| `yarn.job-config-file` | String | No | Unset | Local path, HDFS URI or declared localization path of the job. Only the AM receives the selected file. It takes precedence over `-c` and allows `submit` to omit `-c`. |
| `yarn.kerberos.principal` | String | No | Empty | Principal in the submitter's keytab. |
| `yarn.kerberos.keytab` | String | No | Empty | Local path to the submitter's keytab. |
| `yarn.env.<NAME>` | String | No | Unset | Literal environment value exported to the AM and workers. |
| `yarn.ship-files.<alias>` | String | No | Unset | Local path or HDFS URI of a file localized as `files/<alias>` in both roles. |
| `yarn.local-resources.<group>.source-dir` | String | No | Unset | Submitter-local or HDFS directory containing the files named in this group's `localize` block. |
| `yarn.local-resources.<group>.content.<file>` | String | No | Unset | UTF-8 contents of a generated file. Use HOCON triple quotes for multiline text. |
| `yarn.local-resources.<group>.localize.<file>` | String | For directory sources | `files/<file>` for inline content | Destination in the container working directory, under `files/`. Supports renaming and subdirectories. |
| `yarn.inline-files.<alias>` | String | No | Unset | Shorthand for generating a file at `files/<alias>` from literal text. |
| `yarn.jvm-options-file` | String | No | `jvm_options` | Common Java argument file, applied before the role-specific file. |
| `yarn.master.jvm-options-file` | String | No | `jvm_master_options` | Java argument file for the AM. |
| `yarn.worker.jvm-options-file` | String | No | `jvm_worker_options` | Java argument file for workers. |
| `yarn.master.hazelcast-config` | String | No | Engine default | Hazelcast member configuration for the AM (`.yaml`, `.yml`, or `.xml`). |
| `yarn.worker.hazelcast-config` | String | No | Engine default | Hazelcast member configuration for workers (`.yaml`, `.yml`, or `.xml`). |
| `yarn.log4j-config-file` | String | No | `log4j2_client.properties` | Log4j2 configuration for both roles. Container stdout, stderr and the default file log directory are available to YARN log aggregation. |
| `yarn.seatunnel-config-file` | String | No | `config/seatunnel.yaml` | Engine YAML shared by the AM and workers, including checkpoint storage settings. |

### Application options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `application.name` | String | No | `seatunnel` | YARN application display name. |
| `application.job-id` | Long | No | Generated | Positive native Zeta job ID. |
| `application.restore-job-id` | Long | No | Unset | Historical job ID whose latest eligible checkpoint initializes this execution. |
| `application.worker-count` | Integer | No | `1` | Fixed number of worker containers. |
| `application.worker.memory-mb` | Integer | No | `1024` | Container memory per worker in MiB. |
| `application.worker.cpu-cores` | Integer | No | `1` | Virtual CPU cores per worker. |
| `application.worker.slots` | Integer | No | `2` | Fixed execution slots per worker. |
| `application.master.memory-mb` | Integer | No | `1024` | ApplicationMaster container memory in MiB. |
| `application.master.cpu-cores` | Integer | No | `1` | ApplicationMaster virtual CPU cores. |
| `application.master.port` | Integer | No | `5801` | Starting Hazelcast port; a later free port is selected on collision. |
| `application.startup-timeout-millis` | Long | No | `120000` | Applied separately to AM startup and worker allocation and registration. |

## Distribution and staging

Use the original binary archive with its connectors and drivers. A path without a URI scheme resolves on the submission host. Local archives, ordinary HDFS paths and archives on another HDFS nameservice are copied into the application's private staging directory. The submitter inspects the archive there.

To reuse a release across submissions, point `yarn.distribution` at a named snapshot on the same HDFS filesystem as `yarn.staging-dir`, for example `hdfs://nameservice1/releases/.snapshot/v1/seatunnel.tgz`. The client inspects the snapshot in place and records its qualified path, size and modification time. NodeManagers use that snapshot for both the AM and workers. Keep the snapshot at that path and readable until all applications using it have finished.

| Distribution source | What the NodeManagers localize | Who retains the source |
| --- | --- | --- |
| Submitter-local file | Private archive under the application staging directory | Submitter |
| Ordinary HDFS file, including one on another nameservice | Private archive under the application staging directory | HDFS release owner |
| Named snapshot on the staging HDFS | The snapshot file recorded in the manifest | Snapshot owner, until all applications using it finish |

### Permissions

| Identity | Access required |
| --- | --- |
| Submitter | Submit to the selected queue, read the archive and source files, and create a directory below `yarn.staging-dir`. |
| ApplicationMaster | Register with the ResourceManager, request worker containers, read the staged resources and remove its own application directory on exit. |
| Container localization | Read the declared HDFS resources with the application's credentials. The selected job is localized only for the AM. |
| Recovery submission | Read checkpoints under the previous job ID and write checkpoints under the new job ID. |

The application staging directory has mode `0700`. Put reusable archives and durable checkpoints outside it. The release owner controls snapshot retention.

## Kerberos

Hadoop selects the authentication mode. In the default `simple` mode, the submitter, AM and workers use the effective Hadoop user's short name. For example, a gateway process running as `svc` with `HADOOP_USER_NAME=etl` submits and stages files as `etl`. Set `yarn.hadoop-user-name` to select another user for all three. Simple mode ignores Kerberos options, including keytab paths.

When the cluster uses `hadoop.security.authentication=kerberos`, configure the ResourceManager principal in `yarn.resourcemanager.principal`. Use an existing ticket from `kinit`, or put both of the following options in a deployment file on the submission host.

```hocon
yarn.kerberos.principal = "seatunnel@EXAMPLE.COM"
yarn.kerberos.keytab = "/etc/security/keytabs/seatunnel.keytab"
```

The submitter obtains HDFS delegation tokens for the staging filesystem. The ResourceManager can renew them up to the HDFS token lifetime. The AM forwards HDFS tokens to workers, keeping AM control tokens in its own process. The keytab stays on the submission host. Use the same identity for status and cancellation.

## Scheduling

Queue, priority and tags apply to the YARN application. The AM and workers can request different node labels. `yarn.active-deadline-seconds` requests a ResourceManager lifetime for the entire application, counted from submission and subject to queue policy. `application.startup-timeout-millis` bounds AM startup and worker registration. `--wait-timeout` only bounds how long the CLI polls.

Set the AM and worker labels separately when they run on different node populations. An omitted worker label inherits the AM label. With two workers that each request 2 GiB and one vCore, plus a 1 GiB AM with one vCore, the queue needs room for three containers, 5 GiB and three vCores before scheduler rounding.

```hocon
yarn {
  queue = analytics
  priority = 1
  tags = "orders,nightly"
  master.node-label = control
  worker.node-label = batch
}
```

## Master HTTP endpoint

```hocon
yarn.master.http-port = 8095
```

This setting enables HTTP on the AM and overrides the HTTP port in the localized `seatunnel.yaml`. The AM binds exactly that port. A bind conflict fails startup. Workers keep their HTTP listener disabled. `application.master.port` controls the separate Hazelcast connection used by workers.

The AM registers `http://<AM NodeManager host>:8095/` with YARN, including the engine's HTTP context path if configured. The existing engine settings for basic authentication and request limits apply. Open the AM address while the application is running to use the packaged UI, or request `/overview` for its REST overview.

The ResourceManager Web Application Proxy can forward requests under `/proxy/<applicationId>/`. The packaged UI requests assets under `/assets/` and APIs such as `/overview` at the origin root. For the full UI, use the AM directly or route a dedicated gateway origin to it. Set up the external address, port and access policy in your network.

## Job configuration

Use `-c job.conf` to carry a local job file in the private AM specification. With `yarn.job-config-file`, the submitter stages a local file, an HDFS file, or a file declared in `local-resources`. The selected job is localized as `job.conf` only for the AM. An inline job can use placeholders such as `${DATA_REGION}` inside a triple-quoted string. The AM resolves them when it parses the job.

## Local resources and runtime files

Give each group in `yarn.local-resources` a name such as `runtime` or `worker`. Use `source-dir` with `localize` to select individual files from one local or HDFS directory. Relative source directories resolve against the submitter's working directory. To generate files from the deployment configuration, use a group with `content`. Each key is a filename and each value is written verbatim as UTF-8.

A generated file defaults to `files/<filename>`. Add a `localize` entry to rename it or place it in a subdirectory, for example `files/runtime/engine.yaml`. Each destination belongs to one file. Filenames and target path components start with a letter or digit and may also contain dots, underscores and hyphens. Both roles receive these files except the selected job, which goes only to the AM. For a single file, `ship-files.<alias>` accepts a local path or HDFS URI and `inline-files.<alias>` accepts literal text.

| Reference in a runtime option | Resolution |
| --- | --- |
| `jvm_worker_options` | File under the distribution's `config/` directory for JVM, logging and Hazelcast settings |
| `config/seatunnel.yaml` | Engine YAML relative to the extracted distribution root |
| `files/runtime/engine.yaml` | Destination declared in `local-resources` and localized by the NodeManager |
| Absolute path | File installed at the same path on every node where the relevant role may run |

## Environment variables

```hocon
yarn.env {
  DATA_REGION = east
  JOB_PARALLELISM = "2"
}
```

Both JVMs receive these literal values. A job can use `${JOB_PARALLELISM}` in `env.parallelism`. Keep that placeholder inside a triple-quoted string when embedding a job in the deployment file. Hadoop supplies container identity, credentials and localization variables. In simple mode, select a Hadoop user with `yarn.hadoop-user-name`.

## JVM options, Hazelcast and logging

For JVM, Hazelcast and Log4j2 settings, a filename refers to the distribution's `config/` directory. Engine configuration paths are relative to the distribution root. Each setting also accepts a shipped `files/<alias>` or an absolute path available in every target container. The default JVM argument files are used when present in the archive. Set any JVM file option to `disabled` to skip that file.

The common JVM argument file is loaded first, followed by the file for the role. Put member configuration in `yarn.master.hazelcast-config` and `yarn.worker.hazelcast-config`. YARN supplies the cluster name and addresses at launch. For aggregated logs use `yarn logs -applicationId "$APP_ID"`.

## Complete example

Save the following as `yarn.conf`. The queue, node labels and HDFS paths should exist in your cluster.

```hocon
application {
  name = "orders-batch"
  worker-count = 2
  worker.slots = 4
}
yarn {
  distribution = "hdfs://nameservice1/releases/seatunnel.tgz"
  job-config-file = "hdfs://nameservice1/jobs/orders.conf"
  config-dir = "/etc/hadoop/conf"
  queue = "analytics"
  master.node-label = "batch"
  worker.node-label = "batch"
  active-deadline-seconds = 3600
  env {
    DATA_REGION = east
  }
  seatunnel-config-file = files/runtime/engine.yaml
  master.hazelcast-config = files/runtime/hazelcast-master.yaml
  worker.hazelcast-config = files/runtime/hazelcast-worker.yaml
  worker.jvm-options-file = files/worker.opts

  local-resources {
    runtime {
      source-dir = "hdfs://nameservice1/seatunnel/conf"
      localize {
        "seatunnel.yaml" = files/runtime/engine.yaml
        "hazelcast-master.yaml" = files/runtime/hazelcast-master.yaml
        "hazelcast-worker.yaml" = files/runtime/hazelcast-worker.yaml
      }
    }

    worker {
      content {
        "worker.opts" = "-Duser.timezone=UTC"
      }
    }
  }
}
```

```bash
bin/seatunnel-application.sh submit -t yarn -a yarn.conf --wait
```

## Capacity planning

Worker count multiplied by slots per worker is the fixed execution capacity. Size it for the task groups created by the sources, transforms and sinks. Each resource request must fit the queue limits and a NodeManager. The default JVM heap is 75% of container memory with a 64 MiB floor.
