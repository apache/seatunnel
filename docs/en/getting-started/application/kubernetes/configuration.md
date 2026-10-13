---
sidebar_position: 4
title: Configuration
---

# Kubernetes configuration

Application Mode reads two files on the submitting host: the deployment file (`-a`) with
`application.*` and `kubernetes.*` options, and the job file (`-c`). This page is the reference for
the deployment file; see [Quick start](quick-start.md) for the first submission and
[Checkpoint recovery](checkpoint-recovery.md) for restore behaviour.

## Configuration sources

| Source | Configures | Notes |
| --- | --- | --- |
| Deployment file (`-a`) | `application.*` and `kubernetes.*` | Resolved on the submitting host. Put sensitive values here, not on the command line. |
| Option overrides (`-i key=value`) | Any deployment option | Resolved on the submitter too; command-line arguments may be visible to other users. |
| Job file (`-c`) | The job itself | Carried to the master verbatim, including unresolved `${VAR}` placeholders. |
| Pod environment | `${VAR}` values in the job | Built from `kubernetes.env`, `kubernetes.env-from.*` and mounted Secret/ConfigMap files on the master. |

:::caution
Command-line overrides (`-i key=value`) may be visible to other users on the submitting host. Keep
`kubernetes.kubeconfig` and other sensitive values in a permission-restricted deployment file.
:::

## Option reference

### Kubernetes options

| Option | Default | Description |
| --- | --- | --- |
| `kubernetes.namespace` | `default` | Existing namespace containing all application resources. |
| `kubernetes.image` | Required | Image containing SeaTunnel, the provider, and job plugins. |
| `kubernetes.image-pull-policy` | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `kubernetes.image-pull-secrets` | Empty | Comma-separated existing Secret names used to pull images from private registries. |
| `kubernetes.service-account` | `default` | Existing ServiceAccount used by the master. |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | Absolute distribution path in the image, without `:`. |
| `kubernetes.configmap.<entry>` | Unset | ConfigMap mount entry: `ref` references an existing ConfigMap and mounts declared keys through `subPath`, while `data` entries are written into a ConfigMap SeaTunnel creates and owns. An optional `name` overrides the default `<application-id>-<entry>` so other options can reference it. |
| `kubernetes.secret-ref` | Unset | Existing Secret mounted read-only at `/etc/seatunnel-secrets/application`; never created or deleted. Point `kubernetes.job-config-file` at `<secret-directory>/job.conf` when it carries the job configuration. |
| `kubernetes.secret.name` | Application Job name | Name of the Secret SeaTunnel creates from `kubernetes.secret.data.*` entries. |
| `kubernetes.secret.data.<KEY>` | Unset | Key/value entry of the application Secret SeaTunnel creates; the Secret is owned by the Job and removed on cancellation. |
| `kubernetes.secret-directory` | `/etc/seatunnel-secrets/application` | Absolute directory where the configured application Secret is mounted read-only; set `kubernetes.job-config-file` to a file there when the Secret carries the job configuration. |
| `kubernetes.job-config-file` | Unset | Optional job configuration file that replaces the inline job configuration before parsing: a file name under `<seatunnel-home>/config` (ConfigMap-friendly) or an absolute path such as `<secret-directory>/job.conf`. Unset keeps the submitted `-c` file. |
| `kubernetes.env.<NAME>` | Unset | One environment variable per name: a literal value, `secret:<name>/<key>` or `configmap:<name>/<key>`. |
| `kubernetes.env-from.secret` / `kubernetes.env-from.configmap` | Unset | Existing Secret/ConfigMap whose keys are all injected as environment variables (Kubernetes `envFrom`). |
| `kubernetes.kubeconfig` | SDK default | Local submitter kubeconfig; it is not sent to the master. Configure it in a permission-restricted deployment file rather than with `-i`. |
| `kubernetes.finished-job-retention-seconds` | `86400` | Positive retention period for completed or failed Jobs. |
| `kubernetes.checkpoint-pvc` | Unset | Existing PVC mounted at `kubernetes.checkpoint-directory` in the master. |
| `kubernetes.checkpoint-directory` | `/opt/seatunnel/checkpoints` | Absolute container path where the checkpoint PVC is mounted on the master. |
| `kubernetes.master.jvm-options-file` | `jvm_master_options` | Master JVM options file: a file name under `<seatunnel-home>/config` or an absolute path, applied through the Java `@argfile` syntax; `none` disables it. |
| `kubernetes.worker.jvm-options-file` | `jvm_worker_options` | Worker JVM options file, using the same forms as the master option. |
| `kubernetes.jvm-options-file` | `jvm_options` | Common JVM options file applied to master and workers before the role-specific file; `none` disables it. |
| `kubernetes.log4j-config-file` | `log4j2_client.properties` | Log4j2 configuration passed as `-Dlog4j2.configurationFile`: a file name under `<seatunnel-home>/config`, an absolute path, or `none`. The default keeps console logs; set `log4j2.properties` for the file-based server configuration. |
| `kubernetes.seatunnel-config-file` | `config/seatunnel.yaml` | Engine configuration passed as `-Dseatunnel.config`: a path relative to `<seatunnel-home>`, an absolute `.yaml` path, or `none` to disable the flag. |
| `kubernetes.master.labels` | Empty | Additional master Pod labels as comma-separated `key:value` pairs. SeaTunnel ownership labels are reserved. |
| `kubernetes.worker.labels` | Empty | Additional worker Pod labels as comma-separated `key:value` pairs. SeaTunnel ownership labels are reserved. |
| `kubernetes.master.annotations` | Empty | Master Pod annotations as comma-separated `key:value` pairs. |
| `kubernetes.worker.annotations` | Empty | Worker Pod annotations as comma-separated `key:value` pairs. |
| `kubernetes.master.node-selector` | Empty | Master Pod node selector as comma-separated `key:value` pairs. |
| `kubernetes.worker.node-selector` | Empty | Worker Pod node selector as comma-separated `key:value` pairs. |
| `kubernetes.master.tolerations` | Empty | Master Pod tolerations as `key[=value][:Effect]` entries separated by `;`, for example `dedicated=seatunnel:NoSchedule`. |
| `kubernetes.worker.tolerations` | Empty | Worker Pod tolerations, using the same syntax as the master option. |

### Application options

| Option | Default | Description |
| --- | --- | --- |
| `application.name` | `seatunnel` | Readable prefix of the unique Job name. |
| `application.job-id` | Generated | Positive native Zeta job ID. |
| `application.restore-job-id` | Unset | Historical job ID whose latest eligible checkpoint initializes this execution. |
| `application.worker-count` | `1` | Fixed number of worker Pods. |
| `application.worker.memory-mb` | `1024` | Worker memory request and limit in MiB. |
| `application.worker.cpu-cores` | `1` | Worker CPU request and limit. |
| `application.worker.slots` | `2` | Fixed execution slots per worker. |
| `application.master.memory-mb` | `1024` | Master memory request and limit in MiB. |
| `application.master.cpu-cores` | `1` | Master CPU request and limit. |
| `application.master.port` | `5801` | Master Hazelcast TCP port. |
| `application.startup-timeout-millis` | `120000` | Applied to worker creation and registration on the master and to client connection setup; submission does not wait for master startup. |

:::note
Pod JVM heap uses 75% of the memory limit, leaving the remainder for off-heap allocations and JVM
overhead.
:::

:::note
`application.startup-timeout-millis` is not a job execution limit and does not bound submission.
The submitter returns as soon as the API accepts the started Job, so slow image pulls, admission,
or scheduling problems never fail the submit command; observe them with `status` and Pod events.
The master uses the same timeout separately for workers.
:::

## Guides

### Images and registry credentials

Use `kubernetes.image-pull-secrets` for existing private-registry Secrets.

:::note
The provider does not create those credentials or expose an arbitrary Pod template. Local files and
plugin JARs referenced by a job are not downloaded dynamically and must already be present in the
image.
:::

### Scheduling

Use `kubernetes.master.node-selector` / `kubernetes.worker.node-selector` to target a node pool,
and the matching `kubernetes.*.tolerations` to tolerate its taints. Workers are created by the
master at runtime, so worker tolerations travel inside the localized specification:

```hocon
kubernetes {
  master.node-selector = "pool:seatunnel"
  worker.node-selector = "pool:seatunnel"
  master.tolerations = "dedicated=seatunnel:NoSchedule"
  worker.tolerations = "dedicated=seatunnel:NoSchedule;spot:NoExecute"
}
```

Entries are `key[=value][:Effect]` separated by `;`: a key without a value uses the `Exists`
operator, and an explicit effect must be `NoSchedule`, `PreferNoSchedule` or `NoExecute`. Without
tolerations, the Pods stay `Pending` on a tainted node pool and report `had untolerated taint`
events.

### Job configuration

`-c job.conf` is read verbatim on the submitting host and carried inside the localized
specification; the submitter only parse-checks it with placeholders left unresolved. When the
master starts, the file optionally selected by `kubernetes.job-config-file` replaces that content
before the job is parsed. The option defaults to `null`, so nothing is mounted or read unless it is
configured:

1. unset (the default) or empty: the submitted `-c` file is always used;
2. set and a file exists at the resolved path - a name under `<seatunnel-home>/config` or an
   absolute path a Secret or ConfigMap key is mounted at: that mounted file is used;
3. set but the file is missing: the master logs a warning and falls back to `-c`.

The surviving text is parsed exactly once on the master, where `${VAR}` is resolved against the Pod
environment, so the submitting host never bakes its own values into the job.

**ConfigMap (recommended).** Point `kubernetes.job-config-file` at a relative name and mount that
key under `<seatunnel-home>/config`; no Secret permission is involved:

```hocon
kubernetes {
  job-config-file = job.conf
  configmap.job-conf {
    ref = platform-job-config
    mount {
      job.conf = /opt/seatunnel/config/job.conf
    }
  }
}
```

**Secret.** Keep the Secret mount and select the mounted file explicitly:

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  job-config-file = /etc/seatunnel-secrets/application/job.conf
}
```

:::tip
Values written directly into the deployment file are resolved on the submitting host, but `${VAR}`
inside a quoted (or triple-quoted) value is carried literally, which lets created Secret or
ConfigMap data hold a job file with master-side placeholders.
:::

### Secrets

An application Secret has two mutually exclusive modes. Both mount the Secret read-only at
`kubernetes.secret-directory` (default `/etc/seatunnel-secrets/application`) in the master and
worker Pods. Point `kubernetes.job-config-file` at `<secret-directory>/job.conf` when the Secret
carries the job configuration.

| Mode | Configuration | Lifecycle |
| --- | --- | --- |
| Reference | `kubernetes.secret-ref` | Created and managed by your platform; SeaTunnel never creates or deletes it. |
| Create | `kubernetes.secret.name` / `kubernetes.secret.data.*` | Created and owned by SeaTunnel; carries the Job owner reference and is removed with the application. |

**Reference an existing Secret.** SeaTunnel only mounts it. Inject its keys as Pod environment
variables when the job configuration uses `${VAR}` placeholders:

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  env.MYSQL_PASSWORD = "secret:platform-job-secrets/password"
}
```

**Let SeaTunnel create the Secret.** `data` entries are resolved on the submitting host and written
into an Opaque Secret, whose default name is the application ID and can be overridden with `name`.
Inject the created keys through `kubernetes.env`:

```hocon
kubernetes {
  secret {
    name = seatunnel-job-secrets
    data {
      MYSQL_PASSWORD = ${MYSQL_PASSWORD}   # resolved on the submitting host
      S3_SECRET_KEY = "s3-secret-key"
    }
  }
  env {
    MYSQL_PASSWORD = "secret:seatunnel-job-secrets/MYSQL_PASSWORD"
    S3_SECRET_KEY = "secret:seatunnel-job-secrets/S3_SECRET_KEY"
  }
}
```

A created Secret can also carry the job file; select it explicitly because the default path points
at `<seatunnel-home>/config`:

```hocon
kubernetes {
  secret {
    name = seatunnel-job-secrets
    data {
      job.conf = """
        env {
          job.mode = "BATCH"
          parallelism = 2
        }
        source {
          FakeSource {
            row.num = 10
            schema { fields { id = int } }
          }
        }
        sink {
          Console {}
        }
        """
    }
  }
  secret-directory = /etc/seatunnel-secrets/application
  job-config-file = /etc/seatunnel-secrets/application/job.conf
}
```

:::caution
Keep connector credentials out of ConfigMaps: they are not confidential. Use a Secret with
`kubernetes.env`/`${VAR}` when the job needs them.
:::

### ConfigMaps and runtime files

Every `kubernetes.configmap.<entry>` entry mounts one or more files through `subPath` in both the
master and worker Pods. Each mounted key must declare its full container path in `mount`, so
SeaTunnel never assumes a configuration directory.

| Mode | Configuration | Lifecycle |
| --- | --- | --- |
| Reference | `ref` + `mount` | Created and managed by your platform; SeaTunnel only mounts it and validates the referenced keys before submission. |
| Create | `name` (optional) + `data` + `mount` | Created and owned by SeaTunnel as `name` or `<application-id>-<entry>`; carries the Job owner reference and is removed with the application. |

**Mount files from existing ConfigMaps.** Use one entry per ConfigMap to mount files from different
ConfigMaps at different paths:

```hocon
kubernetes.configmap {
  engine-config {
    ref = platform-engine-config
    mount {
      seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
    }
  }
  logging-config {
    ref = platform-logging-config
    mount {
      log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
    }
  }
}
```

**Let SeaTunnel create every runtime file.** `data` accepts YAML, properties and plain text; write
each file as a triple-quoted value and mount it at the path the container reads. The example below
replaces the engine configuration, both Hazelcast member configurations, logging, JVM options and
the job file:

```hocon
kubernetes {
  job-config-file = job.conf
  configmap {
    runtime {
      name = seatunnel-runtime
      data {
        seatunnel.yaml = """
          seatunnel:
            engine:
              classloader-cache-mode: true
              checkpoint:
                interval: 10000
                timeout: 60000
          """
        hazelcast-master.yaml = """
          hazelcast:
            cluster-name: seatunnel
            network:
              port:
                auto-increment: false
                port: 5801
          """
        hazelcast-worker.yaml = """
          hazelcast:
            cluster-name: seatunnel
            network:
              port:
                auto-increment: true
                port: 5802
          """
        log4j2_client.properties = """
          rootLogger.level = INFO
          rootLogger.appenderRef.console.ref = console
          appender.console.type = Console
          appender.console.name = console
          """
        jvm_options = """
          -XX:+UseG1GC
          -XX:MaxMetaspaceSize=2g
          """
        job.conf = """
          env {
            job.mode = "BATCH"
            parallelism = 2
          }
          source {
            FakeSource {
              plugin_output = "example"
              row.num = 10
              schema { fields { id = int } }
            }
          }
          sink {
            Console {
              plugin_input = "example"
            }
          }
          """
      }
      mount {
        seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
        hazelcast-master.yaml = /opt/seatunnel/config/hazelcast-master.yaml
        hazelcast-worker.yaml = /opt/seatunnel/config/hazelcast-worker.yaml
        log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
        jvm_options = /opt/seatunnel/config/jvm_options
        job.conf = /opt/seatunnel/config/job.conf
      }
    }
  }
  master.hazelcast-config = hazelcast-master.yaml
  worker.hazelcast-config = hazelcast-worker.yaml
}
```

:::note
Referenced ConfigMaps are never modified or deleted; their mounted keys are validated before
submission. When the submitting identity cannot read a referenced ConfigMap, validation is skipped
with a warning and the Pod still mounts it through its own service account.
:::

:::note
Created ConfigMaps are named `name` or `<application-id>-<entry>`, carry the Job owner reference and
disappear with the application. Set `name` when another option must reference the ConfigMap.
:::

:::tip
Every key is mounted individually through `subPath`, so a ConfigMap never hides the other files the
image ships in the same directory. Referenced keys and created keys may not share a mount path.
:::

Runtime file options pick the mounted paths: `kubernetes.seatunnel-config-file` (engine
configuration), `kubernetes.master.hazelcast-config` / `kubernetes.worker.hazelcast-config`,
`kubernetes.jvm-options-file` plus the role-specific files, `kubernetes.log4j-config-file` and
`kubernetes.job-config-file`.

### Environment variables

`kubernetes.env.<NAME>` injects one variable per name; the value is a literal, a Secret key
reference `secret:<name>/<key>` or a ConfigMap key reference `configmap:<name>/<key>`.
`kubernetes.env-from.secret` / `kubernetes.env-from.configmap` inject every key of an existing
object at once:

```hocon
kubernetes {
  env {
    JOB_MODE = "BATCH"                                               # literal value
    MYSQL_PASSWORD = "secret:platform-job-secrets/password"          # secretKeyRef
    ENGINE_FILE = "configmap:platform-engine-config/seatunnel.yaml"  # configMapKeyRef
  }
  env-from.secret = platform-extra-secrets
}
```

Injected values reach the master Pod environment, where `${VAR}` placeholders in the job
configuration - inline or mounted - are resolved.

:::note
An explicit `env.<NAME>` wins over a duplicate key from `env-from`, and Kubernetes skips `envFrom`
keys that are not valid environment variable names.
:::

### JVM options and logging

Application mode passes the distribution JVM option files to the JVM through the Java `@argfile`
syntax: the common `jvm_options` first, then `jvm_master_options` in the master container and
`jvm_worker_options` in worker containers. Mount replacements through `kubernetes.configmap` and
they take effect without any other change:

```hocon
kubernetes.configmap.jvm-options {
  ref = platform-jvm-options
  mount {
    jvm_master_options = /opt/seatunnel/config/jvm_master_options
    jvm_worker_options = /opt/seatunnel/config/jvm_worker_options
  }
}
```

Set `kubernetes.master.jvm-options-file` or `kubernetes.worker.jvm-options-file` to an absolute
path for a different location, or to `none` to skip the file.

:::note
The mandatory Java 11+ module flags (`--add-opens` / `--add-exports`) used by Hazelcast and the
Kerberos configuration reload are always appended, so a mounted file only adds or overrides
options.
:::

Logging uses `kubernetes.log4j-config-file` (default `log4j2_client.properties`, console appenders
so `kubectl logs` stays readable). Set it to `log4j2.properties` to use the distribution's
file-based server configuration, and mount that file at the configured path; logs then go to
`<seatunnel-home>/logs`.

## Complete example

```hocon
application {
  name = "example-batch"
  worker-count = 2
  master.memory-mb = 1024
  master.cpu-cores = 1
  worker.memory-mb = 2048
  worker.cpu-cores = 2
  worker.slots = 4
  startup-timeout-millis = 180000
}

kubernetes {
  namespace = seatunnel
  image = "registry.example.com/seatunnel/application:3.0.1"
  image-pull-policy = IfNotPresent
  service-account = seatunnel-application
  job-config-file = job.conf

  # Platform-managed runtime files; job.conf is selected explicitly above.
  configmap {
    runtime {
      ref = platform-runtime-config
      mount {
        seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
        log4j2_client.properties = /opt/seatunnel/config/log4j2_client.properties
        hazelcast-master.yaml = /opt/seatunnel/config/hazelcast-master.yaml
        hazelcast-worker.yaml = /opt/seatunnel/config/hazelcast-worker.yaml
        jvm_options = /opt/seatunnel/config/jvm_options
        job.conf = /opt/seatunnel/config/job.conf
      }
    }
  }

  # Credentials stay in a Secret; the job references them with ${VAR}.
  secret-ref = platform-job-secrets
  env {
    MYSQL_PASSWORD = "secret:platform-job-secrets/password"
  }

  master.hazelcast-config = hazelcast-master.yaml
  worker.hazelcast-config = hazelcast-worker.yaml
  checkpoint-pvc = seatunnel-checkpoints
  finished-job-retention-seconds = 86400
}
```

```bash
bin/seatunnel-application.sh submit --target kubernetes -c job.conf -a kubernetes.conf
```

## Capacity planning

:::caution
Pod requests equal limits. Plan worker count, worker slots, job parallelism, ResourceQuota,
LimitRange, and node capacity together. The master does not provide task slots.
:::
