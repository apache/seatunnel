---
sidebar_position: 4
title: Configuration
---

# Kubernetes configuration

## Kubernetes options

| Option | Default | Description |
| --- | --- | --- |
| `kubernetes.namespace` | `default` | Existing namespace containing all application resources. |
| `kubernetes.image` | Required | Image containing SeaTunnel, the provider, and job plugins. |
| `kubernetes.image-pull-policy` | `IfNotPresent` | `Always`, `IfNotPresent`, or `Never`. |
| `kubernetes.image-pull-secrets` | Empty | Comma-separated existing Secret names used to pull images from private registries. |
| `kubernetes.service-account` | `default` | Existing ServiceAccount used by the master. |
| `kubernetes.seatunnel-home` | `/opt/seatunnel` | Absolute distribution path in the image, without `:`. |
| `kubernetes.configmap.<entry>` | Unset | ConfigMap mount entry: `ref` references an existing ConfigMap and mounts declared keys through `subPath`, while `data` entries are written into a ConfigMap SeaTunnel creates and owns. An optional `name` overrides the default `<application-id>-<entry>` so other options can reference it. |
| `kubernetes.secret-ref` | Unset | Existing Secret mounted read-only at `/etc/seatunnel-secrets/application`; never created or deleted. A `job.conf` key replaces the inline job configuration. |
| `kubernetes.secret.name` | Application Job name | Name of the Secret SeaTunnel creates from `kubernetes.secret.data.*` entries. |
| `kubernetes.secret.data.<KEY>` | Unset | Key/value entry of the application Secret SeaTunnel creates; the Secret is owned by the Job and removed on cancellation. |
| `kubernetes.secret-directory` | `/etc/seatunnel-secrets/application` | Absolute directory where the configured application Secret is mounted read-only; a `job.conf` key there replaces the inline job configuration. |
| `kubernetes.env` | Empty | Comma-separated entries injected into master and worker containers: `NAME=value`, `NAME=secret:<name>/<key>` or `NAME=configmap:<name>/<key>`. |
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

## Shared application options

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

Pod JVM heap uses 75% of the memory limit, leaving the remainder for off-heap allocations and JVM overhead.

`application.startup-timeout-millis` is not a job execution limit and does not bound submission. The submitter returns as soon as the API accepts the started Job, so slow image pulls, admission, or scheduling problems never fail the submit command; observe them with `status` and Pod events. The master uses the same timeout separately for workers.

## Images and credentials

Use `kubernetes.image-pull-secrets` for existing private-registry Secrets. The provider does not create those credentials or expose an arbitrary Pod template. Local files and plugin JARs referenced by a job are not downloaded dynamically and must already be present in the image.

Use `kubernetes.configmap.<entry>` to place runtime configuration files such as `seatunnel.yaml` and `log4j2_client.properties` in the master and worker Pods. Each key is mounted read-only at the path its `mount` entry declares, so the remaining files the image ships under `<seatunnel-home>/config`, including `jvm_options`, stay in place.

By default the localized application specification, including the resolved job configuration, is carried in the master container environment, so submission requires no permission to create Secrets. `kubernetes.secret-ref` adds a read-only reference to an existing Secret and `kubernetes.secret.data.*` lets SeaTunnel create and own one; a `job.conf` key in either Secret replaces the inline job configuration. Referenced Secrets are never modified or deleted, while created Secrets carry the Job owner reference and disappear with the application. Keep connector credentials out of the inline specification and use Secrets managed by your platform.

Variables that the submitting host resolves are substituted before submission. Remaining `${VAR}` placeholders stay in the job configuration and are resolved on the master against the Pod environment, including values injected by `kubernetes.env` through `secretKeyRef` or `configMapKeyRef`. A placeholder that exists nowhere is reported when the master starts, not when the job is submitted.

### Application Secret

An application Secret has two mutually exclusive modes. Both mount the Secret read-only at
`kubernetes.secret-directory` (default `/etc/seatunnel-secrets/application`) in the master and
worker Pods, and a `job.conf` key in the Secret replaces the inline job configuration.

| Mode | Configuration | Lifecycle |
| --- | --- | --- |
| Reference | `kubernetes.secret-ref` | Created and managed by your platform; SeaTunnel never creates or deletes it. |
| Create | `kubernetes.secret.name` / `kubernetes.secret.data.*` | Created and owned by SeaTunnel; carries the Job owner reference and is removed with the application. |

**Reference an existing Secret.** SeaTunnel only mounts it. Inject its keys as Pod environment
variables when the job configuration uses `${VAR}` placeholders:

```hocon
kubernetes {
  secret-ref = platform-job-secrets
  env = "MYSQL_PASSWORD=secret:platform-job-secrets/password"
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
  env = "MYSQL_PASSWORD=secret:seatunnel-job-secrets/MYSQL_PASSWORD,S3_SECRET_KEY=secret:seatunnel-job-secrets/S3_SECRET_KEY"
}
```

- Configure exactly one mode. Leaving both unset creates and references nothing, and is not an
  error.
- The Secret namespace always follows `kubernetes.namespace`.
- Placeholders that the submitting host cannot resolve stay in the job configuration and are
  resolved on the master from the injected environment.

### Application ConfigMap

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

**Let SeaTunnel create a ConfigMap.** Set `name` when another option must reference the created
ConfigMap, for example an environment variable using `configMapKeyRef`; without `name` the
ConfigMap is named `<application-id>-<entry>`:

```hocon
kubernetes {
  configmap.runtime {
    name = seatunnel-runtime-config
    data {
      seatunnel.yaml = "engine { ... }"
    }
    mount {
      seatunnel.yaml = /opt/seatunnel/config/seatunnel.yaml
    }
  }
  env = "ENGINE_CONFIG=configmap:seatunnel-runtime-config/seatunnel.yaml"
}
```

- Referenced ConfigMaps are never modified or deleted; their mounted keys are validated before
  submission. When the submitting identity cannot read a referenced ConfigMap, validation is
  skipped with a warning and the Pod still mounts it through its own service account.
- Every key is mounted individually through `subPath`, so a ConfigMap never hides the other files
  the image ships in the same directory.
- `seatunnel.yaml` is loaded from `kubernetes.seatunnel-config-file` (default
  `config/seatunnel.yaml`, relative to `<seatunnel-home>`); mount a replacement at that path to
  change the engine configuration.

### JVM options

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
path for a different location, or to `none` to skip the file. The mandatory Java 11+ module flags
(`--add-opens` / `--add-exports`) used by Hazelcast and the Kerberos configuration reload are
always appended, so a mounted file only adds or overrides options.

Logging uses `kubernetes.log4j-config-file` (default `log4j2_client.properties`, console appenders
so `kubectl logs` stays readable). Set it to `log4j2.properties` to use the distribution's
file-based server configuration, and mount that file at the configured path; logs then go to
`<seatunnel-home>/logs`.

Use command-line `-ikey=value` only for non-sensitive overrides because command-line arguments may be visible to other users on the submission host. Put `kubernetes.kubeconfig` and other sensitive deployment values in a permission-restricted deployment configuration file, or rely on the Kubernetes SDK default credentials.

## Capacity planning

Pod requests equal limits. Plan worker count, worker slots, job parallelism, ResourceQuota, LimitRange, and node capacity together. The master does not provide task slots.
