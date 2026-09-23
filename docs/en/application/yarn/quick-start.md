---
sidebar_position: 3
title: Quick Start
---

# YARN quick start

## 1. Prepare the distribution

### Use a released distribution

Download and extract a binary distribution that includes Application Mode from the [SeaTunnel download page](https://seatunnel.apache.org/download/). A released distribution already contains the runtime code, so Maven is not required on the submission host.

Confirm that the distribution contains:

```text
apache-seatunnel-<version>/
├── bin/seatunnel-application.sh
├── config/
├── starter/seatunnel-starter.jar
├── lib/
├── connectors/
└── resource-managers/yarn/
```

### Build from source

To test unreleased functionality or local code changes, run from the repository root:

```bash
./mvnw -Prelease,seatunnel \
  -pl seatunnel-dist -am -DskipTests -Dskip.ui=true package
```

The result is `seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz`.

Add every connector, format, transform, and driver required by the job before submission.

## 2. Create a job configuration

Save as `job.conf`:

```hocon
env {
  job.mode = "BATCH"
  parallelism = 2
}
source {
  FakeSource {
    row.num = 10
    split.num = 2
    schema { fields { id = int, name = string } }
  }
}
sink { Console {} }
```

## 3. Create a deployment configuration

Copy the distribution template and edit it for the target cluster:

```bash
cp config/v1.yarn.conf.template yarn-deployment.conf
```

The resulting `yarn-deployment.conf` should contain:

```hocon
application {
  name = "example-yarn-application"
  worker-count = 2
  worker.memory-mb = 1024
  worker.cpu-cores = 1
  worker.slots = 2
  master.memory-mb = 1024
  master.cpu-cores = 1
  startup-timeout-millis = 120000
}
yarn {
  distribution = "/opt/packages/apache-seatunnel-<version>-bin.tar.gz"
  config-dir = "/etc/hadoop/conf"
  staging-dir = "hdfs:///user/seatunnel/applications"
  queue = "default"
}
```

`yarn.distribution` is a local archive readable by the submission host. The provider accepts `.tar.gz`, `.tgz`, and `.zip` and locates the directory containing `starter/seatunnel-starter.jar` automatically.

## 4. Submit and wait

From the extracted SeaTunnel directory:

```bash
bin/seatunnel-application.sh submit --target yarn \
  --config job.conf --deployment-config yarn-deployment.conf --wait
```

The command prints the YARN application ID and native Zeta job ID. Keep the application ID for status and cancellation. Keep the Zeta job ID for checkpoint recovery.

Remove `--wait` to exit the client after application startup. Closing the client does not cancel the remote job.

## 5. Inspect or cancel

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --deployment-config yarn-deployment.conf
```

Use the same Hadoop configuration and `yarn.staging-dir` for status and cancellation. A command-line `-Dkey=value` overrides deployment configuration, for example `-Dapplication.worker-count=3`.

Continue with [Configuration](configuration.md), [Checkpoint recovery](checkpoint-recovery.md), and [FAQ](faq.md).
