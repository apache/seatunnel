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
  -pl seatunnel-dist -am -DskipTests package
```

The result is `seatunnel-dist/target/apache-seatunnel-<version>-bin.tar.gz`.

Add every connector, format, transform, and driver required by the job before submission.

## 2. Check the cluster and choose the job

Run these checks as the submitting user. `HADOOP_CONF_DIR` should contain this cluster's `core-site.xml`, `hdfs-site.xml` and `yarn-site.xml`. On a Kerberos cluster, obtain a ticket with `kinit` and inspect it with `klist`, or configure a submitter keytab. See [Kerberos](configuration.md#kerberos).

```bash
export HADOOP_CONF_DIR=/etc/hadoop/conf
yarn node -list
yarn queue -status default
hdfs dfs -mkdir -p .seatunnel/applications
hdfs dfs -ls -d .seatunnel/applications
```

Choose a queue with room for the AM and the requested workers. A relative HDFS staging path resolves under the submitter's working directory. If you use a shared release, check that this identity can read the archive.

The distribution template includes a FakeSource-to-Console job in `yarn.local-resources.job.content`. Edit it directly or supply your own local or HDFS path in `yarn.job-config-file`. To submit a separate job with `-c job.conf`, remove the demo resource group and its `job-config-file` setting.

## 3. Create a deployment configuration

Copy the distribution template and edit it for the target cluster:

```bash
cp config/v1.yarn.conf.template yarn-deployment.conf
```

Keep the template's job block and set the distribution and Hadoop configuration paths for your cluster.

```hocon
yarn.distribution = "/opt/packages/apache-seatunnel-<version>-bin.tar.gz"
yarn.config-dir = "/etc/hadoop/conf"
```

Set `yarn.distribution` to a `.tar.gz`, `.tgz`, or `.zip` archive containing `starter/seatunnel-starter.jar`. Local paths and ordinary HDFS URIs such as `hdfs://nameservice1/releases/seatunnel.tgz` use an application-private copy. To use a release in place, supply a named snapshot on the staging HDFS, such as `hdfs://nameservice1/releases/.snapshot/v1/seatunnel.tgz`. Keep the snapshot readable for the lifetime of the application.

To open the Web UI during a job, add `yarn.master.http-port = 8095` to the deployment file. Choose an available port on an eligible AM node. The source build above includes the page assets. A clean build with `-Dskip.ui=true` serves the REST API only.

## 4. Submit and wait

From the extracted SeaTunnel directory:

```bash
bin/seatunnel-application.sh submit --target yarn \
  --application-config yarn-deployment.conf --wait
```

The command prints the YARN application ID and native Zeta job ID. Keep the application ID for status and cancellation. Keep the Zeta job ID for checkpoint recovery.

Remove `--wait` to exit the client after application startup. Closing the client does not cancel the remote job.

To submit a job already on HDFS, pass its URI with `yarn.job-config-file`. The Hadoop client loads cluster settings from `HADOOP_CONF_DIR` in this example. Both the deployment file and `-c` are optional with these overrides.

```bash
bin/seatunnel-application.sh submit --target yarn \
  -Dyarn.distribution=hdfs://nameservice1/releases/seatunnel.tgz \
  -Dyarn.job-config-file=hdfs://nameservice1/jobs/orders.conf --wait
```

## 5. Inspect or cancel

The template runs a short batch job. Expect `SUCCEEDED` and Console output in the worker logs. For a long-running job, query its status from another terminal. If you enabled the AM web port, open `http://<AM-host>:8095/` (or `https://<AM-host>:8095/` with engine TLS enabled) while the AM is running. Use `yarn logs -applicationId "$APP_ID"` to inspect aggregated logs after completion.

```bash
bin/seatunnel-application.sh status --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf

bin/seatunnel-application.sh cancel --target yarn \
  --id application_0000000000000_0001 \
  --application-config yarn-deployment.conf
```

Use the same Hadoop configuration and `yarn.staging-dir` for status and cancellation. A command-line `-Dkey=value` overrides deployment configuration, for example `-Dapplication.worker-count=3`.

Continue with [Commands](commands.md), [Configuration](configuration.md), [Checkpoint recovery](checkpoint-recovery.md), and [FAQ](faq.md).
