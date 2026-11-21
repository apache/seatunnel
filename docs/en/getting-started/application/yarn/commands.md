---
sidebar_position: 4
title: Commands
---

# YARN application commands

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Use the application launcher to submit to a YARN queue, follow an application by its ResourceManager ID, or cancel it. The same Hadoop configuration and identity should be available to each command.

## Synopsis

```bash
bin/seatunnel-application.sh submit -t yarn [-a yarn.conf] [-c job.conf] [-Dkey=value ...] [--wait]
bin/seatunnel-application.sh status -t yarn --id APPLICATION_ID [-a yarn.conf] [--wait]
bin/seatunnel-application.sh cancel -t yarn --id APPLICATION_ID [-a yarn.conf]
```

## submit

The client registers the distribution, stages the configuration under the application ID and submits one AM. It prints both IDs once the AM is running or a fast batch job has already succeeded. The YARN ID identifies the application for status and cancellation. The Zeta job ID identifies its checkpoints.

```text
Application ID: application_1700000000000_0001
Job ID: 917381145928572929
```

Use `-c job.conf` for a submitter-local job. To select a job already on HDFS, supply `yarn.job-config-file`. With `HADOOP_CONF_DIR` pointing at the cluster, the following command also supplies the distribution through an override and needs no deployment file.

```bash
bin/seatunnel-application.sh submit -t yarn \
  -Dyarn.distribution=hdfs://nameservice1/releases/seatunnel.tgz \
  -Dyarn.job-config-file=hdfs://nameservice1/jobs/orders.conf
```

## status

Query the ResourceManager for one snapshot, or use `--wait` to follow the job to completion. `ACCEPTED` maps to `DEPLOYING`, `KILLED` maps to `CANCELED`, and `FINISHED` uses the final result reported by the AM. The `--wait-timeout` value is in milliseconds and bounds client polling. The complete exit-code contract is in the [shared CLI reference](../cli.md#status-and-waiting).

```bash
bin/seatunnel-application.sh status -t yarn --id "$APP_ID" -a yarn.conf --wait
```

With `--wait`, `SUCCEEDED` and `CANCELED` exit with code `0`, `FAILED` exits with `1`, and `UNKNOWN` exits with `2`. Set `--wait-timeout` to bound the client wait. A client timeout leaves the YARN application running.

## cancel

Cancellation asks YARN to kill the application and cleans up its staging directory. The ResourceManager retains the application report according to cluster policy. Once the report expires, SeaTunnel returns `UNKNOWN`. A later submission can restore a retained checkpoint by Zeta job ID.

```bash
bin/seatunnel-application.sh cancel -t yarn --id "$APP_ID" -a yarn.conf
```

## Inspecting in YARN

Set `APP_ID` to the ID printed by `submit`. Inspect the queue, state, diagnostics and tracking URL, list the application attempts, then retrieve aggregated container logs. For a specific attempt, `yarn container -list <attemptId>` lists its containers and their NodeManager addresses. If you enabled `yarn.master.http-port`, the AM's host and configured port also provide the direct UI and REST address while the job runs.

```bash
yarn application -status "$APP_ID"
yarn applicationattempt -list "$APP_ID"
yarn logs -applicationId "$APP_ID"
```
