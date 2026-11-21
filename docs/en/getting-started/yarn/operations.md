---
sidebar_position: 6
---

# YARN Operations

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

## Check the service and registered Workers

Check the YARN service and Zeta HTTP endpoint from the submission host. For the [hybrid cluster](hybrid-cluster-mode.md), use its combined member's endpoint.

```bash
yarn app -status zeta-hybrid
curl -fsS http://hybrid-0.zeta-hybrid.etl.example.net:8080/overview
```

For the [separated cluster](separated-cluster-mode.md), use the Master endpoint.

```bash
yarn app -status zeta-session
curl -fsS http://master-0.zeta-session.etl.example.net:8080/overview
```

The YARN service status lists the application ID, component instances and container hosts. In the Zeta response, `workers` counts Workers that have joined the cluster. Each example starts one member that executes tasks. Wait for `workers` to reach `1` before submitting a job.

In separated mode, if YARN allocates a Worker but the count stays at zero, check the Worker log, verify that the container can resolve and reach the Master on port `5801`, and compare the Hazelcast cluster name in the Master and Worker configurations.

## Read container logs

With YARN log aggregation enabled, set `APP_ID` to the YARN application ID from the status output for the service run you want to inspect.

```bash
yarn logs -applicationId "$APP_ID"
```

To include Zeta engine messages in container output, enable the stdout and stderr console appender references in `config/log4j2.properties` before packaging SeaTunnel.

## Scale cluster components

### Scale hybrid members

In hybrid mode, scaling the `hybrid` component changes execution capacity, membership in Master elections and IMap partition placement together. Place members with the example's fixed ports (`5801` and `8080`) on distinct hosts. Before removing a member, check that the remaining members can host the configured backups and run the jobs.

After configuring placement and discovery for two hybrid members, increase the component size and wait for `/overview` to report two Workers.

```bash
yarn app -flex zeta-hybrid -component hybrid 2
```

### Scale Workers in separated mode

The example uses fixed ports `5802` and `8081` for every Worker. With host networking, configure YARN placement to keep Workers on separate NodeManager hosts before scaling. Check that the queue has enough memory and vcores for the additional container.

```bash
yarn app -flex zeta-session -component worker 2
```

This sets the desired Worker count to two. Check `/overview` for `workers` equal to `2` before submitting jobs that need the additional capacity.

To return to one Worker, wait for batch jobs to finish or stop affected streaming jobs with a savepoint. Verify the remaining Worker has enough slots before reducing capacity.

```bash
yarn app -flex zeta-session -component worker 1
```

## Stop and restart the service

Before planned downtime, let batch jobs finish and take a savepoint for any streaming job you intend to resume. Stop the service to release its containers. The commands below use the separated service name. For hybrid mode, replace `zeta-session` with `zeta-hybrid`.

```bash
yarn app -stop zeta-session
```

Start the saved service again and check Worker registration before submitting or restoring jobs.

```bash
yarn app -start zeta-session
yarn app -status zeta-session
```

Both examples store checkpoints in HDFS. To recover running jobs automatically after a full cluster outage, also [configure a durable IMap MapStore](configuration.md) on the members that store job metadata. For planned restarts, keep the job ID and configuration for restoring each savepoint. [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)
