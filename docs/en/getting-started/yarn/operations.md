---
sidebar_position: 4
---

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

# YARN Operations

## Check the service and registered Workers

Run these commands from the submission host, using the Master address configured for your service.

```bash
yarn app -status zeta-session
curl -fsS http://master-0.zeta-session.etl.example.net:8080/overview
```

The YARN service status lists the application ID, component instances and container hosts. In the Zeta response, `workers` counts Workers that have joined the cluster. For the single-Worker example, wait for `workers` to reach `1` before submitting a job.

If YARN allocates a Worker but the count stays at zero, check the Worker log, verify that the container can resolve and reach the Master on port `5801`, and compare the Hazelcast cluster name in the Master and Worker configurations.

## Read container logs

With YARN log aggregation enabled, set `APP_ID` to the YARN application ID from the status output for the service run you want to inspect.

```bash
yarn logs -applicationId "$APP_ID"
```

To include Zeta engine messages in container output, enable the stdout and stderr console appender references in `config/log4j2.properties` before packaging SeaTunnel.

## Scale Workers

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

Before planned downtime, let batch jobs finish and take a savepoint for any streaming job you intend to resume. Stop the service to release its containers.

```bash
yarn app -stop zeta-session
```

Start the saved service again and check Worker registration before submitting or restoring jobs.

```bash
yarn app -start zeta-session
yarn app -status zeta-session
```

The example has one Master and configures HDFS for checkpoints. To recover running jobs automatically after a full cluster outage, the Master also needs a durable IMap MapStore for job metadata. For planned restarts, keep the job ID and configuration for restoring each savepoint. [State storage and recovery](../../engines/zeta/state-storage-and-recovery.md)
