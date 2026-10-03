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

# Upgrade Compatibility Checks

This directory contains the scripts and scenario files used by the
`Upgrade Compatibility` GitHub Actions workflow.

The workflow verifies that a savepoint written by the previous stable SeaTunnel
release can be restored by the current `dev` distribution. A passing run means
the current code can read the selected scenario's checkpoint, source state and
sink state from the older release and finish the restored job.

## Current Scenarios

`generic-fake-localfile` starts a streaming `FakeSource` job on the old release,
writes through the transactional `LocalFile` sink, creates a savepoint while the
job is still running, starts the current `dev` distribution with the same
checkpoint storage, restores the same job id, and verifies the restored output
with the `Assert` sink.

`mysql-cdc-multitable-localfile` starts a MySQL 8 container, seeds two source
tables, starts a multi-table `MySQL-CDC` job on the old release, writes through
the transactional `LocalFile` sink, creates a savepoint, restores the same job
id on the current `dev` distribution, and verifies the restored output with the
`Assert` sink.

The generic scenario is intentionally service-free. The MySQL CDC scenario keeps
its Docker setup local to the scenario so database-specific setup does not leak
into the shared runner.

## Running Locally

Build the current distribution first:

```shell
./mvnw -B -T 1 package -DskipTests -DskipIT=true -Dlicense.skipAddThirdParty=true -Dskip.ui=true -pl seatunnel-dist -am
```

Then run the default scenario:

```shell
OLD_SEATUNNEL_VERSION=2.3.13 SCENARIO=generic-fake-localfile \
  bash tools/upgrade_compatibility/run_upgrade_compatibility.sh
```

To run the MySQL CDC scenario:

```shell
OLD_SEATUNNEL_VERSION=2.3.13 SCENARIO=mysql-cdc-multitable-localfile \
  bash tools/upgrade_compatibility/run_upgrade_compatibility.sh
```

Run the focused runner tests with:

```shell
bash tools/upgrade_compatibility/run_upgrade_compatibility_test.sh
```

## Adding Scenarios

Add a new directory under `scenarios/` with these files:

- `seatunnel.yaml`: engine configuration template. Use `__CHECKPOINT_DIR__`
  for the shared checkpoint namespace.
- `job.conf`: streaming job template. Use `__SINK_DIR__` for the output path.
- `assert.conf`: batch assertion job template. Use `__SINK_DIR__` for the
  output path.
- `plugin_config`: connector artifact ids required by the old release.
- `setup.sh` and `teardown.sh`: optional executable hooks for external services
  or extra dependency setup.
- `endless`: optional marker file for streaming sources that should be canceled
  after savepoint instead of waiting for natural job completion.

CDC scenarios should keep their database setup local to the scenario or a
dedicated helper script, and should avoid broadening the generic runner unless
the same hook is useful for more than one scenario.

## Compatibility Contract

The upgrade compatibility workflow asserts the following guarantee:

> A savepoint created by SeaTunnel version N-1 (latest stable release) can be
> restored by the current `dev` branch build, and the job will resume processing
> and produce correct output as verified by the Assert sink.

For the full design document, scenario-selection strategy, trigger policy, and
failure-classification rules, see the
[Upgrade Compatibility STIP](../../docs/en/design/upgrade-compatibility-stip.md).
