<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Ingres JDBC instance validation

`JdbcIngresIT` is skipped by default. It uses an existing, licensed Ingres instance
and a locally supplied driver. It does not download a database image, accept a
commercial license, or add the driver as a Maven dependency.

If you need an instance, request an Ingres evaluation from Actian.
Obtain an evaluation license from Actian before activating the instance.

Prerequisites:

- Docker capable of running SeaTunnel E2E tests, with the engine and connectors built.
- An Ingres JDBC URL reachable from both the test JVM and the engine container.
- A test account with CREATE TABLE, INSERT, SELECT, DROP TABLE, and metadata access for the specified owner.
- A user-supplied `iijdbc.jar` obtained from Actian. `com.ingres.jdbc:iijdbc:12.1-4.6.5` is a candidate driver, not a verified version. Record the actual server and driver versions with the results.

Set these environment variables instead of putting the password on the command line:

| Variable | Value |
| --- | --- |
| `INGRES_E2E` | `true` to explicitly enable the test |
| `INGRES_E2E_URL` | For example, `jdbc:ingres://ingres.example.com:II7/testdb` |
| `INGRES_E2E_USER` | Test account |
| `INGRES_E2E_PASSWORD` | Test password |
| `INGRES_E2E_OWNER` | Existing owner, in its actual stored case, without SQL quotes or dots |
| `INGRES_E2E_DRIVER_JAR` | Absolute path to the local `iijdbc.jar` |
| `RUN_ENGINES` | `zeta` |

Run from the repository root:

```shell
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-jdbc-e2e/connector-jdbc-e2e-part-7 -am \
  -DskipUT -DskipIT=false -Dit.test=JdbcIngresIT -Dfailsafe.failIfNoSpecifiedTests=false verify
```

The test creates a randomly named `st_ingres_*` table and inserts two rows. JDBC
Source discovers the owner-qualified table metadata and reads each row separately
by primary key. The Assert sink checks the row count, primary-key value, NULL and
field values. Data includes an integer key, a quoted mixed-case `Order` column,
a NULL string and `DECIMAL(10,2)` values.

Cleanup drops only the table created by this test. If the process is forcibly
terminated, use the test logs to identify and remove that test table manually.
Each job uses a complete temporary configuration generated from this test's own
template, with no includes. It is copied directly into the Zeta container with
owner-only permissions and deleted after the test, including when job submission
fails. The driver is copied into the test container only for the test.

This path tests standard JDBC Source reads. It does not validate Ingres JDBC Sink
execution, native upsert, Ingres-specific types, CDC, catalog or connector-managed
schema creation. A skipped test is not evidence of real-server compatibility.
