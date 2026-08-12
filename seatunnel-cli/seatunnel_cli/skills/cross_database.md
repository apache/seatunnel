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

---
name: cross_database
description: Cross-database synchronization between different database systems
triggers:
  - cross database
  - database to database
  - db to db
  - mysql to postgresql
  - mysql to postgres
  - postgres to mysql
  - oracle to mysql
  - sqlserver to postgresql
  - migrate database
  - heterogeneous
tools:
  - get_connector_info
  - list_connectors
composable: true
---

## When to Use

User wants to sync data between two different database systems (e.g., MySQL → PostgreSQL,
Oracle → MySQL, SQL Server → PostgreSQL). The key challenge is handling different JDBC
drivers, URL formats, credential sets, and potential type mapping differences.

## Domain Knowledge

- Cross-database sync typically uses Jdbc connector for both source and sink.
- Source and sink need SEPARATE credentials — use different `${ENV_VAR}` names (e.g., `${SOURCE_DB_USER}` vs `${SINK_DB_USER}`).
- JDBC URL formats differ by database:
  - MySQL: `jdbc:mysql://<host>:<port>/<database>`
  - PostgreSQL: `jdbc:postgresql://<host>:<port>/<database>`
  - Oracle: `jdbc:oracle:thin:@<host>:<port>:<sid>`
  - SQL Server: `jdbc:sqlserver://<host>:<port>;databaseName=<database>`
- JDBC drivers:
  - MySQL: `com.mysql.cj.jdbc.Driver`
  - PostgreSQL: `org.postgresql.Driver`
  - Oracle: `oracle.jdbc.driver.OracleDriver`
  - SQL Server: `com.microsoft.sqlserver.jdbc.SQLServerDriver`
- `generate_sink_sql = true` on Jdbc sink enables auto DDL — SeaTunnel creates the target table if it doesn't exist.
- Data type mapping is handled automatically by SeaTunnel's internal type system, but edge cases (e.g., Oracle NUMBER, SQL Server NVARCHAR) may need attention.

## SOP

1. **Identify source and sink database types** from the user request.
2. **Resolve JDBC details** for both: driver class, URL template, default port.
3. **Use separate credential env vars**: `${SOURCE_DB_USER}` / `${SOURCE_DB_PASSWORD}` for source, `${SINK_DB_USER}` / `${SINK_DB_PASSWORD}` for sink.
4. **Fill source Jdbc options**: url, driver, user, password, query or table_path.
5. **Fill sink Jdbc options**: url, driver, user, password, database, table, `generate_sink_sql = true`.
6. **Set routing** via plugin_output/plugin_input.
7. **Apply golden example** for Jdbc→Jdbc if available.
8. **Validate**: both JDBC URLs well-formed, drivers correct for each DB type, credentials separated.

## Constraints

- NEVER use the same `${DB_USER}` for both source and sink — they are different systems with different credentials.
- ALWAYS include the correct JDBC driver for each database type.
- ALWAYS use the correct JDBC URL format for each database type.
- Do NOT assume source and sink use the same port or host.

## Pattern

```hocon
env {
  parallelism = <parallelism>
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "<source_jdbc_url>"
    driver = "<source_driver>"
    user = "${SOURCE_DB_USER}"
    password = "${SOURCE_DB_PASSWORD}"
    query = "SELECT * FROM <table>"
    plugin_output = "<routing_label>"
  }
}

sink {
  Jdbc {
    url = "<sink_jdbc_url>"
    driver = "<sink_driver>"
    user = "${SINK_DB_USER}"
    password = "${SINK_DB_PASSWORD}"
    database = "<sink_database>"
    table = "<table>"
    generate_sink_sql = true
    plugin_input = "<routing_label>"
  }
}
```
