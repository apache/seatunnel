import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

## Description

The JDBC Source connector reads tables or custom query results from databases through a JDBC driver. It supports column projection, row filtering, parallel snapshot reads, and reading multiple tables in one source configuration.

JDBC Source is a bounded source: it reads the rows visible to the database query and then finishes. Use a CDC connector instead when the job must continue capturing later inserts, updates, and deletes.

If this is your first JDBC Source job, start with [Choose a read mode](#choose-a-read-mode) and the [Quick start](#quick-start-postgresql). The complete option reference follows those sections.

## Using Dependency

Install the `connector-jdbc` plugin first:

```plugin_config
--seatunnel-connectors--
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
```

JDBC driver licenses and redistribution terms vary by database vendor, and the driver version must be compatible with both the database and the Java runtime. SeaTunnel therefore does not bundle every JDBC driver. Download the appropriate driver yourself and place its JAR in the engine-specific directory below before starting the job.

### Spark and Flink engines

Place the JDBC driver in `${SEATUNNEL_HOME}/plugins/Jdbc/lib/` on every node that runs SeaTunnel.

### Zeta engine

Place the JDBC driver in `${SEATUNNEL_HOME}/lib/` on every SeaTunnel node, then restart the affected SeaTunnel processes so the driver is loaded.

See the [driver reference](#driver-reference) for common driver class names and download locations.

## Choose a read mode

Choose a single-table or multi-table layout before configuring parallelism. In the single-table layout, `table_path` and `query` can be used separately or together. The multi-table `table_list` layout is mutually exclusive with top-level `table_path` and `query`.

| Use case | Configuration | Behavior |
|----------|---------------|----------|
| Read one table with automatic schema discovery and dynamic splitting | `table_path` | Recommended for a full-table snapshot. SeaTunnel reads table metadata and uses `split.size` when the table has a usable split key. |
| Control selected columns, joins, or database-side expressions | `query`, optionally with `table_path` | SeaTunnel executes the SQL you provide. Add `table_path` when explicit table identity and metadata are also needed. Query key inference is unsafe for some joins; see [Query and primary-key caution](#query-and-primary-key-caution). |
| Read multiple tables or table-name patterns | `table_list` | Each entry can define `table_path`, an optional `query`, and split settings. Top-level `table_path` and `query` cannot be used together with `table_list`. |

Use `where_condition` only for a common filter that should be added to every selected table or query. Its value must start with `where`, for example `where updated_at >= '2026-01-01'`.

## Quick start: PostgreSQL

This example reads three rows from PostgreSQL and prints them to the SeaTunnel log.

1. Put a compatible PostgreSQL JDBC driver in the directory described in [Using Dependency](#using-dependency).

2. As a PostgreSQL administrator, connect to an existing `sales` database, create a dedicated tutorial table, and grant a read-only account access. If `seatunnel_reader` already exists, omit the `CREATE ROLE` statement and reuse the account:

```sql
CREATE ROLE seatunnel_reader WITH LOGIN PASSWORD 'change_me';

DROP TABLE IF EXISTS public.seatunnel_jdbc_source_quick_start;

CREATE TABLE public.seatunnel_jdbc_source_quick_start (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);

INSERT INTO public.seatunnel_jdbc_source_quick_start VALUES
  (1, 'Alice', 120.50),
  (2, 'Bob', 80.00),
  (3, 'Carol', 42.00);

GRANT CONNECT ON DATABASE sales TO seatunnel_reader;
GRANT USAGE ON SCHEMA public TO seatunnel_reader;
GRANT SELECT ON TABLE public.seatunnel_jdbc_source_quick_start TO seatunnel_reader;
```

The `DROP TABLE` makes the tutorial data repeatable. Do not use that statement with a business table.

3. Save the following job as `${SEATUNNEL_HOME}/config/jdbc-source-quick-start.conf`. Replace the host, credentials, and database name for your environment.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://postgresql.example.com:5432/sales"
    driver = "org.postgresql.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM public.seatunnel_jdbc_source_quick_start ORDER BY id"
  }
}

sink {
  Console {}
}
```

4. Run the job:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-source-quick-start.conf -m local
```

5. Confirm that the Console sink prints rows with the following values:

| id | customer_name | amount |
|----|---------------|-------:|
| 1 | Alice | 120.50 |
| 2 | Bob | 80.00 |
| 3 | Carol | 42.00 |

If the job fails before reading rows, check [Troubleshooting](#troubleshooting) first.

:::note

When connecting to MariaDB, use MariaDB Connector/J with the matching URL and driver:

```hocon
url = "jdbc:mariadb://localhost:3306/database"
driver = "org.mariadb.jdbc.Driver"
```

Do not use MySQL Connector/J with a `jdbc:mysql:` URL for MariaDB. That configuration selects the MySQL dialect, which can reject a MariaDB server version as an unsupported MySQL version.

:::

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)

Use `query` to select only the required columns.

- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Options

`url` and `driver` are always required. Authentication is optional because some databases allow unauthenticated connections. Use either the top-level single-table layout or `table_list`; top-level `table_path` and `query` may be combined. `username` is the preferred account key, while legacy configurations using `user` remain supported as a fallback.

| name                                       | type    | required  | default value   | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|-----------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | Yes       | -               | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | Yes       | -               | The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| username                                   | String  | No        | -               | Database account name. The legacy key `user` is still accepted as a fallback.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| password                                   | String  | No        | -               | Password for the database account.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| query                                      | String  | No        | -               | SQL query to execute. It can be combined with `table_path` when explicit table identity and metadata are also needed.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| compatible_mode                            | String  | No        | -               | The compatible mode of database, required when the database supports multiple compatible modes.<br/> For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'. <br/> when using starrocks, you need set it to `starrocks`                                                                                                                                                                                                                                                                                                                                                                                             |
| dialect                                    | String  | No        | -               | The appointed dialect, if it does not exist, is still obtained according to the url, and the priority is higher than the url. <br/> For example,when using starrocks, you need set it to `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| connection_check_timeout_sec               | Int     | No        | 30              | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| connect_timeout_ms                         | Int     | No        | 86400000        | Connection timeout in milliseconds when establishing the JDBC connection. `0` means no timeout.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| socket_timeout_ms                          | Int     | No        | 86400000        | Socket read timeout in milliseconds after the JDBC connection is established. `0` means no timeout.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| partition_column                           | String  | No        | -               | The column name for split data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | String  | No        | -               | Inclusive upper value used for query-based partitioning. When omitted, SeaTunnel queries the source for the maximum value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| partition_lower_bound                      | String  | No        | -               | Inclusive lower value used for query-based partitioning. When omitted, SeaTunnel queries the source for the minimum value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| partition_num                              | Int     | No        | 10              | Number of splits when top-level `query` and `partition_column` select the fixed splitter, whether or not `table_path` is also present. Dynamic `table_path` and `table_list` layouts use `split.size` instead.                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| decimal_type_narrowing                     | Boolean | No        | true            | Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now. Please refer to `decimal_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| int_type_narrowing                         | Boolean | No        | true            | Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now. Please refer to `int_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| handle_blob_as_string                      | Boolean | No        | false           | If true, BLOB type will be converted to STRING type. **Only supported for Oracle database**. This is useful for handling large BLOB fields in Oracle that exceed the default size limit. When transmitting Oracle's BLOB fields to systems like Doris, setting this to true can make the data transfer more efficient.                                                                                                                                                                                                                                                                                                                             |
| use_kerberos                               | Boolean | No        | false           | Whether to enable Kerberos authentication for JDBC connections.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| kerberos_principal                         | String  | No        | -               | Kerberos principal used when `use_kerberos = true`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| kerberos_keytab_path                       | String  | No        | -               | Path of the Kerberos keytab file used when `use_kerberos = true`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| krb5_path                                  | String  | No        | /etc/krb5.conf  | Path of the Kerberos krb5 configuration file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| use_select_count                           | Boolean | No        | false           | Use select count for table count rather then other methods in dynamic chunk split stage. This is currently only available for jdbc-oracle.In this scenario, select count directly is used when it is faster to update statistics using sql from analysis table                                                                                                                                                                                                                                                                                                                                                                                     |
| skip_analyze                               | Boolean | No        | false           | Skip the analysis of table count in dynamic chunk split stage. This is currently only available for jdbc-oracle.In this scenario, you schedule analysis table sql to update related table statistics periodically or your table data does not change frequently                                                                                                                                                                                                                                                                                                                                                                                    |
| use_regex                                  | Boolean | No        | false           | Control regular expression matching for table_path. When set to `true`, the table_path will be treated as a regular expression pattern. When set to `false` or not specified, the table_path will be treated as an exact path (no regex matching). |
| fetch_size                                 | Int     | No        | 0               | For queries that return a large number of objects, you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.                                                                                                                                                                                                                                                                                                                                                                                               |
| properties                                 | Map     | No        | -               | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL.                                                                                                                                                                                                                                                                                                                                                                                                     |
| table_path                                 | String  | No        | -               | Full table path. It can be used alone or together with `query` in the single-table layout. <br/>Examples: <br/>`- mysql: "testdb.table1" `<br/>`- oracle: "test_schema.table1" `<br/>`- sqlserver: "testdb.test_schema.table1"` <br/>`- postgresql: "testdb.test_schema.table1"`  <br/>`- iris: "test_schema.table1"`                                                                                                                                                                                                                                                               |
| table_list                                 | Array   | No        | -               | The list of tables to be read, you can use this configuration instead of `table_path`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| where_condition                            | String  | No        | -               | Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.size                                 | Int     | No        | 8096            | Target rows per split when the dynamic splitter is used, including `table_path` and `table_list` layouts. It does not control the top-level fixed `query` plus `partition_column` mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| split.even-distribution.factor.lower-bound | Double  | No        | 0.05            | Not recommended for use.<br/> The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| split.even-distribution.factor.upper-bound | Double  | No        | 100             | Not recommended for use.<br/> The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| split.sample-sharding.threshold            | Int     | No        | 1000            | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                                                 |
| split.inverse-sampling.rate                | Int     | No        | 1000            | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                                                            |
| split.allow-sampling                       | Boolean | No        | true            | Whether to allow sampling-based sharding strategy. When set to false, the system will fall back to unevenly-sized chunk splitting (iterative query approach) regardless of the shard count.                                                                                                                                                              |
| enable_concurrent_read                     | Boolean | No        | true            | Whether to enable concurrent read with split during the snapshot phase. When set to false, the source skips split analysis and reads the table as a single split, which is useful for tables without indexes.                                                                                                                                           |
| common-options                             |         | No        | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| split.string_split_mode                    | String  | No        | sample          | Supports different string splitting algorithms. By default, `sample` is used to determine the split by sampling the string value. You can switch to `charset_based` to enable charset-based string splitting algorithm. When set to `charset_based`, the algorithm assumes characters of partition_column are within ASCII range 32-126, which covers most character-based splitting scenarios.                                                                                                                                                                                                                                                    |
| split.string-strategy                      | String  | No        | -               | Controls how String partition columns are split. Available values are `none`, `hash`, `range`, and `auto`. `range` and `auto` currently require MySQL binary collation and fixed-length printable ASCII key values. Other JDBC dialects reject `range` and `auto` until their range split support is explicitly validated. `auto` tries range splitting first and falls back to hash splitting when range splitting is unsafe. When this option is not set, SeaTunnel keeps the existing `split.string_split_mode` behavior.                                                                                                                                                                                                                                           |
| split.string_split_mode_collate            | String  | No        | -               | Specifies the collation to use when string_split_mode is set to `charset_based` and the table has a special collation. If not specified, the database's default collation will be used.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### Table Matching

Use the full table path expected by the database dialect:

| Database family | Example |
|-----------------|---------|
| MySQL | `sales.orders` |
| PostgreSQL and SQL Server | `sales.public.orders` |
| Oracle | `SALES.ORDERS` |

`use_regex = false` performs exact matching and is the safest default. Set `use_regex = true` only when the table part of `table_path` is intentionally a regular expression:

```text
table_path = "sales.orders_\\d+"
use_regex = true
```

In HOCON strings, a regular-expression backslash must be escaped, so `\d+` is written as `\\d+` in the file. The final unescaped dot separates the database/schema path from the table pattern.

Many JDBC drivers treat schema and table arguments passed to `DatabaseMetaData` as SQL `LIKE` patterns. SeaTunnel performs an exact identifier check after metadata discovery, but you should still use the exact case for case-sensitive database identifiers.

:::note Views and table matching

Whether `table_path` (with or without `use_regex`) also matches database views, not only base tables, depends on the dialect's internal table-listing query. There is no option to explicitly include or exclude views:

- MySQL and PostgreSQL list views alongside base tables, so a broad pattern like `db.*` also matches views.
- SQL Server filters to `TABLE_TYPE = 'BASE TABLE'` and excludes views.
- Oracle and Dameng query `ALL_TABLES`, which excludes views as a side effect of that view not listing them.

To read only specific base tables regardless of dialect, list them explicitly in `table_list` instead of relying on a broad regular expression.

:::

### decimal_type_narrowing

Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now.

eg:

decimal_type_narrowing = true

| Oracle        | SeaTunnel |
|---------------|-----------|
| NUMBER(1, 0)  | Boolean   |
| NUMBER(6, 0)  | INT       |
| NUMBER(10, 0) | BIGINT    |

decimal_type_narrowing = false

| Oracle        | SeaTunnel      |
|---------------|----------------|
| NUMBER(1, 0)  | Decimal(1, 0)  |
| NUMBER(6, 0)  | Decimal(6, 0)  |
| NUMBER(10, 0) | Decimal(10, 0) |

### int_type_narrowing

Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now.

eg:

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

### dialect [string]

The appointed dialect, if it does not exist, is still obtained according to the url, and the priority is higher than the url. For example,when using starrocks, you need set it to `starrocks`. Similarly, when using mysql, you need to set its value to `mysql`.

If one dialect not supported by SeaTunnel, it will use the default dialect `GenericDialect`. Just make sure the driver you provided support the database you want to connect.

#### dialect list

|           | Dialect Name |          |
|-----------|--------------|----------|
| Greenplum | DB2          | Dameng   |
| Gbase8a   | HIVE         | KingBase |
| MySQL     | StarRocks    | Oracle   |
| Phoenix   | Postgres     | Redshift |
| SapHana   | Snowflake    | Sqlite   |
| SqlServer | Tablestore   | Teradata |
| Vertica   | OceanBase    | XUGU     |
| IRIS      | Inceptor     | Highgo   |
| YashanDB  |              |          |

Dameng `NCHAR` source columns are mapped to SeaTunnel `STRING`.

## Parallel Reader

Parallelism determines how many readers can run at the same time; the split configuration determines how many independent splits are available.

### Recommended: `table_path` with dynamic splitting

For a full-table snapshot, configure `table_path` and normally leave split-key discovery to SeaTunnel. The connector uses `partition_column` when provided. Otherwise it searches the primary key and then unique indexes for the first supported column. String, numeric, and date columns are supported split-key types.

For tables with a **composite primary key** (`PRIMARY KEY (a, b, ...)`), the dynamic splitter splits on **all key columns** as a tuple (e.g. `(a, b) > (?, ?)`, emitted in a portable expanded `OR`/`AND` form), so tables whose first key column repeats heavily still split into balanced chunks via the remaining key columns. This applies when the dynamic splitter is active (the default), the dialect enables composite splitting, and all key columns are supported split-key types; otherwise the connector falls back to splitting on the first supported primary-key column as before. Composite splitting is currently enabled for **MySQL, PostgreSQL, SQLite, SQL Server, and Oracle** (Oracle requires 12c Release 1 or later because the boundary queries use the `FETCH FIRST`/`OFFSET` pagination syntax); other dialects keep the single-column behavior.

`split.size` is the target row count per split. It is not a hard row limit: actual split sizes depend on key distribution and database statistics. If no supported primary key, unique index, or explicit `partition_column` exists, the table is read by one split even when job parallelism is greater than one.

### Top-level `query` with fixed partitions

Only the top-level combination of `query` and `partition_column` selects the legacy fixed splitter; `partition_num` then controls the number of splits. The partition column must be present in the query result. Optional lower and upper bounds avoid extra `MIN`/`MAX` queries, but incorrect bounds can omit source rows, so use them only when the full data range is known.

Entries inside `table_list` continue to use dynamic splitting even when they include `query` or partition settings. Do not expect `split.size` to affect the top-level fixed partition mode.

### Query and primary-key caution

When SeaTunnel infers a key for `query`, it inherits metadata from the underlying table of the first result column. For joins or multi-table queries, that key is not guaranteed to be unique across the complete result set. Prefer a single-reader query or explicitly choose a partition column whose values divide the result safely.

## Driver reference

The following values are starting points. Confirm the driver artifact, license, database compatibility, and Java compatibility with the database vendor before deployment.

| datasource        | driver                                              | url                                                                    | maven                                                                                                                         |
|-------------------|-----------------------------------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| mysql             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| postgresql        | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| dm                | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| phoenix           | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| sqlserver         | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| oracle            | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlite            | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| gbase8a           | com.gbase.jdbc.Driver                               | jdbc:gbase://localhost:5258/test                                        | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                           |
| starrocks         | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| tablestore        | com.alicloud.openservices.tablestore.jdbc.OTSDriver | `jdbc:ots:https://<instance_name>.<region_id>.ots.aliyuncs.com/<instance_name>` | https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc                                           |
| saphana           | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| doris             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver           | jdbc&#58;snowflake://&lt;account_name&gt;.snowflakecomputing.com        | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                              |
| Redshift          | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb?defaultRowFetchSize=1000         | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Vertica           | com.vertica.jdbc.Driver                             | jdbc:vertica://localhost:5433                                          | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| Kingbase          | com.kingbase8.Driver                                | jdbc:kingbase8://localhost:54321/db_test                               | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| OceanBase         | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| Hive              | org.apache.hive.jdbc.HiveDriver                     | jdbc:hive2://localhost:10000                                           | https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar                                 |
| xugu              | com.xugu.cloudjdbc.Driver                           | jdbc:xugu://localhost:5138                                             | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver                    | jdbc:IRIS://localhost:1972/%SYS                                        | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |
| opengauss         | org.opengauss.Driver                                | jdbc:opengauss://localhost:5432/postgres                               | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar                              |
| Highgo            | com.highgo.jdbc.Driver                              | jdbc:highgo://localhost:5866/highgo                                    | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                                                   |
| Presto            | com.facebook.presto.jdbc.PrestoDriver               | jdbc:presto://localhost:8080/presto                                    | https://repo1.maven.org/maven2/com/facebook/presto/presto-jdbc/0.279/presto-jdbc-0.279.jar                                    |
| Trino             | io.trino.jdbc.TrinoDriver                           | jdbc:trino://localhost:8080/trino                                      | https://repo1.maven.org/maven2/io/trino/trino-jdbc/460/trino-jdbc-460.jar                                                     |
| YashanDB          | com.yashandb.jdbc.Driver                            | jdbc:yasdb://localhost:1688/SYS                                        | https://repo1.maven.org/maven2/com/yashandb/yashandb-jdbc/1.10.7/yashandb-jdbc-1.10.7.jar                                     |

## Common patterns

### Custom query

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM orders WHERE status = 'PAID'"
  }
}

sink {
  Console {}
}
```

### Oracle BLOB as STRING

Set `handle_blob_as_string = true` when Oracle BLOB values should be exposed as SeaTunnel STRING values, for example before writing them to Doris.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@oracle.example.com:1521/SERVICE_NAME"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT ID, NAME, CONTENT_BLOB FROM MY_TABLE"
    handle_blob_as_string = true  # Enable BLOB to String conversion for Oracle
  }
}

sink {
  Console {}
}
```

### Custom query partitioned by a column

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "seatunnel_reader"
        password = "change_me"
        query = "SELECT id, customer_name, amount FROM orders"
        partition_column = "id"
        partition_num = 10
    }
}

sink {
  Console {}
}
```

### Explicit partition boundaries

Specify bounds only when they cover the complete source range. SeaTunnel does not read values outside the configured interval.

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
    Jdbc {
        url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "seatunnel_reader"
        password = "change_me"
        query = "SELECT id, customer_name, amount FROM orders"
        partition_column = "id"
        partition_lower_bound = 1
        partition_upper_bound = 500
        partition_num = 10
    }
}

sink {
  Console {}
}
```

### Dynamic splitting by primary key or unique index

This example uses `table_path` without the top-level `query` plus `partition_column` combination, so it uses dynamic splitting. Start with the default split settings, then tune `split.size` only after measuring the source database load and job throughput.

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        username = "seatunnel_reader"
        password = "change_me"
        table_path = "sales.orders"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### Multiple tables

Use `table_list` when different tables need different queries or matching rules.

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    username = "seatunnel_reader"
    password = "change_me"

    table_list = [
        {
          table_path = "sales.orders"
        },
        {
          table_path = "sales.customers"
          query = "SELECT id, name FROM customers WHERE id > 100"
        },
        {
          table_path = "sales.archive_\\d+"
          use_regex = true
        }
    ]
  }
}

sink {
  Console {}
}
```

## Troubleshooting

### JDBC driver class cannot be found

Confirm that the driver JAR is in the engine-specific directory on every execution node, that the configured `driver` class exists in that JAR, and that the affected process was restarted after the JAR was added.

### Connection succeeds in a SQL client but fails in SeaTunnel

The hostname must be reachable from the SeaTunnel process, not only from your laptop. Check network routing, firewall rules, TLS settings, credentials, database name, and `connection_check_timeout_sec`. Do not copy an example hostname without replacing it.

### Table or columns cannot be discovered

Check the database-specific `table_path` format, identifier case, and the account's metadata and `SELECT` privileges. For a custom query, first execute the exact SQL with the same account in a database client.

### Parallelism does not increase the number of readers

For dynamic splitting, verify that the table has a supported primary key or unique index, or configure `partition_column`. To select the legacy fixed splitter for a top-level `query`, configure both `partition_column` and a suitable `partition_num`. A table without a safe split key is intentionally read by one split.

### Rows are missing after setting partition bounds

`partition_lower_bound` and `partition_upper_bound` define the range SeaTunnel reads; they are not only performance hints. Remove them to let SeaTunnel discover the bounds, or correct them so they include every required row.

## Changelog

<ChangeLog />
