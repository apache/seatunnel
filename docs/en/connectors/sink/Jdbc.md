import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

## Description

The JDBC Sink connector writes SeaTunnel rows to databases through a JDBC driver. It supports batch and streaming jobs, parallel writers, generated or custom SQL, multi-table writes, CDC events, and optional exactly-once delivery through XA transactions.

If this is your first JDBC Sink job, start with [Choose a write mode](#choose-a-write-mode) and the [Quick start](#quick-start-postgresql). The complete option reference follows those sections.

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

JDBC driver licenses and redistribution terms vary by database vendor, and the driver version must also be compatible with your database and Java runtime. SeaTunnel therefore does not bundle every JDBC driver. Download the appropriate driver yourself and place its JAR in the engine-specific directory below before starting the job.

### Spark and Flink engines

Place the JDBC driver in `${SEATUNNEL_HOME}/plugins/Jdbc/lib/` on every node that runs SeaTunnel.

### Zeta engine

Place the JDBC driver in `${SEATUNNEL_HOME}/lib/` on every SeaTunnel node, then restart the affected SeaTunnel processes so the driver is loaded.

See the [driver reference](#driver-reference) for common driver class names and download locations.

## Choose a write mode

JDBC Sink has two mutually exclusive write modes. Choose one before configuring the remaining options.

| Use case | Required configuration | Behavior |
|----------|------------------------|----------|
| SeaTunnel generates SQL | `generate_sink_sql = true`, `database`, and normally `table` | Recommended for most jobs. SeaTunnel can generate INSERT, database-native UPSERT, UPDATE, and DELETE statements from the upstream schema and row kind. Save modes and automatic table creation are available. |
| You provide SQL | `query = "INSERT ... VALUES (?, ...)"` | Use when the target statement must be fully controlled. The `?` parameters are bound in upstream field order. Save mode options are not executed in this mode. |

Do not configure both modes. `generate_sink_sql` defaults to `false`, so a job without `generate_sink_sql = true` must provide `query`.

For generated SQL, configure `primary_keys` when the target needs upsert, update, or delete behavior. If it is omitted, SeaTunnel tries to inherit a primary key, then the first unique key, from upstream catalog metadata. Without any usable key, generated SQL falls back to plain INSERT.

## Quick start: PostgreSQL

This beginner example uses generated SQL and a pre-created target table. The configuration and expected result below have been verified against PostgreSQL 14, including the inserted rows and their final database values.

1. Put a compatible PostgreSQL JDBC driver in the directory described in [Using Dependency](#using-dependency).

2. Create the target table:

```sql
CREATE TABLE public.orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);
```

3. Save the following job as `${SEATUNNEL_HOME}/config/jdbc-sink-quick-start.conf`. Replace the host, credentials, and database name for your environment.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 3
    schema = {
      fields {
        id = bigint
        customer_name = string
        amount = "decimal(10, 2)"
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice", 120.50] }
      { kind = INSERT, fields = [2, "Bob", 80.00] }
      { kind = INSERT, fields = [3, "Carol", 42.00] }
    ]
  }
}

sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/sales"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "change_me"
    generate_sink_sql = true
    database = "sales"
    table = "public.orders"
    primary_keys = ["id"]
    schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

4. Run the job:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-sink-quick-start.conf -m local
```

5. Verify the result:

```sql
SELECT id, customer_name, amount
FROM public.orders
ORDER BY id;
```

Expected rows:

| id | customer_name | amount |
|----|---------------|-------:|
| 1 | Alice | 120.50 |
| 2 | Bob | 80.00 |
| 3 | Carol | 42.00 |

If the job fails before writing, check [Troubleshooting](#troubleshooting) first.

:::note

When connecting to MariaDB, use MariaDB Connector/J with the matching URL and driver:

```hocon
url = "jdbc:mariadb://localhost:3306/database"
driver = "org.mariadb.jdbc.Driver"
```

Do not use MySQL Connector/J with a `jdbc:mysql:` URL for MariaDB. That configuration selects the MySQL dialect, which can reject a MariaDB server version as an unsupported MySQL version.

:::

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

Exactly-once delivery uses XA transactions and therefore requires XA support from both the database and its JDBC driver. See [Exactly-once prerequisites](#exactly-once-prerequisites).

- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md) (Zeta engine only)

## Options

`url`, `driver`, `schema_save_mode`, and `data_save_mode` are always required by the connector option rule. The two save mode options have defaults, so they can normally be omitted from job files. Other options become required according to the selected write mode:

- Generated SQL: set `generate_sink_sql = true` and configure `database`; configure `table` unless upstream metadata supplies the target table dynamically.
- Custom SQL: leave `generate_sink_sql = false` and configure `query`.
- Exactly-once: set `is_exactly_once = true`, `xa_data_source_class_name`, and `max_retries = 0`.

| Name                                      | Type    | Required | Default                      |
|-------------------------------------------|---------|----------|------------------------------|
| url                                       | String  | Yes      | -                            |
| driver                                    | String  | Yes      | -                            |
| username                                  | String  | No       | -                            |
| password                                  | String  | No       | -                            |
| query                                     | String  | No       | -                            |
| compatible_mode                           | String  | No       | -                            |
| dialect                                   | String  | No       | -                            | 
| database                                  | String  | No       | -                            |
| table                                     | String  | No       | -                            |
| tablePrefix                               | String  | No       | -                            |
| tableSuffix                               | String  | No       | -                            |
| primary_keys                              | Array   | No       | -                            |
| connection_check_timeout_sec              | Int     | No       | 30                           |
| connect_timeout_ms                        | Int     | No       | 86400000                     |
| socket_timeout_ms                         | Int     | No       | 86400000                     |
| max_retries                               | Int     | No       | 0                            |
| batch_size                                | Int     | No       | 1000                         |
| batch_interval_ms                         | Long    | No       | 0                            |
| is_exactly_once                           | Boolean | No       | false                        |
| generate_sink_sql                         | Boolean | No       | false                        |
| xa_data_source_class_name                 | String  | No       | -                            |
| max_commit_attempts                       | Int     | No       | 3                            |
| transaction_timeout_sec                   | Int     | No       | -1                           |
| auto_commit                               | Boolean | No       | true                         |
| field_ide                                 | String  | No       | -                            |
| properties                                | Map     | No       | -                            |
| common-options                            |         | No       | -                            |
| schema_save_mode                          | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode                            | Enum    | No       | APPEND_DATA                  |
| custom_sql                                | String  | No       | -                            |
| enable_upsert                             | Boolean | No       | true                         |
| is_primary_key_updated                    | Boolean | No       | true                         |
| support_upsert_by_insert_only             | Boolean | No       | false                        |
| table_options                             | Map     | No       | -                            |
| use_copy_statement                        | Boolean | No       | false                        |
| oracle_insert_mode                        | Enum    | No       | CONVENTIONAL                 |
| create_index                              | Boolean | No       | true                         |
| use_kerberos                              | Boolean | No       | false                        |
| kerberos_principal                        | String  | No       | -                            |
| kerberos_keytab_path                      | String  | No       | -                            |
| krb5_path                                 | String  | No       | /etc/krb5.conf               |
| access_key_id                             | String  | No       |                              |
| secret_access_key                         | String  | No       |                              |
| region                                    | String  | No       |                              |

### driver [string]

The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.

### username [string]

The database login name. `username` is the canonical option. The legacy key `user` is still accepted as a fallback when `username` is not set.

### password [string]

password

### url [string]

The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]

The parameterized SQL statement used to write each upstream row, for example `INSERT INTO target(id, name) VALUES (?, ?)`. SeaTunnel binds the `?` parameters in upstream field order. Use this option only in custom SQL mode; do not combine it with `generate_sink_sql = true`.

Current limitation: when sink `query` is configured (custom write SQL), JDBC sink does not apply save mode handling. `schema_save_mode`, `data_save_mode`, and `custom_sql` are not executed in this mode. If you need save mode handling, use `generate_sink_sql = true` with `database` and `table`.

### compatible_mode [string]

The compatible mode of database, required when the database supports multiple compatible modes.

For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'. when using StarRocks, you need set it to `starrocks`.

Postgres 9.5 version or below,please set it to `postgresLow` to support cdc

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
| DSQL      |              |          |
| YashanDB  |              |          |
### database [string]

The target database or catalog used in generated SQL mode. This option is required when `generate_sink_sql = true` and must not be combined with `query`.

### table [string]

The target table used in generated SQL mode. Do not combine it with `query`.

The table parameter can fill in the target table name, which will eventually be used as the created or written table name, and supports variables (`${table_name}`, `${schema_name}`). Replacement rules: `${schema_name}` will replace the SCHEMA name passed to the target side, and `${table_name}` will replace the table name passed to the target side.

mysql sink for example:

1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

pgsql (Oracle Sqlserver ...) Sink for example:

1. ${schema_name}.${table_name}_test
2. dbo.tt_${table_name}_sink
3. public.sink_table

Tip: If the target database has the concept of SCHEMA, the table parameter must be written as `xxx.xxx`

### tablePrefix [string]

Deprecated. Use `table` with table placeholders instead. For example, use `table = "prefix_${table_name}_suffix"` instead of configuring `tablePrefix` and `tableSuffix`.

### tableSuffix [string]

Deprecated. Use `table` with table placeholders instead. For example, use `table = "prefix_${table_name}_suffix"` instead of configuring `tablePrefix` and `tableSuffix`.

### primary_keys [array]

The target key columns used to generate database-native UPSERT, UPDATE, and DELETE statements. If omitted, SeaTunnel attempts to inherit a primary key or the first unique key from upstream catalog metadata. If no key is available, generated SQL uses plain INSERT.

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### connect_timeout_ms [int]

Connection timeout in milliseconds when establishing the JDBC connection. The default is 24 hours. Set it to `0` to disable the timeout.

### socket_timeout_ms [int]

Socket read timeout in milliseconds after the JDBC connection is established. The default is 24 hours. Set it to `0` to disable the timeout.

### max_retries [int]

The number of retries after a failed JDBC `executeBatch`. Exactly-once mode requires this option to be `0`; retrying a failed XA batch can violate transaction guarantees.

### batch_size [int]

The maximum number of buffered rows per batch. The sink flushes when the buffer reaches `batch_size`, when a checkpoint prepares a commit, or when the writer closes. A larger value can improve throughput but uses more memory and increases the amount of work retried after a failure.

### batch_interval_ms [long]

The flush interval in milliseconds. When set to a value greater than 0, if the elapsed time since the last flush exceeds this interval, the next `writeRecord` call will trigger a synchronous flush, even if `batch_size` has not been reached. Default value is `0` (disabled). This is a **write-triggered** time check, not a background timer — if no new records arrive (idle partition), no time-based flush occurs; buffered data is flushed at the next `prepareCommit` (checkpoint) or `close`. Note that when `auto_commit = false`, flushed rows are not visible to other transactions until the next commit (e.g. at checkpoint).

### is_exactly_once [boolean]

Enables exactly-once delivery through XA transactions. This requires `xa_data_source_class_name`, `max_retries = 0`, and XA support from the database and driver. Timer flush is not supported in this mode.

### generate_sink_sql [boolean]

When `true`, SeaTunnel generates write statements from the upstream schema and row kind. Configure `database` and normally `table`; do not configure `query`. The default is `false`, which means `query` is required.

### xa_data_source_class_name [string]

The xa data source class name of the database Driver, for example, mysql is `com.mysql.cj.jdbc.MysqlXADataSource`, and
please refer to appendix for other data sources

### max_commit_attempts [int]

The number of retries for transaction commit failures

### transaction_timeout_sec [int]

The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect
exactly-once semantics

### auto_commit [boolean]

Automatic transaction commit is enabled by default

### field_ide [String]

The field "field_ide" is used to identify whether the field needs to be converted to uppercase or lowercase when
synchronizing from the source to the sink. "ORIGINAL" indicates no conversion is needed, "UPPERCASE" indicates
conversion to uppercase, and "LOWERCASE" indicates conversion to lowercase.

### properties

Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

### schema_save_mode [Enum]

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.  
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode [Enum]

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.  
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`CUSTOM_PROCESSING`：User defined processing  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

### custom_sql [String]

When data_save_mode selects CUSTOM_PROCESSING, you should fill in the CUSTOM_SQL parameter. This parameter usually fills in a SQL that can be executed. SQL will be executed before synchronization tasks.

Note: in sink `query` mode, `custom_sql` is not executed. This behavior is a current limitation of JDBC sink.

### table_options [Map]

Sink-specific table options applied when SaveMode creates the target table (DDL phase). They take effect only when `schema_save_mode` triggers table creation, such as `CREATE_SCHEMA_WHEN_NOT_EXIST` or `RECREATE_SCHEMA`. They do **not** affect INSERT/UPSERT at runtime and do **not** run `ALTER TABLE` on existing tables.

Current support:

| Dialect | Supported | Allowed keys |
|---------|-----------|--------------|
| MySQL | Yes | `engine`, `charset`, `collate` |
| TiDB | Yes | `engine`, `charset`, `collate` (via MySQL JDBC protocol and `jdbc:mysql://`) |
| OceanBase (MySQL mode) | Yes | `engine`, `charset`, `collate` |
| PostgreSQL | Yes | `tablespace`, `fillfactor` |
| Dameng | Yes | `tablespace`, `fillfactor` |
| Oracle | Yes | `tablespace`, `pctfree` |
| OceanBase (Oracle mode) | Yes | `tablespace`, `pctfree` (via `compatible_mode=oracle` → Oracle dialect / DDL path) |
| Kingbase | Yes | `tablespace`, `fillfactor` |
| Other JDBC dialects | No | Non-empty `table_options` fails validation at job submission |

Invalid or unsupported keys are validated early via `JdbcSinkFactory` option rules (`--check` and job submission), not only at runtime DDL.

**Dialect notes:**

- **MySQL**: `engine`, `charset`, and `collate` are appended to `CREATE TABLE` and take effect.
- **TiDB**: When connected via `jdbc:mysql://` with a MySQL JDBC driver, TiDB shares the same key whitelist and DDL merge path as MySQL. `charset` and `collate` take effect; `engine` is accepted for MySQL syntax compatibility but is **ignored** by TiDB (storage engine is not configurable).
- **OceanBase (MySQL mode)**: Supported for `jdbc:oceanbase://` when not using Oracle-compatible mode. `charset` and `collate` must be values supported by your OceanBase version (typically a MySQL-compatible subset; use `SHOW CHARSET` / `SHOW COLLATION` on the target). Unsupported values fail when `CREATE TABLE` runs, not at job submission.
- **PostgreSQL**: `fillfactor` is emitted as `WITH (fillfactor=<n>)` and must be an integer in `[10, 100]`; `tablespace` is emitted as `TABLESPACE "..."` using the configured name literally (not rewritten by `fieldIde`). Blank values and illegal characters in `tablespace` (for example `"`) are rejected at job submission. Only these curated keys are accepted (arbitrary `WITH` parameters are not supported). OpenGauss and HighGo inherit the same validation and DDL path via Postgres catalog/dialect.
- **Dameng**: `fillfactor` and `tablespace` are emitted as a Dameng `STORAGE (...)` clause (`FILLFACTOR <n>`, `ON "<tablespace>"`). `fillfactor` must be an integer in `[0, 100]`; `tablespace` uses the configured name literally (not rewritten by `fieldIde`). Blank values and illegal characters in `tablespace` (for example `"`) are rejected at job submission. Only these curated keys are accepted (arbitrary nested `STORAGE` parameters such as `INITIAL` / `NEXT` are not supported). The tablespace must already exist on the target.
- **Oracle / OceanBase (Oracle mode)**: `pctfree` is emitted as `PCTFREE <n>` and must be an integer in `[0, 99]`; `tablespace` is emitted as `TABLESPACE "..."` using the configured name literally (not rewritten by `fieldIde`). Blank values and illegal characters in `tablespace` (for example `"`) are rejected at job submission. Only these curated keys are accepted (nested `STORAGE (...)` and LOB/partition clauses are not supported). The tablespace must already exist on the target.
- **Kingbase**: `fillfactor` is emitted as `WITH (fillfactor=<n>)` and must be an integer in `[10, 100]` (PostgreSQL-compatible); `tablespace` is emitted as `TABLESPACE "..."` using the configured name literally (not rewritten by `fieldIde`). Blank values and illegal characters in `tablespace` (for example `"`) are rejected at job submission. Only these curated keys are accepted (arbitrary `WITH (...)` parameters are not supported). The tablespace must already exist on the target.

SeaTunnel validates the **key whitelist** at submission time for all dialects that support `table_options`. For PostgreSQL (and OpenGauss / HighGo via the same path), Dameng, and Kingbase, it also validates blank values and the `fillfactor` numeric range. For Oracle / OceanBase (Oracle mode), it also validates blank values and the `pctfree` numeric range. Other dialects (for example MySQL) do not verify whether each value is supported by the target database beyond the key whitelist.

Example (MySQL auto-create with engine and charset):

```hocon
sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3307/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "password"
    database = "mydb"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "engine" = "InnoDB"
      "charset" = "utf8mb4"
      "collate" = "utf8mb4_general_ci"
    }
  }
}
```

The generated `CREATE TABLE` statement appends `ENGINE`, `DEFAULT CHARSET`, and `COLLATE` clauses. Keys outside the dialect whitelist (for example `bucket_num`) fail during job submission.

Example (PostgreSQL auto-create with tablespace and fillfactor):

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/mydb"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "password"
    database = "mydb"
    table = "public.orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "pg_default"
      "fillfactor" = "70"
    }
  }
}
```

Example (Dameng auto-create with tablespace and fillfactor):

```hocon
sink {
  Jdbc {
    url = "jdbc:dm://localhost:5236"
    driver = "dm.jdbc.driver.DmDriver"
    username = "SYSDBA"
    password = "SYSDBA"
    database = "DAMENG"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "MAIN"
      "fillfactor" = "80"
    }
  }
}
```

The generated `CREATE TABLE` statement appends `STORAGE (FILLFACTOR 80, ON "MAIN")`.

Example (Oracle auto-create with tablespace and pctfree):

```hocon
sink {
  Jdbc {
    url = "jdbc:oracle:thin:@localhost:1521/ORCLPDB1"
    driver = "oracle.jdbc.OracleDriver"
    username = "scott"
    password = "tiger"
    database = "SCOTT"
    table = "ORDERS"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "USERS"
      "pctfree" = "10"
    }
  }
}
```

Example (Kingbase auto-create with tablespace and fillfactor):

```hocon
sink {
  Jdbc {
    url = "jdbc:kingbase8://localhost:54321/test"
    driver = "com.kingbase8.Driver"
    username = "SYSTEM"
    password = "123456"
    database = "test"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "pg_default"
      "fillfactor" = "70"
    }
  }
}
```

The generated `CREATE TABLE` statement appends `WITH (fillfactor=70)` and `TABLESPACE "pg_default"`.

### enable_upsert [boolean]

Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import

### is_primary_key_updated [boolean]

Whether primary key fields are included when generating update statements. Keep the default unless your target database requires primary key columns to be skipped during updates.

### support_upsert_by_insert_only [boolean]

Whether to support upsert behavior through insert-only statements for compatible dialects. This is an advanced compatibility option and is disabled by default.

### use_copy_statement [boolean]

Use `COPY ${table} FROM STDIN` statement to import data. Only drivers with `getCopyAPI()` method connections are supported.  e.g.: Postgresql driver `org.postgresql.Driver`.

NOTICE: `MAP`, `ARRAY`, `ROW` types are not supported.

### oracle_insert_mode [Enum]

Oracle insert mode. The default value is `CONVENTIONAL`, which keeps the existing JDBC insert behavior.

When set to `APPEND_VALUES`, SeaTunnel adds the Oracle `APPEND_VALUES` hint to generated insert SQL:

```sql
INSERT /*+ APPEND_VALUES */ INTO ...
```

This option is only supported for Oracle JDBC sink insert-only writes. It requires `generate_sink_sql = true`, `auto_commit = true`, no custom `query`, no `primary_keys`, `is_exactly_once = false`, and `support_upsert_by_insert_only = false`.

### create_index [boolean]

Create the index(contains primary key and any other indexes) or not when auto-create table. You can use this option to improve the performance of jdbc writes when migrating large tables.

Notice: Note that this will sacrifice read performance, so you'll need to manually create indexes after the table migration to improve read performance

### use_kerberos [boolean]

Whether to enable Kerberos authentication for JDBC connections. When enabled, also configure `kerberos_principal`, `kerberos_keytab_path`, and `krb5_path` as required by your environment.

### access_key_id [String]
The access_key_id in AWS authentication. Only valid for dialect="dsql"

### secret_access_key [String]
The secret_access_key in AWS authentication. Only valid for dialect="dsql"

### region [String]
The area where Amazon Aurora DSQL is located. Only valid for dialect="dsql"

## Exactly-once prerequisites

When `is_exactly_once = true`, JDBC Sink uses XA transactions. Before enabling it:

- Set `max_retries = 0` and configure the correct `xa_data_source_class_name` for the installed driver.
- PostgreSQL must allow prepared transactions. Set `max_prepared_transactions` to a positive value large enough for the expected concurrent transactions, then restart PostgreSQL if the server requires it.
- MySQL requires a server and Connector/J combination that supports the XA operations used by the sink. Accounts that perform XA recovery may also require the `XA_RECOVER_ADMIN` privilege; check the requirements for your MySQL version.
- Do not configure `sink.flush.interval`; XA transaction boundaries are controlled by checkpoints.

For MySQL non-XA batch jobs, adding `rewriteBatchedStatements=true` to the JDBC URL can improve throughput. Validate the effect with your driver version and workload.

## Driver reference

The following values are starting points. Confirm the driver artifact and version against the database vendor's compatibility matrix.

| datasource        |                    driver                    | url                                                                 | xa_data_source_class_name                          | maven                                                                                                                         |
|-------------------|----------------------------------------------|---------------------------------------------------------------------|----------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| MySQL             | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                    | com.mysql.cj.jdbc.MysqlXADataSource                | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| PostgreSQL        | org.postgresql.Driver                        | jdbc:postgresql://localhost:5432/postgres                           | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| DM                | dm.jdbc.driver.DmDriver                      | jdbc:dm://localhost:5236                                            | dm.jdbc.driver.DmdbXADataSource                    | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| Phoenix           | org.apache.phoenix.queryserver.client.Driver | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF  | /                                                  | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| SQL Server        | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433                                     | com.microsoft.sqlserver.jdbc.SQLServerXADataSource | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| Oracle            | oracle.jdbc.OracleDriver                     | jdbc:oracle:thin:@localhost:1521/xepdb1                             | oracle.jdbc.xa.OracleXADataSource                  | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlite            | org.sqlite.JDBC                              | jdbc:sqlite:test.db                                                 | /                                                  | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| GBase8a           | com.gbase.jdbc.Driver                        | jdbc:gbase://localhost:5258/test                                    | /                                                  | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                            |
| StarRocks         | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                    | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                    | jdbc:db2://localhost:50000/testdb                                   | com.ibm.db2.jcc.DB2XADataSource                    | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| saphana           | com.sap.db.jdbc.Driver                       | jdbc:sap://localhost:39015                                          | /                                                  | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| Doris             | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                    | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                 | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test               | /                                                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Redshift          | com.amazon.redshift.jdbc42.Driver            | jdbc:redshift://localhost:5439/testdb                               | com.amazon.redshift.xa.RedshiftXADataSource        | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver    | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com          | /                                                  | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                               |
| Vertica           | com.vertica.jdbc.Driver                      | jdbc:vertica://localhost:5433                                       | /                                                  | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| Kingbase          | com.kingbase8.Driver                         | jdbc:kingbase8://localhost:54321/db_test                            | /                                                  | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| OceanBase         | com.oceanbase.jdbc.Driver                    | jdbc:oceanbase://localhost:2881                                     | /                                                  | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| xugu              | com.xugu.cloudjdbc.Driver                    | jdbc:xugu://localhost:5138                                          | /                                                  | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver             | jdbc:IRIS://localhost:1972/%SYS                                     | /                                                  | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |
| opengauss         | org.opengauss.Driver                         | jdbc:opengauss://localhost:5432/postgres                            | /                                                  | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar                              |
| Highgo            | com.highgo.jdbc.Driver                       | jdbc:highgo://localhost:5866/highgo                                 | /                                                  | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                                                   |
| Dsql              | org.postgresql.Driver                        | jdbc:postgresql://Amazon Aurora DSQL Cluster Endpoint:5432/postgres | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| YashanDB          | com.yashandb.jdbc.Driver                     | jdbc:yasdb://localhost:1688/SYS                                     | /                                                  | https://repo1.maven.org/maven2/com/yashandb/yashandb-jdbc/1.10.7/yashandb-jdbc-1.10.7.jar                                     |

## Common patterns

### Custom SQL

```hocon
jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"
}

```

### Exactly-once with custom SQL

Turn on exact one-time semantics by setting `is_exactly_once`

```hocon
jdbc {

    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"

    max_retries = 0
    username = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = true

    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
}
```

### Timer flush on Zeta

This engine-level feature is supported only by Zeta. Spark and Flink do not inject `FlushSignal` records, so `sink.flush.interval` does not enable timer flush on those engines. On Zeta, configure `sink.flush.interval` in the `env` block. The engine periodically injects a `FlushSignal` into the record stream, and JDBC Sink flushes all buffered records immediately, regardless of whether `batch_size` has been reached.

:::tip

Timer flush is not supported when `is_exactly_once = true`. In exactly-once mode the sink uses XA transactions whose boundaries are managed by checkpoints; a timer-triggered flush would break transactional guarantees.

:::

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 30000
  sink.flush.interval = 5000
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    database = "sink_database"
    table = "sink_table"
    generate_sink_sql = true
    primary_keys = ["id"]
    batch_size = 10000
  }
}
```

### Change data capture events

jdbc receive CDC example

```hocon
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        username = "root"
        password = "123456"
        
        database = "sink_database"
        table = "sink_table"
        generate_sink_sql = true
        primary_keys = ["key1", "key2"]
    }
}
```

### Create a missing target table

To facilitate the creation of tables when they do not already exist, set the `schema_save_mode`  to `CREATE_SCHEMA_WHEN_NOT_EXIST`.

```hocon
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        username = "root"
        password = "123456"
        generate_sink_sql = true
        database = "sink_database"
        table = "sink_table"
        primary_keys = ["key1", "key2"]
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

### PostgreSQL 9.5 and earlier CDC compatibility

For PostgreSQL versions 9.5 and below, setting `compatible_mode` to `postgresLow` to enable support for PostgreSQL Change Data Capture (CDC) operations.

```hocon
sink {
    jdbc {
        url = "jdbc:postgresql://localhost:5432"
        driver = "org.postgresql.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "postgresLow"
        database = "sink_database"
        table = "sink_table"
        generate_sink_sql = true
        primary_keys = ["key1", "key2"]
    }
}

```

### Multiple tables

#### MySQL CDC source

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    generate_sink_sql = true
    
    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

#### JDBC source

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    generate_sink_sql = true

    database = "${schema_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

#### Amazon Aurora DSQL

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
    Jdbc {
        dialect="Dsql"
        driver = "org.postgresql.Driver"
        url="jdbc:postgresql://ixxxxxxxxxxxxx.dsql.us-east-1.on.aws:5432/postgres"
        username = "admin"
        access_key_id = "ACCESSKEYIDEXAMPLE"
        secret_access_key = "SECRETACCESSKEYEXAMPLE"
        region = "us-east-1"
        database = "postgres"
        generate_sink_sql = true
        primary_keys = ["id"]
        max_retries = 3
        batch_size = 1000

    }
}
```

## Troubleshooting

### Does JDBC Sink support automatic table creation?

Yes. Use `schema_save_mode` to control table creation behavior:

- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Creates the table only if it does not exist.
- `RECREATE_SCHEMA`: Drops and recreates the table on every job start.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Throws an error if the table is missing.
- `IGNORE`: Skips all table creation logic.

Use `generate_sink_sql = true` together with `database` and `table` for automatic INSERT/UPSERT SQL generation.

### How do I enable exactly-once semantics with JDBC Sink?

JDBC Sink supports exactly-once via XA transactions. Enable it with:

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "password"
    max_retries = 0
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
    generate_sink_sql = true
    database = "mydb"
    table = "target_table"
    primary_keys = ["id"]
  }
}
```

Not all databases support XA transactions. Verify that your database and JDBC driver both support XA before enabling this option.

### How do I configure upsert (INSERT or UPDATE) behavior?

SeaTunnel only enters the upsert / update path after it has a final key set. That key can come from explicit `primary_keys`, or, when `primary_keys` is omitted, from upstream catalog metadata. If no primary key is available, SeaTunnel also tries to inherit the first unique key.

When a final key set exists and `enable_upsert = true`, SeaTunnel prefers the database-native upsert statement provided by the target dialect. For example, PostgreSQL generates `INSERT ... ON CONFLICT (...) DO UPDATE` (or `DO NOTHING` when every column is part of the key and there is nothing left to update):

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/sales"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "password"
    generate_sink_sql = true
    database = "sales"
    table = "public.orders"
    primary_keys = ["id"]
  }
}
```

When a final key set exists but `enable_upsert = false`, SeaTunnel stops using native database upsert SQL and falls back to the row-kind-driven insert/update path:

- `INSERT` rows are written as plain INSERTs
- CDC `UPDATE_AFTER` rows are written as UPDATEs
- CDC `DELETE` rows are written as DELETEs

As a result, `enable_upsert = false` is not appropriate for ordinary batch imports that rely on duplicate-key overwrite behavior.

### What happens if I do not configure `primary_keys`?

If `primary_keys` is not configured, SeaTunnel first tries to inherit the primary key from upstream catalog metadata. If there is no primary key, it then tries the first unique key.

JDBC Sink falls back to plain INSERT only when there is no explicit key and nothing usable can be inherited from upstream metadata. In that keyless mode, no database-native upsert SQL is generated, and the sink no longer uses row-kind-aware UPDATE / DELETE executors. For CDC inputs, the write path therefore effectively degrades to plain INSERT batching, and duplicate-key behavior depends entirely on the target table constraints.

### When should I enable `use_copy_statement`?

`use_copy_statement = true` makes JDBC Sink prefer the `COPY <table> (...) FROM STDIN WITH CSV` path instead of regular INSERT / UPSERT SQL. This happens before the normal primary-key-based write path, so COPY is still chosen even if `primary_keys` is configured.

This option is mainly for high-volume PostgreSQL imports, and it has three important constraints:

- the JDBC driver connection must expose `getCopyAPI()`, otherwise the job fails and tells you to switch `use_copy_statement` back to `false`
- it is not a replacement for `ON CONFLICT`, so it does not provide duplicate-key overwrite semantics
- `MAP`, `ARRAY`, and `ROW` types are not supported

### How do I write to multiple tables in a single job?

Use `table = "${table_name}"` and `database = "${schema_name}"` as placeholders. SeaTunnel resolves these from the upstream record's metadata when used with CDC sources or multi-table configurations. Pair with `generate_sink_sql = true` for fully automatic SQL generation.

### Why is my JDBC driver not found?

SeaTunnel does not bundle all JDBC drivers. For Spark and Flink, place the JAR in `${SEATUNNEL_HOME}/plugins/Jdbc/lib/` on every execution node. For Zeta, place it in `${SEATUNNEL_HOME}/lib/` on every SeaTunnel node and restart the affected processes. Common driver file names include:

- MySQL: `mysql-connector-j-8.x.x.jar`
- PostgreSQL: `postgresql-42.x.x.jar`
- Oracle: `ojdbc8.jar`
- SQL Server: `mssql-jdbc-12.x.x.jre11.jar`

## Changelog

<ChangeLog />
