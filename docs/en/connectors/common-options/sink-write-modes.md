---
sidebar_position: 3
---

# Sink Write Modes and Save Modes

Sink configuration has two different decisions that are easy to mix together:

- **Write mode** decides how SeaTunnel writes each row to the target system.
- **Save mode** decides how SeaTunnel handles the existing target table, index, directory, or data before the job starts writing rows.

Read this page when you are choosing among `generate_sink_sql`, `query`, `schema_save_mode`, `data_save_mode`, `custom_sql`, `primary_keys`, or `enable_upsert`.

## Quick Decision Table

| Goal | Prefer | Notes |
|------|--------|-------|
| Let SeaTunnel generate INSERT / UPSERT / UPDATE / DELETE SQL for a JDBC target | `generate_sink_sql = true` with `database`, `table`, and usually `primary_keys` | This is a JDBC Sink feature. It also enables save mode handling for JDBC because SeaTunnel can resolve the target catalog table. |
| Fully control the JDBC write statement | `query = "INSERT ... VALUES (?, ...)"` | Do not combine with `generate_sink_sql = true`. In this mode JDBC Sink does not execute `schema_save_mode`, `data_save_mode`, or `custom_sql`. |
| Create a missing target table or fail when it is missing | `schema_save_mode` | Only works for sinks that expose save mode options and can create or inspect the target through a catalog. |
| Keep, clear, or reject existing target data before writing | `data_save_mode` | Supported values depend on the connector. File sinks usually support only `DROP_DATA`, `APPEND_DATA`, and `ERROR_WHEN_DATA_EXISTS`. |
| Run a custom SQL statement before the job writes data | `data_save_mode = "CUSTOM_PROCESSING"` and `custom_sql` | Only for connectors that expose both options. This is a pre-write hook, not the per-row write SQL. |
| Use database-native upsert in JDBC Sink | `generate_sink_sql = true`, `primary_keys`, `enable_upsert = true` | Without a usable primary key or unique key, JDBC generated SQL falls back to plain INSERT. |
| Write to object storage or file systems | Check the specific File Sink option table | File sinks do not use `generate_sink_sql`. Some file connectors expose save mode options; others do not. |

## JDBC: `generate_sink_sql` vs `query`

JDBC Sink has two mutually exclusive write modes.

| Mode | Required options | Save modes applied? | Typical use case |
|------|------------------|---------------------|------------------|
| Generated SQL | `generate_sink_sql = true`, `database`, and normally `table` | Yes, when the target catalog table can be resolved | Most database sink jobs, CDC writes, automatic table creation, upsert, update, and delete |
| Custom SQL | `query = "INSERT ... VALUES (?, ...)"` | No | You must control the exact target SQL and accept that save mode handling is skipped |

Do not configure both `generate_sink_sql = true` and `query`.

When `generate_sink_sql = true`, configure `primary_keys` if the sink must process UPDATE, DELETE, or upsert records. If `primary_keys` is omitted, SeaTunnel tries to inherit a primary key from upstream catalog metadata, then the first unique key. If neither exists, generated SQL becomes plain INSERT.

## Save Mode Semantics

### `schema_save_mode`

`schema_save_mode` controls the target structure before row writing starts.

| Value | Behavior |
|-------|----------|
| `RECREATE_SCHEMA` | Create the target if it does not exist. If it exists, drop and recreate it. |
| `CREATE_SCHEMA_WHEN_NOT_EXIST` | Create the target only when it does not exist. |
| `ERROR_WHEN_SCHEMA_NOT_EXIST` | Fail when the target does not exist. |
| `IGNORE` | Skip structure handling. |

For database sinks, the target is usually a table. For file sinks, the target is usually a path or directory.

### `data_save_mode`

`data_save_mode` controls existing target data before row writing starts.

| Value | Behavior |
|-------|----------|
| `DROP_DATA` | Keep the structure and clear existing data. |
| `APPEND_DATA` | Keep existing data and append new data. |
| `CUSTOM_PROCESSING` | Execute `custom_sql` before writing. Only supported by connectors that expose both options. |
| `ERROR_WHEN_DATA_EXISTS` | Fail when existing data is found. |

Connector support is not universal. Always use the option table of the connector version you run as the final source of truth.

## Connector Support Boundaries

### JDBC-family sinks

The JDBC Sink and JDBC-based sink pages such as MySQL, PostgreSQL, Oracle, and SQL Server use the JDBC write-mode model:

- `generate_sink_sql` is available.
- `query` is available.
- `schema_save_mode` and `data_save_mode` are available in generated SQL mode.
- `custom_sql` is only executed when save mode handling runs.
- `enable_upsert` only matters after SeaTunnel has a usable primary key or unique key.

See [JDBC Sink](../sink/Jdbc.md) for the complete option reference and examples.

### Doris Sink

Doris Sink supports `schema_save_mode`, `data_save_mode`, `custom_sql`, and `save_mode_create_template`, but it does not use JDBC `generate_sink_sql`.

For CDC DELETE events, Doris also needs Doris-side delete support and the connector option `sink.enable-delete` when applicable. See [Doris Sink](../sink/Doris.md).

### File and object-storage sinks

File sinks write files, so they do not use `generate_sink_sql`, `query`, or database upsert.

Current connector support differs by file connector:

| Connector | Save mode options exposed? | Notes |
|-----------|----------------------------|-------|
| LocalFile | Yes | Handles existing local directories and files. |
| HdfsFile | Yes | Handles existing HDFS directories and files. |
| FtpFile | Yes | Handles existing FTP directories and files. |
| SftpFile | Yes | Handles existing SFTP directories and files. |
| S3File | Yes | Handles existing S3 paths and objects through the file sink save mode flow. |
| OssFile | Yes | Handles existing OSS paths and objects through the file sink save mode flow. |
| ObsFile | No | The current sink option rule does not expose `schema_save_mode` or `data_save_mode`. |
| CosFile | No | The current sink option rule does not expose `schema_save_mode` or `data_save_mode`. |
| BosFile | No | The current sink option rule does not expose `schema_save_mode` or `data_save_mode`. |

If a file connector page does not list `schema_save_mode` or `data_save_mode`, do not assume the option is accepted by that connector.

## Examples

### JDBC generated SQL with save modes

```hocon
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

    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### JDBC custom SQL without save modes

```hocon
sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/sales"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "change_me"

    query = "INSERT INTO orders(id, amount) VALUES (?, ?)"
  }
}
```

In this mode, JDBC Sink writes rows through `query`; it does not execute `schema_save_mode`, `data_save_mode`, or `custom_sql`.

### S3File clear existing data before writing

```hocon
sink {
  S3File {
    path = "/warehouse/orders"
    bucket = "s3a://example-bucket"
    fs.s3a.endpoint = "s3.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "..."
    secret_key = "..."

    file_format_type = "json"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "DROP_DATA"
  }
}
```

## Troubleshooting

### `generate_sink_sql = true` still writes only INSERT

Check whether SeaTunnel has a usable key. Configure `primary_keys` explicitly when you expect upsert, update, or delete behavior.

### `custom_sql` did not run in JDBC Sink

Check whether the sink uses `query`. JDBC custom query mode does not apply save mode handling, so `custom_sql` is skipped.

### A file sink rejects `data_save_mode`

Check the specific connector option table. `S3File`, `OssFile`, `HdfsFile`, `FtpFile`, `SftpFile`, and `LocalFile` expose file save mode options. `ObsFile`, `CosFile`, and `BosFile` currently do not.

### I only want to create the target table

Save mode runs as part of a sink job before row writing. SeaTunnel does not provide a standalone "DDL only" mode through `schema_save_mode`. If the job has no rows, the sink may still initialize, but this is not a replacement for a dedicated schema-management workflow.
