import ChangeLog from '../changelog/connector-hudi.md';

# Hudi

> Hudi sink connector

## Description

The Hudi sink connector writes SeaTunnel rows to an Apache Hudi table stored on HDFS or an
S3-compatible filesystem. It supports both single-table and multi-table jobs, with CDC
changelog persistence, configurable commit cleanup, and pluggable indexing.

Use this connector when you want a copy-on-write or merge-on-read Hudi table backed by
SeaTunnel CDC input (e.g. MySQL-CDC, PostgreSQL-CDC) or batch sources. The connector writes
Hudi data files plus `.hoodie` metadata and lets you choose between `INSERT`, `UPSERT`, and
`BULK_INSERT` write modes through `op_type`.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

:::caution Hive Metastore synchronization

The SeaTunnel Hudi sink writes Hudi data files and `.hoodie` metadata, but it does not register the table in or synchronize the table with Hive Metastore. Options matching `hoodie.datasource.hive_sync.*` are not supported sink options and are not passed to the Hudi write client. Run Apache Hudi's `HiveSyncTool` or another table registration process separately when Hive Metastore registration is required.

:::

## Options

Base configuration:

|            name            |  type   | required | default value                | description |
|----------------------------|---------|----------|------------------------------|-------------|
| table_dfs_path             | string  | yes      | -                            | Root path for Hudi table data and metadata. |
| conf_files_path            | string  | no       | -                            | Semicolon-separated local paths to HDFS client configuration files. |
| table_list                 | Array   | no       | -                            | Per-table settings for multi-table jobs. |
| schema_save_mode           | enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the target schema before the job starts. |
| data_save_mode             | enum    | no       | APPEND_DATA                  | How to handle existing table data before the job starts. |
| common-options             | Config  | no       | -                            | [Common sink options](../common-options/sink-common-options.md). |

Table list configuration:

|       name                 |  type  | required | default value | description |
|----------------------------|--------|----------|---------------|-------------|
| table_name                 | string | yes      | -             | Target Hudi table name. |
| database                   | string | no       | default       | Target Hudi database name. |
| table_type                 | enum   | no       | COPY_ON_WRITE | Hudi table type: `COPY_ON_WRITE` or `MERGE_ON_READ`. |
| op_type                    | enum   | no       | INSERT        | Write operation: `INSERT`, `UPSERT`, or `BULK_INSERT`. |
| record_key_fields          | string | no       | -             | Field or fields used to build the Hudi record key; required for `UPSERT`. |
| partition_fields           | string | no       | -             | Field or fields used to build the partition path. |
| precombine_field           | string | no       | -             | Field used to resolve multiple updates to the same record. |
| batch_interval_ms          | Int    | no       | 1000          | Currently unused; flushes are triggered only by `batch_size` or a checkpoint. |
| batch_size                 | Int    | no       | 1000          | Maximum buffered rows before a flush. |
| insert_shuffle_parallelism | Int    | no       | 2             | Shuffle parallelism for insert operations. |
| upsert_shuffle_parallelism | Int    | no       | 2             | Shuffle parallelism for upsert operations. |
| min_commits_to_keep        | Int    | no       | 20            | Minimum number of commits retained during cleaning. |
| max_commits_to_keep        | Int    | no       | 30            | Maximum number of commits retained during cleaning. |
| index_type                 | enum   | no       | BLOOM         | Hudi index type: `BLOOM`, `SIMPLE`, or `GLOBAL_BLOOM`. |
| index_class_name           | string | no       | -             | Fully qualified custom Hudi index class name. |
| record_byte_size           | Int    | no       | 1024          | Estimated average record size in bytes. |
| cdc_enabled                | boolean| no       | false         | Persist Hudi CDC change-log data when enabled. |

Note: When this configuration corresponds to a single table, you can flatten the configuration items in `table_list` to the outer layer. For a multi-table job, keep table-specific options inside each `table_list` entry; `table_dfs_path`, `conf_files_path`, `schema_save_mode`, and `data_save_mode` remain at the sink level.

`record_key_fields` is required for `UPSERT` (validated at startup) and for `BULK_INSERT` (currently unvalidated — omitting it causes a `NullPointerException` during writing, not a config-time error). For CDC input, the upstream record must contain the fields used by `record_key_fields`; set `cdc_enabled = true` only when the Hudi CDC change log is required.

### table_name [string]

`table_name` The name of hudi table.

### database [string]

`database` The database of hudi table.

### table_dfs_path [string]

`table_dfs_path` The dfs root path of hudi table, such as 'hdfs://nameserivce/data/hudi/'.

### table_type [enum]

`table_type` The type of hudi table. The value is `COPY_ON_WRITE` or `MERGE_ON_READ`.

### record_key_fields [string]

`record_key_fields` The record key fields of hudi table, its are used to generate record key. It must be configured when op_type is `UPSERT`.

### partition_fields [string]

`partition_fields` The partition key fields of hudi table, its are used to generate partition.

### precombine_field [string]

`precombine_field` The precombine field of hudi table, its are used in preCombining before actual write. 

### index_type [string]

`index_type` The index type of hudi table. Currently, `BLOOM`, `SIMPLE`, and `GLOBAL_BLOOM` are supported.

### index_class_name [string]

`index_class_name` The customized index classpath of hudi table, example `org.apache.seatunnel.connectors.seatunnel.hudi.index.CustomHudiIndex`.

### record_byte_size [Int]

`record_byte_size` The byte size of each record, This value can be used to help calculate the approximate number of records in each hudi data file. Adjusting this value can effectively reduce the number of hudi data file write magnifications.

### conf_files_path [string]

`conf_files_path` The environment conf file path list(local path), which used to init hdfs client to read hudi table file. The example is '/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml'.

### op_type [enum]

`op_type` The operation type of hudi table. The value is `insert` or `upsert` or `bulk_insert`.

### batch_interval_ms [Int]

`batch_interval_ms` is retained for compatibility. To schedule time-based flushes on Zeta, configure
`sink.flush.interval` in the job `env` block.

### batch_size [Int]

`batch_size` The maximum number of rows buffered before one flush to Hudi.

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` The parallelism of insert data to hudi table.

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` The parallelism of upsert data to hudi table.

### min_commits_to_keep [Int]

`min_commits_to_keep` The min commits to keep of hudi table.

### max_commits_to_keep [Int]

`max_commits_to_keep` The max commits to keep of hudi table.

### cdc_enabled [boolean]

`cdc_enabled` Whether to persist the CDC change log. When enable, persist the change data if necessary, and the table can be queried as a CDC query mode.

### schema_save_mode [Enum]

Controls how the connector handles the target table schema before the job starts:

- `RECREATE_SCHEMA`: Create the table if it does not exist; otherwise delete and recreate it.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Create the table only when it does not exist.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Fail when the table does not exist.
- `IGNORE`: Do not perform schema handling.

### data_save_mode [Enum]

Choose how to handle existing data before the synchronization task starts.

`DROP_DATA`: Keep the table structure and delete existing data.

`APPEND_DATA`: Keep the table structure and existing data.

`ERROR_WHEN_DATA_EXISTS`: Throw an error when data already exists.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Timer Flush

Timer flush is an engine-level feature supported only by Zeta. Configure `sink.flush.interval` in the job `env` block
to write pending Hudi records even when `batch_size` has not been reached. Spark and Flink do not inject `FlushSignal`
records and therefore do not trigger this scheduled flush.

```hocon
env {
  sink.flush.interval = 5000
}
```

Hudi timer flush reuses the connector's synchronized batch flush and the Hudi client's auto-commit behavior. The Hudi
sink does not provide a 2PC exactly-once writer, so timer flush provides at-least-once delivery. Retries can create
additional commits. With `INSERT`, generated record keys can also produce duplicate rows after recovery; `UPSERT` with
stable `record_key_fields` limits duplicate logical records.

## Examples

### Single Table Upsert

When `op_type` is `UPSERT`, `record_key_fields` must be configured.

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    database = "st"
    table_name = "st_test"
    table_type = "COPY_ON_WRITE"
    op_type = "UPSERT"
    record_key_fields = "c_bigint"
    batch_size = 1000
    batch_interval_ms = 1000
  }
}
```

### Minimal Single Table

For append-only writes, only `table_dfs_path` and `table_name` are required.

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    table_name = "st_test"
  }
}
```

### Multiple Tables

Use `table_list` when the upstream source produces multiple tables.

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
  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    table_list = [
      {
        database = "st1"
        table_name = "role"
        table_type = "COPY_ON_WRITE"
        op_type = "INSERT"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "user"
        table_type = "COPY_ON_WRITE"
        op_type = "UPSERT"
        record_key_fields = "user_id"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "Bucket"
        table_type = "MERGE_ON_READ"
      }
    ]
  }
}
```

### CDC To Hudi

Enable `cdc_enabled` when the target Hudi table needs to persist CDC changelog information.

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    database = "st"
    table_name = "st_test"
    table_type = "COPY_ON_WRITE"
    op_type = "UPSERT"
    record_key_fields = "id"
    cdc_enabled = true
  }
}
```

### S3 Storage

The sink can write to an S3-compatible path. The `connector-hudi` module does not depend on `hadoop-aws`/`aws-java-sdk`, so resolving the `s3a://` scheme requires placing `hadoop-aws` and the matching AWS SDK bundle (or SeaTunnel's `seatunnel-hadoop-aws` jar) into `$SEATUNNEL_HOME/lib` (or the connector's plugin lib directory) before the example below will work. After that, provide the required Hadoop filesystem settings through `conf_files_path` (or the runtime classpath), then use an `s3a://` table path.

```hocon
sink {
  Hudi {
    table_dfs_path = "s3a://hudi/"
    conf_files_path = "/etc/hadoop/core-site.xml;/etc/hadoop/hdfs-site.xml"
    table_name = "st_test"
    op_type = "UPSERT"
    record_key_fields = "id"
  }
}
```

## Changelog

<ChangeLog />
