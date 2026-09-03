import ChangeLog from '../changelog/connector-file-sftp.md';

# SftpFile

> Sftp file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write data to a remote directory over SFTP. The connector supports multiple file formats
(`text`, `csv`, `parquet`, `orc`, `json`, `excel`, `binary`, `xml`, `canal_json`, `debezium_json`,
`maxwell_json`), partition-based output, custom file names, and runtime schema evolution for CDC
pipelines.

:::tip

If you use Spark/Flink, make sure your Spark/Flink cluster already integrates Hadoop. The tested
Hadoop version is 2.x.

If you use SeaTunnel Engine, the Hadoop JARs are bundled with the engine. You can confirm by
checking `${SEATUNNEL_HOME}/lib`.

The connector supports both password authentication and public-key authentication via the
`keyfile` option.

:::

## Key features

- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  By default, we use 2PC commit to ensure `exactly-once`.

- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

  Use `${database_name}` and `${table_name}` placeholders in the `path` to route rows from different upstream tables to separate output directories.

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] canal_json
  - [x] debezium_json
  - [x] maxwell_json

## Options

| name                                  | type    | required | default value                              | remarks                                                                                                                                                                         |
|---------------------------------------|---------|----------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | yes      | -                                          | The target SFTP host.                                                                                                                                                           |
| port                                  | int     | yes      | -                                          | The target SFTP port, typically `22`.                                                                                                                                            |
| user                                  | string  | yes      | -                                          | SFTP login user name.                                                                                                                                                           |
| password                              | string  | no       | -                                          | SFTP login password. Required when `keyfile` is not set.                                                                                                                         |
| keyfile                               | string  | no       | -                                          | Private key file path used for SFTP public key authentication.                                                                                                                   |
| path                                  | string  | yes      | -                                          | Target directory on the remote SFTP server.                                                                                                                                     |
| tmp_path                              | string  | yes      | /tmp/seatunnel                             | Staging directory on the SFTP server; the connector writes output files here first and then `mv`s them into `path` once a checkpoint completes.                                   |
| custom_filename                       | boolean | no       | false                                      | Whether you need to customize the file name.                                                                                                                                     |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when `custom_filename = true`.                                                                                                                                        |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when `custom_filename = true`.                                                                                                                                        |
| file_format_type                      | string  | no       | "csv"                                      | One of `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `canal_json`, `debezium_json`, `maxwell_json`.                                                         |
| filename_extension                    | string  | no       | -                                          | Override the default file name extensions with a custom file name extension, e.g. `.xml`, `.json`, `dat`, `.customtype`.                                                          |
| field_delimiter                       | string  | no       | '\001' for text and ',' for csv            | Only used when `file_format_type` is `text` or `csv`.                                                                                                                            |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when `file_format_type` is `text`, `csv`, or `json`.                                                                                                                   |
| have_partition                        | boolean | no       | false                                      | Whether to partition the output by upstream field values.                                                                                                                        |
| partition_by                          | array   | no       | -                                          | Only used when `have_partition = true`.                                                                                                                                         |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used when `have_partition = true`.                                                                                                                                         |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used when `have_partition = true`.                                                                                                                                         |
| sink_columns                          | array   | no       |                                            | Columns to write to the file. When empty, all upstream columns are written.                                                                                                       |
| is_enable_transaction                 | boolean | no       | true                                       | Reserved. Only `true` is supported and it is the default behavior.                                                                                                               |
| batch_size                            | int     | no       | 1000000                                    | Maximum number of rows per file before rotation.                                                                                                                                 |
| compress_codec                        | string  | no       | none                                       | Compression codec, see details below.                                                                                                                                           |
| common-options                        | object  | no       | -                                          | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md).                                                                  |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when `file_format_type` is `excel`.                                                                                                                                    |
| sheet_max_rows                        | int     | no       | 1048576                                    | Only used when `file_format_type` is `excel`.                                                                                                                                    |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when `file_format_type` is `excel`.                                                                                                                                    |
| csv_string_quote_mode                 | enum    | no       | MINIMAL                                    | Only used when `file_format_type` is `csv`.                                                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when `file_format_type` is `xml`.                                                                                                                                     |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when `file_format_type` is `xml`.                                                                                                                                     |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when `file_format_type` is `xml`.                                                                                                                                     |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism writes a single file; with this on, `batch_size` does not take effect and the file name has no block suffix.                                                    |
| create_empty_file_when_no_data        | boolean | no       | false                                      | When there is no upstream data, an empty file is still created.                                                                                                                 |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when `file_format_type` is `parquet`.                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when `file_format_type` is `parquet`.                                                                                                                                 |
| enable_header_write                   | boolean | no       | false                                      | Only used when `file_format_type` is `text` or `csv`. When `true`, a header row is written.                                                                                      |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when `file_format_type` is `json`, `text`, `csv`, or `xml`.                                                                                                            |
| schema_evolution_enabled              | boolean | no       | false                                      | Enable CDC schema evolution (ADD/DROP/RENAME/MODIFY) at runtime without job restart. Not supported for `binary`. See details below.                                              |
| schema_save_mode                      | string  | no       | CREATE_SCHEMA_WHEN_NOT_EXIST               | How the target directory is handled when the job starts.                                                                                                                        |
| data_save_mode                        | string  | no       | APPEND_DATA                                | How existing data files in the target directory are handled when the job starts.                                                                                                |
| merge_update_event                    | boolean | no       | false                                      | Only used when `file_format_type` is `canal_json`, `debezium_json`, or `maxwell_json`. When `true`, UPDATE_AFTER and UPDATE_BEFORE are merged into a single UPDATE event.          |

### file_name_expression [string]

Only used when `custom_filename` is `true`.

`file_name_expression` describes the file expression that will be used to build the file name
inside `path`. You can add the variables `${now}` or `${uuid}` in the expression, for example
`test_${uuid}_${now}`. `${now}` represents the current time, and its format can be defined by
specifying the option `filename_time_format`.

Please note that, when `is_enable_transaction` is `true`, `${transactionId}_` is automatically
prepended to the file name.

### compress_codec [string]

The compression codec to apply to output files. The supported combinations are:

- `text` / `json` / `csv`: `lzo`, `none`
- `orc`: `lzo`, `snappy`, `lz4`, `zlib`, `none`
- `parquet`: `lzo`, `snappy`, `lz4`, `gzip`, `brotli`, `zstd`, `none`

The `excel` format does not support any compression codec.

### schema_save_mode [string]

How the target directory is handled when the job starts:

- `CREATE_SCHEMA_WHEN_NOT_EXIST` (default): create the directory when it does not exist; skip otherwise.
- `RECREATE_SCHEMA`: delete and recreate the directory when it exists.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: fail fast when the directory does not exist.
- `IGNORE`: do not touch the directory.

### data_save_mode [string]

How existing files in the target directory are handled when the job starts:

- `APPEND_DATA` (default): keep the directory and existing files; append new data.
- `DROP_DATA`: keep the directory; delete existing data files.
- `ERROR_WHEN_DATA_EXISTS`: fail fast when there are existing files.

### schema_evolution_enabled [boolean]

When set to `true`, the SFTP file sink handles CDC schema change events (ADD COLUMN, DROP COLUMN,
RENAME COLUMN, MODIFY COLUMN type) at runtime without requiring a job restart. On each schema
change the current output file is closed and a new file is opened with the updated schema.

**Supported formats:** all file formats except `binary`. Setting this option together with
`file_format_type = binary` fails at job startup with a config validation error.

**Partition constraint:** when `have_partition = true`, dropping a column listed in `partition_by`
is rejected and fails fast. Partition columns must remain stable across schema changes.

**When `schema_evolution_enabled = false` (default):** if the upstream CDC source has
`schema-changes.enabled = true` and an `AlterTableEvent` arrives at the sink, the job fails
immediately with an actionable error:

> `Received AlterTableEvent but schema_evolution_enabled=false at this sink. Either set schema_evolution_enabled=true to handle schema changes, or set schema-changes.enabled=false at the CDC source to suppress them.`

Users on the default CDC source config (`schema-changes.enabled = false`) are completely
unaffected.

**Known limitation:** schema changes are not atomic with checkpointing. If the job crashes in the
narrow window between file rotation and the schema metadata update, rows written after restore may
use the pre-change schema. This is a known architectural gap shared with other SeaTunnel sinks.

## Example

### Text With Partitions And Custom Filename

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/seatunnel/job1"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name", "age"]
    is_enable_transaction = true
  }
}
```

### Multiple Tables

When the source end covers multiple tables and each should land in its own directory, use
`${table_name}` in the path.

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/seatunnel/job1/${table_name}"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name", "age"]
    is_enable_transaction = true
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "DROP_DATA"
  }
}
```

### CDC With Schema Evolution

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    password = "********"
    path = "/data/sftp/cdc/${table_name}"
    tmp_path = "/data/sftp/cdc/tmp"
    file_format_type = "parquet"
    schema_evolution_enabled = true
    have_partition = true
    partition_by = ["updated_at_month"]
  }
}
```

### Parquet With Public Key Authentication

```hocon
sink {
  SftpFile {
    host = "sftp.example.com"
    port = 22
    user = "seatunnel"
    keyfile = "/home/seatunnel/.ssh/id_rsa"
    path = "/data/sftp/seatunnel/parquet"
    tmp_path = "/data/sftp/seatunnel/tmp"
    file_format_type = "parquet"
    sink_columns = ["name", "age"]
  }
}
```

## Changelog

<ChangeLog />