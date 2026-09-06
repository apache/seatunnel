import ChangeLog from '../changelog/connector-file-gcs.md';

# GcsFile

> Google Cloud Storage file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] multiple table sink
- [x] partitioned output
- [x] save modes
- [x] file formats: `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `canal_json`, `debezium_json`, and `maxwell_json`

## Description

Writes data to Google Cloud Storage through the Google Cloud Storage connector for Hadoop. The
connector reuses SeaTunnel's file sink implementation for serialization, partitioning, file
rotation, checkpoint commits, multiple-table jobs, and save-mode handling.

Set `bucket` to a bucket URI such as `gs://my-bucket`. Set `path` and `tmp_path` to prefixes inside
that bucket, such as `/warehouse/orders` and `/tmp/seatunnel/orders`. Do not include an object path
in `bucket`.

When transactions are enabled, writers first create files under `tmp_path` and publish them to
`path` during commit. The configured identity therefore needs read, create, list, and delete
permissions for both prefixes, plus object move permission when using the driver's native move
operation. Transactional commits do not provide atomic visibility of an entire
directory to external readers: the Hadoop GCS connector can publish files using copy and delete
operations. Disabling `is_enable_transaction` disables the file sink's transactional commit path.

## Dependency

The connector uses `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.33:shaded`, which is
Apache License 2.0 software and targets Java 8. The shaded GCS Hadoop library is packaged in the
`connector-file-gcs` connector JAR. Spark and Flink deployments must provide a compatible Hadoop 3
runtime on every driver and worker.

## Authentication

The connector supports these authentication modes:

1. **Application Default Credentials (ADC):** omit `service_account_key_file`. The Hadoop GCS
   connector discovers credentials from `GOOGLE_APPLICATION_CREDENTIALS` or the service account
   attached to the Google Cloud runtime.
2. **Service-account JSON file:** set `service_account_key_file` to a local path that exists at the
   same location on every node that writes GCS.

The explicit `service_account_key_file` option takes precedence over the corresponding entry in
`hadoop_gcs_properties`. Do not store service-account JSON content in the job configuration or in
`hadoop_gcs_properties`.

## Sink Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| path | string | yes | - | Destination prefix inside `bucket`, for example `/warehouse/orders`. Supports `${database_name}`, `${schema_name}`, and `${table_name}` placeholders. |
| bucket | string | yes | - | GCS bucket URI, for example `gs://my-bucket`. |
| service_account_key_file | string | no | - | Service-account JSON key file on every worker. When omitted, ADC is used. |
| hadoop_gcs_properties | map | no | - | Additional `fs.gs.*` Hadoop properties. Explicit connector options take precedence. |
| tmp_path | string | no | `/tmp/seatunnel` | Temporary prefix inside the configured bucket used before transactional commit. Use a distinct prefix from `path`. |
| file_format_type | string | no | `csv` | Output file format. |
| schema_save_mode | enum | no | `CREATE_SCHEMA_WHEN_NOT_EXIST` | How the destination prefix is prepared before writing. |
| data_save_mode | enum | no | `APPEND_DATA` | Whether existing objects are kept, deleted, or rejected before writing. |
| custom_filename | boolean | no | `false` | Enables `file_name_expression`. |
| file_name_expression | string | conditional | `${transactionId}` | Output filename expression when `custom_filename=true`. Supports `${transactionId}`, `${now}`, and `${uuid}`. |
| filename_time_format | string | no | `yyyy.MM.dd` | Time format used by `${now}`. |
| filename_extension | string | no | format-specific | Overrides the output filename extension. |
| have_partition | boolean | no | `false` | Writes rows into partition directories. |
| partition_by | list | conditional | - | Partition columns when `have_partition=true`. |
| partition_dir_expression | string | no | `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/` | Partition directory expression. |
| is_partition_field_write_in_file | boolean | no | `false` | Whether partition fields remain in output rows. |
| sink_columns | list | no | all columns | Columns written to the output file, in output order. |
| is_enable_transaction | boolean | no | `true` | Stages files and publishes them during commit. |
| batch_size | int | no | `1000000` | Maximum rows in a split output file. |
| single_file_mode | boolean | no | `false` | Produces one output file per parallel sink task. Not supported with checkpointing or streaming mode. |
| create_empty_file_when_no_data | boolean | no | `false` | Creates an empty output file when no rows arrive. |
| row_delimiter | string | conditional | `\n` | Row delimiter for text, CSV, and JSON. |
| field_delimiter | string | conditional | `\001` | Field delimiter for text. |
| encoding | string | conditional | `UTF-8` | Encoding for text, CSV, JSON, and XML. |
| enable_header_write | boolean | conditional | `false` | Writes a header for text or CSV output. |
| compress_codec | string | no | `none` | Compression codec supported by the selected format. |
| common-options | | no | - | See [Sink Common Options](../common-options/sink-common-options.md). |

The sink also accepts the format-specific file sink options documented by the corresponding
SeaTunnel file formats, including Parquet INT96 and XML element options.

When `is_partition_field_write_in_file=true`, set `parse_partition_from_path=false` on a file
source reading that output if its schema already includes those partition columns.

## Save Modes

`schema_save_mode` controls destination-prefix creation:

- `RECREATE_SCHEMA`: delete and recreate the prefix.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: create the prefix only when absent.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: fail when the prefix does not exist.
- `IGNORE`: do not prepare the prefix.

`data_save_mode` controls existing objects:

- `APPEND_DATA`: keep existing objects and add new output files.
- `DROP_DATA`: delete objects below `path` before writing.
- `ERROR_WHEN_DATA_EXISTS`: fail when objects already exist below `path`.

These operations apply only to `path` inside the configured bucket. GcsFile rejects a bucket-root
`path` when `RECREATE_SCHEMA` or `DROP_DATA` is selected; configure a dedicated prefix instead.

## Examples

### Write Parquet With ADC

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/orders"
    tmp_path = "/tmp/seatunnel/orders"
    file_format_type = "parquet"
    data_save_mode = "APPEND_DATA"
  }
}
```

### Write Partitioned CSV With a Service Account

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/exports/customers"
    tmp_path = "/tmp/seatunnel/customers"
    file_format_type = "csv"
    service_account_key_file = "/opt/seatunnel/keys/gcs-writer.json"
    enable_header_write = true
    have_partition = true
    partition_by = ["region"]
    hadoop_gcs_properties = {
      "fs.gs.project.id" = "my-project"
    }
  }
}
```

### Multiple-Table Output

Use table placeholders to keep each upstream table under a separate destination and temporary
prefix:

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/${database_name}/${table_name}"
    tmp_path = "/tmp/seatunnel/${database_name}/${table_name}"
    file_format_type = "parquet"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Changelog

<ChangeLog />
