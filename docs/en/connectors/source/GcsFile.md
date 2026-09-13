import ChangeLog from '../changelog/connector-file-gcs.md';

# GcsFile

> Google Cloud Storage file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)
- [x] multiple table source
- [x] file formats: `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `markdown`, and `pdf`

## Description

Reads files from Google Cloud Storage through the Google Cloud Storage connector for Hadoop. The
connector reuses SeaTunnel's file source implementation for format parsing, schema discovery,
projection, splitting, and multiple-table jobs.

Set `bucket` to a bucket URI such as `gs://my-bucket`. Set `path` to the object or prefix inside that
bucket, such as `/data/orders`. Do not include an object path in `bucket`.

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
   same location on every node that reads GCS.

The explicit `service_account_key_file` option takes precedence over the corresponding entry in
`hadoop_gcs_properties`.

## Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| path | string | yes | - | Object or prefix path inside `bucket`, for example `/data/orders`. |
| file_format_type | string | yes | - | File format: `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `markdown`, or `pdf`. |
| bucket | string | yes | - | GCS bucket URI, for example `gs://my-bucket`. |
| service_account_key_file | string | no | - | Service-account JSON key file on every worker. When omitted, ADC is used. |
| hadoop_gcs_properties | map | no | - | Additional `fs.gs.*` Hadoop properties. Explicit connector options take precedence. |
| schema | config | conditional | - | Required for `text`, `json`, `excel`, `csv`, and `xml`. See [Schema Feature](../../introduction/concepts/schema-feature.md). |
| read_columns | list | no | - | Columns to project from the source. |
| field_delimiter | string | no | `\001` for text, `,` for CSV | Field delimiter for text and CSV files. `delimiter` is an alias. |
| row_delimiter | string | no | `\n` | Row delimiter for text files. |
| skip_header_row_number | long | no | `0` | Number of initial text or CSV rows to skip. |
| encoding | string | no | `UTF-8` | Character encoding for text, JSON, CSV, and XML files. |
| parse_partition_from_path | boolean | no | `true` | Adds partition values parsed from paths such as `/year=2026/month=08`. |
| recursive_file_scan | boolean | no | `true` | Whether to scan subdirectories recursively. |
| file_filter_pattern | string | no | - | File name filter pattern. |
| filename_extension | string | no | - | File extension filter, for example `csv` or `.json`. |
| compress_codec | string | no | `none` | Compression codec for a single compressed file. |
| archive_compress_codec | string | no | `none` | Archive compression codec. |
| enable_file_split | boolean | no | `false` | Enables logical splitting for uncompressed text, CSV, JSON, and Parquet files. |
| file_split_size | long | conditional | `134217728` | Split size in bytes when `enable_file_split=true`. |
| null_format | string | no | - | Text representation of null values. |
| quote_char | string | no | `"` | CSV quote character. |
| escape_char | string | no | - | CSV escape character. |
| sheet_name | string | no | - | Excel worksheet to read. |
| excel_engine | string | no | `POI` | Excel reader: `POI` or `EasyExcel`. |
| poi_excel_max_file_size | long | no | `52428800` | Maximum Excel file size in bytes for the POI engine. |
| xml_row_tag | string | conditional | - | XML element representing one row. |
| xml_use_attr_format | boolean | conditional | - | Whether XML values are read from attributes. |
| discovery_mode | string | no | `once` | `once` or `continuous`. Continuous mode currently requires update sync and binary format. |
| scan_interval | string | no | `10S` | Poll interval for continuous discovery. |
| start_mode | string | no | `earliest` | `earliest` processes existing files; `latest` starts with later changes. |
| sync_mode | string | no | `full` | `full` or `update`. Update mode currently supports binary format only. |
| target_path | string | conditional | - | Required for `sync_mode=update`; used to compare objects by relative path. |
| target_hadoop_conf | map | no | - | Hadoop configuration for the comparison target. |
| update_strategy | string | no | `distcp` | Update comparison strategy: `distcp` or `strict`. |
| compare_mode | string | no | `len_mtime` | `len_mtime` or `checksum`; checksum requires strict strategy. |
| update_compare_parallelism | int | no | `8` | Parallelism for target metadata lookups, from 1 through 64. |
| update_compare_bulk_threshold | int | no | `0` | Positive values enable bulk directory listing at the threshold; `0` disables it. |
| post_sync_action | string | no | `none` | Post-checkpoint action for continuous discovery: `none`, `delete`, or `backup`. |
| backup_path | string | conditional | - | Required for `post_sync_action=backup`. Must not overlap the source path. |
| retention_max_age | string | no | - | Maximum age for SeaTunnel backup objects. |
| retention_check_interval | string | no | `1H` | Backup retention scan interval. |
| common-options | | no | - | See [Source Common Options](../common-options/source-common-options.md). |

## Example

### Read Parquet With ADC

```hocon
source {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/orders"
    file_format_type = "parquet"
  }
}
```

### Read CSV With a Service Account

```hocon
source {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/landing/customers"
    file_format_type = "csv"
    service_account_key_file = "/opt/seatunnel/keys/gcs-reader.json"
    skip_header_row_number = 1
    schema {
      fields {
        id = long
        name = string
      }
    }
    hadoop_gcs_properties = {
      "fs.gs.project.id" = "my-project"
    }
  }
}
```

## Changelog

<ChangeLog />
