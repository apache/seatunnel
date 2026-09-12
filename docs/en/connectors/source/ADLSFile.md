---
sidebar_position: 1
---

# ADLSFile

> Azure Data Lake Storage Gen2 File Source Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] text, csv, parquet, orc, json, excel, xml, binary, markdown, pdf

## Description

Reads files from an Azure Data Lake Storage Gen2 filesystem through Hadoop ABFS. Use a file path
for one file, or a directory with `file_filter_pattern` and recursive scanning for multiple files.

## Supported DataSource Info

| Datasource | Supported versions |
|------------|--------------------|
| ADLS Gen2  | Current            |

## Dependency

When running with SeaTunnel Zeta, the ADLS runtime compatibility module supplies the Hadoop Azure
client and its transitive dependencies. For Spark or Flink, the cluster must provide a compatible
Hadoop runtime. The ADLS runtime is built and tested with Hadoop Azure 3.2.0.

## Data Type Mapping

For `text`, `csv`, `json`, `excel`, and `xml`, configure `schema` when an explicit row shape is
needed. Parquet and ORC readers can infer their schema from the file. `binary` emits file content
as bytes. Markdown and PDF readers expose structured document fields.

## Options

| name | type | required | default value | Description |
|------|------|----------|---------------|-------------|
| path | string | yes | - | ADLS path or directory to read. |
| file_format_type | string | yes | - | Input format. |
| account_name | string | yes | - | ADLS storage account name. |
| container | string | yes | - | ADLS Gen2 filesystem (container) name. |
| endpoint_suffix | string | yes | `dfs.core.windows.net` | Storage endpoint suffix. |
| auth_type | enum | yes | `SHARED_KEY` | `SHARED_KEY` or `OAUTH_CLIENT_CREDENTIALS`. |
| account_key | string | conditional | - | Required for `SHARED_KEY`. |
| tenant_id | string | conditional | - | Microsoft Entra tenant ID. |
| client_id | string | conditional | - | Microsoft Entra application/client ID. |
| client_secret | string | conditional | - | Microsoft Entra client secret. |
| authority_host | string | no | `https://login.microsoftonline.com` | Microsoft Entra authority host. |
| hadoop_adls_properties | map | no | - | Additional Hadoop ABFS properties. |
| file_filter_pattern | string | no | - | Regular expression used to select files. |
| recursive_file_scan | boolean | no | `true` | Scan nested directories. |
| parse_partition_from_path | boolean | no | `true` | Derive partition fields from `key=value` path segments. |
| read_columns | list | no | - | Project selected columns. |
| field_delimiter | string | no | `\001` for text, `,` for CSV | Field delimiter for text formats. |
| row_delimiter | string | no | `\n` | Row delimiter for text, CSV, and JSON. |
| skip_header_row_number | long | no | `0` | Number of header rows to skip. |
| encoding | string | no | `UTF-8` | Input character encoding. |
| enable_file_split | boolean | no | `false` | Split supported large files for parallel reading. |
| file_split_size | long | conditional | - | Split size when file splitting is enabled. |
| discovery_mode | enum | no | `CONTINUOUS` | File discovery mode. |
| start_mode | enum | no | - | Starting point for continuous discovery. |
| sync_mode | enum | no | `FULL` | Full or update synchronization mode. |
| post_sync_action | enum | no | `NONE` | Action after a file is synchronized. |
| common-options | object | no | - | See [Source Common Options](../common-options/source-common-options.md). |

### Authentication examples

Shared key:

```hocon
ADLSFile {
  path = "/landing/input"
  account_name = "mystorageaccount"
  container = "data"
  auth_type = "SHARED_KEY"
  account_key = ${?ADLS_ACCOUNT_KEY}
  file_format_type = "csv"
  skip_header_row_number = 1
  schema { fields { id = bigint, name = string } }
}
```

Microsoft Entra client credentials use `auth_type = "OAUTH_CLIENT_CREDENTIALS"` with `tenant_id`,
`client_id`, and `client_secret`.

## Example

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  ADLSFile {
    path = "/landing/input"
    file_filter_pattern = ".*\\.csv"
    recursive_file_scan = true
    account_name = "mystorageaccount"
    container = "data"
    auth_type = "SHARED_KEY"
    account_key = ${?ADLS_ACCOUNT_KEY}
    file_format_type = "csv"
    skip_header_row_number = 1
    schema { fields { id = bigint, name = string, amount = double } }
  }
}

sink {
  Console { }
}
```

## Changelog

Initial ADLS Gen2 file source documentation.
