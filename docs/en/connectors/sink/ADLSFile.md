import ChangeLog from '../changelog/connector-file-adls.md';

# ADLSFile

> Azure Data Lake Storage Gen2 File Sink Connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] support multiple table write
- [x] text, csv, parquet, orc, json, excel, xml, binary, canal_json, debezium_json, maxwell_json

## Description

Writes files to an Azure Data Lake Storage Gen2 filesystem through Hadoop ABFS. The connector
uses the secure `abfss://` endpoint and supports shared-key and Microsoft Entra client-credential
authentication.

## Supported DataSource Info

| Datasource | Supported versions |
|------------|--------------------|
| ADLS Gen2  | Current            |

## Dependency

When running with SeaTunnel Zeta, the ADLS runtime compatibility module supplies the Hadoop Azure
client and its transitive dependencies. For Spark or Flink, the cluster must provide a compatible
Hadoop runtime. The ADLS runtime is built and tested with Hadoop Azure 3.2.0.

## Data Type Mapping

For `text` and `csv`, values are written as delimited text. For `parquet`, `orc`, `json`, `excel`,
and `xml`, the connector uses the corresponding SeaTunnel file writer and schema. `binary` writes
the input bytes without interpreting their contents.

## Sink Options

| name | type | required | default value | Description |
|------|------|----------|---------------|-------------|
| path | string | yes | - | Destination directory inside the ADLS filesystem. |
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
| file_format_type | string | no | `csv` | Output format. |
| tmp_path | string | no | `/tmp/seatunnel` | Staging directory used before commit. |
| custom_filename | boolean | no | `false` | Generate a configured filename. |
| file_name_expression | string | no | `${transactionId}` | Filename expression when custom naming is enabled. |
| filename_extension | string | no | - | Override the default extension. |
| have_partition | boolean | no | `false` | Write rows into partition directories. |
| partition_by | array | no | - | Fields used to build partition directories. |
| partition_dir_expression | string | no | `${k0}=${v0}/...` | Partition directory expression. |
| sink_columns | array | no | all fields | Columns written to the file. |
| batch_size | int | no | `1000000` | Maximum rows written before rotating a file. |
| compress_codec | string | no | `none` | Compression codec for the selected format. |
| is_enable_transaction | boolean | no | `true` | Enable transactional file commit. |
| schema_save_mode | enum | no | `CREATE_SCHEMA_WHEN_NOT_EXIST` | Destination schema handling. |
| data_save_mode | enum | no | `APPEND_DATA` | `APPEND_DATA`, `DROP_DATA`, or `ERROR_WHEN_DATA_EXISTS`. |
| enable_header_write | boolean | no | `false` | Write headers for text and CSV files. |
| encoding | string | no | `UTF-8` | Character encoding for text formats. |
| common-options | object | no | - | See [Sink Common Options](../common-options/sink-common-options.md). |

### Authentication examples

```hocon
ADLSFile {
  path = "/landing"
  account_name = "mystorageaccount"
  container = "data"
  auth_type = "SHARED_KEY"
  account_key = ${?ADLS_ACCOUNT_KEY}
  file_format_type = "parquet"
}
```

For Microsoft Entra client credentials, use `auth_type = "OAUTH_CLIENT_CREDENTIALS"` with
`tenant_id`, `client_id`, and `client_secret`.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/data/input"
    file_filter_pattern = ".*\\.csv"
    file_format_type = "csv"
    skip_header_row_number = 1
    schema { fields { id = bigint, name = string, amount = double } }
  }
}

sink {
  ADLSFile {
    parallelism = 1
    path = "/landing"
    account_name = "mystorageaccount"
    container = "data"
    auth_type = "SHARED_KEY"
    account_key = ${?ADLS_ACCOUNT_KEY}
    file_format_type = "csv"
  }
}
```

## Changelog

Initial ADLS Gen2 file sink documentation.
<ChangeLog />
