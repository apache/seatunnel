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

## Transactional commit prerequisite

Enable hierarchical namespace (HNS) on the ADLS Gen2 storage account for transactional commit
and exactly-once semantics. The commit renames staged files into `path`; without HNS, a
directory rename can copy and delete blobs one by one and leave partial output on failure.
`tmp_path` must be in the same container as `path` so commit uses a rename within that container.

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
| filename_time_format | string | no | `yyyy.MM.dd` | Format of `${now}` in custom filenames. |
| filename_extension | string | no | - | Override the default extension. |
| field_delimiter | string | no | `\001` | Field delimiter for text and CSV. |
| row_delimiter | string | no | `\n` | Row delimiter for text, CSV, and JSON. |
| have_partition | boolean | no | `false` | Write rows into partition directories. |
| partition_by | array | no | - | Fields used to build partition directories. |
| partition_dir_expression | string | no | `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/` | Partition directory expression. |
| is_partition_field_write_in_file | boolean | no | `false` | Include partition fields in output rows. |
| sink_columns | array | no | all fields | Columns written to the file. |
| batch_size | int | no | `1000000` | Maximum rows written before rotating a file. |
| single_file_mode | boolean | no | `false` | Write one file per parallel writer; disables batch-size rotation. |
| create_empty_file_when_no_data | boolean | no | `false` | Create an output file even when the writer receives no rows. |
| compress_codec | string | no | `NONE` | Compression codec for the selected format. |
| is_enable_transaction | boolean | no | `true` | Enable transactional file commit. |
| schema_save_mode | enum | no | `CREATE_SCHEMA_WHEN_NOT_EXIST` | Destination schema handling. |
| data_save_mode | enum | no | `APPEND_DATA` | `APPEND_DATA`, `DROP_DATA`, or `ERROR_WHEN_DATA_EXISTS`. |
| enable_header_write | boolean | no | `false` | Write headers for text and CSV files. |
| encoding | string | no | `UTF-8` | Character encoding for text formats. |
| xml_root_tag | string | no | `RECORDS` | Root tag for XML output. |
| xml_row_tag | string | no | `RECORD` | Row tag for XML output. |
| xml_use_attr_format | boolean | no | - | Write XML data using attributes. |
| parquet_avro_write_fixed_as_int96 | array | no | `[]` | Parquet fields written as INT96 from 12-byte fixed values. |
| parquet_avro_write_timestamp_as_int96 | boolean | no | `false` | Write Parquet timestamps as INT96. |
| multi_table_sink_replica | int | no | `1` | Writer replicas for multi-table sink. |
| date_format | string | no | `yyyy-MM-dd` | Date output format. |
| datetime_format | string | no | `yyyy-MM-dd HH:mm:ss` | Datetime output format. |
| time_format | string | no | `HH:mm:ss` | Time output format. |
| common-options | object | no | - | See [Sink Common Options](../common-options/sink-common-options.md). |

### ADLS configuration rules

`account_name` must contain 3–24 lowercase letters or digits. `container` must be 3–63
characters, start and end with a lowercase letter or digit, and contain only lowercase letters,
digits, and single hyphens. `endpoint_suffix` must be a DNS suffix without a scheme or path.

Authentication modes are exclusive: `SHARED_KEY` requires `account_key` and rejects
`tenant_id`, `client_id`, and `client_secret`; `OAUTH_CLIENT_CREDENTIALS` requires those three
OAuth fields and rejects `account_key`. `tenant_id` must be a GUID or DNS name. `authority_host`
must be an HTTPS origin without a path, query, or fragment (a final `/` is accepted).

`hadoop_adls_properties` is for ABFS tuning. It rejects `fs.defaultFS` and keys beginning with
`fs.abfs`, `fs.s3`, `fs.azure.account.auth.type`, `fs.azure.account.key`,
`fs.azure.account.oauth`, `fs.azure.sas.`, `fs.azure.delegation.`,
`fs.azure.enable.delegation.token`, `fs.azure.identity.`, or `fs.azure.shellkeyprovider.`
(case-insensitive). These routing, credential, and provider settings are connector-owned.

`account_key` and `client_secret` are automatically masked in parsed-configuration logs. This
is log masking, not configuration encryption. The `account_key` log-mask keyword is a global
core-starter default and also applies to other connectors using that option name.

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
