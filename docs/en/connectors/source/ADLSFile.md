import ChangeLog from '../changelog/connector-file-adls.md';

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
| file_format_type | enum | yes | - | Input format. |
| account_name | string | yes | - | ADLS storage account name. |
| container | string | yes | - | ADLS Gen2 filesystem (container) name. |
| endpoint_suffix | string | yes | `dfs.core.windows.net` | Storage endpoint suffix. |
| auth_type | enum | yes | `SHARED_KEY` | `SHARED_KEY` or `OAUTH_CLIENT_CREDENTIALS`. |
| account_key | string | conditional | - | Required for `SHARED_KEY`. |
| tenant_id | string | conditional | - | Microsoft Entra tenant ID. |
| client_id | string | conditional | - | Microsoft Entra application/client ID. |
| client_secret | string | conditional | - | Microsoft Entra client secret. |
| authority_host | string | no | `https://login.microsoftonline.com` | HTTPS authority origin used for OAuth client credentials. |
| hadoop_adls_properties | map | no | - | Additional Hadoop ABFS properties. |
| file_filter_pattern | string | no | - | Regular expression used to select files. |
| recursive_file_scan | boolean | no | `true` | Scan nested directories. |
| parse_partition_from_path | boolean | no | `true` | Derive partition fields from `key=value` path segments. |
| read_columns | list | no | - | Project selected columns. |
| schema | object | conditional | - | Row schema for text, JSON, Excel, CSV, and XML inputs. |
| field_delimiter | string | no | `\001` | Field delimiter for text formats. |
| row_delimiter | string | no | `\n` | Row delimiter for text input. |
| skip_header_row_number | long | no | `0` | Number of header rows to skip. |
| encoding | string | no | `UTF-8` | Input character encoding. |
| enable_file_split | boolean | no | `false` | Split supported large files for parallel reading. |
| file_split_size | long | conditional | `134217728` | Split size in bytes when file splitting is enabled. |
| discovery_mode | enum | no | `ONCE` | File discovery mode. |
| scan_interval | duration | no | `10S` | Polling interval in continuous discovery. |
| start_mode | enum | no | `EARLIEST` | Starting point for continuous discovery. |
| sync_mode | enum | no | `FULL` | Full or update synchronization mode. |
| post_sync_action | enum | no | `NONE` | Action after a file is synchronized. |
| target_path | string | conditional | - | Target path to compare when `sync_mode=UPDATE`. |
| target_hadoop_conf | map | no | - | Hadoop settings for the update comparison target. |
| update_strategy | enum | no | `DISTCP` | Update comparison strategy; `DISTCP` or `STRICT`. |
| compare_mode | enum | no | `LEN_MTIME` | Compare length and modification time, or checksum with `STRICT`. |
| update_compare_parallelism | int | no | `8` | Parallel target metadata lookups. |
| update_compare_bulk_threshold | int | no | `0` | Candidate count that triggers directory listing; zero disables it. |
| backup_path | string | conditional | - | Destination when `post_sync_action=BACKUP`. |
| retention_max_age | duration | no | - | Maximum age of backed-up files before cleanup. |
| retention_check_interval | duration | no | `1H` | Backup retention scan interval. |
| null_format | string | no | - | String representing a null value. |
| quote_char | string | no | `"` | Character enclosing CSV fields. |
| escape_char | string | no | - | Escape character for CSV fields. |
| sheet_name | string | no | - | Excel sheet to read. |
| excel_engine | enum | no | `POI` | Excel reader, `POI` or `EasyExcel`. |
| poi_excel_max_file_size | long | no | `52428800` | Maximum Excel file size in bytes for POI. |
| compress_codec | enum | no | `NONE` | Input compression codec. |
| archive_compress_codec | enum | no | `NONE` | Archive compression codec. |
| xml_row_tag | string | conditional | - | Row tag for XML input. |
| xml_use_attr_format | boolean | conditional | - | Read XML data from attributes. |
| markdown_rag_metadata_enabled | boolean | no | `false` | Append RAG metadata for Markdown input. |
| pdf_rag_metadata_enabled | boolean | no | `false` | Append RAG metadata for PDF input. |
| filename_extension | string | no | - | Filter by filename extension. |
| date_format | string | no | `yyyy-MM-dd` | Date parsing format. |
| datetime_format | string | no | `yyyy-MM-dd HH:mm:ss` | Datetime parsing format. |
| time_format | string | no | `HH:mm:ss` | Time parsing format. |
| common-options | object | no | - | See [Source Common Options](../common-options/source-common-options.md). |

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

<ChangeLog />
