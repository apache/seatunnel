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
| hadoop_adls_properties | map | no | - | Additional Hadoop ABFS properties supplied by trusted operators. See ADLS configuration rules below. |
| file_filter_pattern | string | no | - | Regular expression used to select files. |
| recursive_file_scan | boolean | no | `true` | Scan nested directories. |
| parse_partition_from_path | boolean | no | `true` | Derive partition fields from `key=value` path segments. |
| read_columns | list | no | - | Project selected columns. |
| schema | object | conditional | - | Row schema for text, JSON, Excel, CSV, and XML inputs. |
| metalake_type | enum | no | `GRAVITINO` | Metadata lake used to resolve table schemas. |
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

`authority_host` is a trust boundary: validation checks URL syntax but does not restrict the
destination host. OAuth sends `client_id` and `client_secret` to the token endpoint under this
origin. Only trusted operators should set this option, using the Microsoft Entra authority for
their Azure cloud. Do not accept it from untrusted job input.

Leading and trailing whitespace is trimmed from each `hadoop_adls_properties` value before it is
passed to Hadoop.

`hadoop_adls_properties` is for ABFS tuning. It rejects `fs.defaultFS` and keys beginning with
`fs.abfs`, `fs.azure.account.auth.type`, `fs.azure.account.key`,
`fs.azure.account.oauth`, `fs.azure.sas.`, `fs.azure.delegation.`,
`fs.azure.enable.delegation.token`, `fs.azure.identity.`, `fs.azure.shellkeyprovider.`,
`abfs.external.authorization.class`, `fs.azure.abfs.endpoint`, `fs.azure.custom.token.provider.class`,
`fs.azure.encryption.context.provider.type`, `fs.azure.encryption.encoded.client-provided-key`,
`fs.azure.metrics.account.key`, or `hadoop.security.credential.provider.path` (case-insensitive).
These routing, credential, and provider settings are connector-owned.

Validation uses an exact-key/prefix denylist. The denylist is not exhaustive: other Hadoop
settings can still load classes or select credential/token providers. Only trusted operators
should supply `hadoop_adls_properties`. Do not accept this map from untrusted job input.

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

## Live ABFS/Gen2 integration test

`ADLSFileIT` in `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e` tests both
the ADLSFile source and sink against real ADLS Gen2 storage over secure ABFS (`abfss://`).
Default CI uses the separate WASB/Azurite test; the live test is opt-in because it needs Azure
resources and credentials. An Azurite test result does not verify live ABFS/Gen2 I/O.

### 1. Prepare Azure storage and the local environment

- Use a dedicated [ADLS Gen2 storage account with hierarchical namespace enabled](https://learn.microsoft.com/en-us/azure/storage/blobs/create-data-lake-storage-account)
  and create a filesystem (container) for testing.
- Obtain a [storage account access key](https://learn.microsoft.com/en-us/azure/storage/common/storage-account-keys-manage).
  Use the key value, not a connection string or SAS token. Shared Key authorization must be
  allowed on this test account. The existing live test uses `SHARED_KEY`; it does not exercise
  OAuth client credentials.
- The fixtures use `dfs.core.windows.net`. This procedure targets Azure public cloud.
  Both the host running Maven and the Docker containers must be able to reach the storage
  endpoints; account firewall rules must permit that access.
- Install JDK 11 or 17, set `JAVA_HOME`, and start Docker with Linux containers enabled.
  See the [development setup guide](../../developer/setup.md) for build prerequisites.

Choose a dedicated, absolute container-relative test prefix, such as `/seatunnel-e2e/alice/run-001`.
The test recursively deletes `<prefix>/file-round-trip-e2e` before setup and during teardown.
Do not place existing data there, and use different prefixes for concurrent runs.

### 2. Build the checkout

Run from the repository root. Build before setting the live-test variables:

```bash
./mvnw clean install -DskipUT=true -DskipIT=true
```

In Windows PowerShell, use `./mvnw.cmd` instead of `./mvnw` for this and the later Maven command.
The build prepares the connector, runtime, engine, and E2E dependencies used by the test containers.

### 3. Set the live-test variables

Set these variables in the same terminal that will run Maven:

| Variable | Value |
|----------|-------|
| `SEATUNNEL_ADLS_IT` | `true` to enable `ADLSFileIT`. |
| `SEATUNNEL_ADLS_ACCOUNT` | Storage account name, without a URL or endpoint suffix. |
| `SEATUNNEL_ADLS_CONTAINER` | Name of the existing test filesystem/container. |
| `SEATUNNEL_ADLS_ACCOUNT_KEY` | Storage account access key. |
| `SEATUNNEL_ADLS_TEST_PREFIX` | Dedicated absolute path inside the container, without `.` or `..` segments. |
| `RUN_ENGINES` | `zeta` for this walkthrough. |

Bash (replace the account, container, and prefix examples):

```bash
export SEATUNNEL_ADLS_IT=true
export SEATUNNEL_ADLS_ACCOUNT=mystorageaccount
export SEATUNNEL_ADLS_CONTAINER=seatunnel-tests
export SEATUNNEL_ADLS_TEST_PREFIX=/seatunnel-e2e/alice/run-001
export RUN_ENGINES=zeta
read -r -s -p 'Storage account access key: ' SEATUNNEL_ADLS_ACCOUNT_KEY
printf '\n'
export SEATUNNEL_ADLS_ACCOUNT_KEY
```

Windows PowerShell:

```powershell
$env:SEATUNNEL_ADLS_IT = 'true'
$env:SEATUNNEL_ADLS_ACCOUNT = 'mystorageaccount'
$env:SEATUNNEL_ADLS_CONTAINER = 'seatunnel-tests'
$env:SEATUNNEL_ADLS_TEST_PREFIX = '/seatunnel-e2e/alice/run-001'
$env:RUN_ENGINES = 'zeta'
$adlsTestKey = Read-Host 'Storage account access key' -AsSecureString
$env:SEATUNNEL_ADLS_ACCOUNT_KEY = [System.Net.NetworkCredential]::new('', $adlsTestKey).Password
Remove-Variable adlsTestKey
```

The prompts avoid putting the key in command history. For CI, inject it from a secret store.
The test forwards the account, container, key, and generated test root to the engine containers;
no interactive Azure Portal login is needed during execution.

### 4. Run the live test

From the repository root, in the same terminal:

```bash
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e -DskipUT=true -DskipIT=false -Dit.test=ADLSFileIT verify
```

Use `RUN_ENGINES=spark` for the eligible Spark containers, or `all` for all eligible containers.
The test excludes Spark 2.4 and Flink containers because their Hadoop runtime is incompatible
with this connector's Hadoop 3 Azure runtime.

### 5. Check the result and clean up

The test creates two input CSV files containing four records, runs the ADLSFile source-to-sink
job, verifies output creation and removal of a stale output file, checks that no temporary
files remain, and reads the output back through ADLSFile into an Assert sink. The Assert sink
checks four records, non-null fields, and ID bounds. Teardown deletes the generated test root.

Inspect `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-file-adls-e2e/target/failsafe-reports/`
for the `ADLSFileIT` text/XML report. Confirm that the test actually ran with no failures or errors;
a skipped test or build success alone does not establish live ABFS coverage.
If skipped, check `SEATUNNEL_ADLS_IT` and `-DskipIT=false`. For authentication or connection
failures, check the account/key, Shared Key authorization, container name, and network access.

If the process is interrupted, inspect and remove only your dedicated
`<prefix>/file-round-trip-e2e` directory if cleanup did not finish. Close the terminal or remove
the test environment variables afterward, especially `SEATUNNEL_ADLS_ACCOUNT_KEY` and
`SEATUNNEL_ADLS_IT`.

## Changelog

Initial ADLS Gen2 file source documentation.

<ChangeLog />
