import ChangeLog from '../changelog/connector-file-adls.md';

# ADLSFile

> Azure Data Lake Storage Gen2 文件 Source 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] 批处理和流处理
- [x] 多模态文件读取
- [x] exactly-once、列投影和并行读取
- [x] text、csv、parquet、orc、json、excel、xml、binary、markdown、pdf

## 描述

通过 Hadoop ABFS 从 Azure Data Lake Storage Gen2 文件系统读取文件。`path` 可以指向单个文件，
也可以指向目录，并使用 `file_filter_pattern` 和递归扫描读取多个文件。

## 依赖

使用 SeaTunnel Zeta 时，ADLS runtime 模块会提供 Hadoop Azure 客户端及其依赖。使用 Spark 或
Flink 时，集群需要提供兼容的 Hadoop runtime。本连接器使用 Hadoop Azure 3.2.0 构建和测试。

## Source 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| path | string | 是 | - | 要读取的 ADLS 文件或目录。 |
| file_format_type | enum | 是 | - | 输入文件格式。 |
| account_name | string | 是 | - | ADLS 存储账户名称。 |
| container | string | 是 | - | ADLS Gen2 文件系统（容器）名称。 |
| endpoint_suffix | string | 是 | `dfs.core.windows.net` | 存储端点后缀。 |
| auth_type | enum | 是 | `SHARED_KEY` | `SHARED_KEY` 或 `OAUTH_CLIENT_CREDENTIALS`。 |
| account_key | string | 条件必填 | - | 使用共享密钥认证时必填。 |
| tenant_id | string | 条件必填 | - | Microsoft Entra 租户 ID。 |
| client_id | string | 条件必填 | - | Microsoft Entra 应用程序 ID。 |
| client_secret | string | 条件必填 | - | Microsoft Entra 客户端密钥。 |
| authority_host | string | 否 | `https://login.microsoftonline.com` | OAuth 客户端凭据认证使用的 HTTPS authority 源站。 |
| hadoop_adls_properties | map | 否 | - | 额外的 Hadoop ABFS 属性。 |
| file_filter_pattern | string | 否 | - | 选择文件的正则表达式。 |
| recursive_file_scan | boolean | 否 | `true` | 是否扫描嵌套目录。 |
| parse_partition_from_path | boolean | 否 | `true` | 从 `key=value` 路径片段解析分区字段。 |
| read_columns | list | 否 | - | 要读取的字段列表。 |
| schema | object | 条件必填 | - | text、JSON、Excel、CSV 和 XML 输入的行 Schema。 |
| field_delimiter | string | 否 | `\001` | 文本字段分隔符。 |
| row_delimiter | string | 否 | `\n` | text 输入的行分隔符。 |
| skip_header_row_number | long | 否 | `0` | 跳过的表头行数。 |
| encoding | string | 否 | `UTF-8` | 输入字符编码。 |
| enable_file_split | boolean | 否 | `false` | 是否拆分支持的超大文件。 |
| file_split_size | long | 条件必填 | `134217728` | 启用文件拆分时的拆分大小（字节）。 |
| discovery_mode | enum | 否 | `ONCE` | 文件发现模式。 |
| scan_interval | duration | 否 | `10S` | 连续发现时的扫描间隔。 |
| start_mode | enum | 否 | `EARLIEST` | 连续发现的起始模式。 |
| sync_mode | enum | 否 | `FULL` | 全量或更新同步模式。 |
| post_sync_action | enum | 否 | `NONE` | 文件同步后的动作。 |
| target_path | string | 条件必填 | - | `sync_mode=UPDATE` 时用于比较的目标路径。 |
| target_hadoop_conf | map | 否 | - | 更新比较目标的 Hadoop 配置。 |
| update_strategy | enum | 否 | `DISTCP` | 更新比较策略：`DISTCP` 或 `STRICT`。 |
| compare_mode | enum | 否 | `LEN_MTIME` | 按长度与修改时间比较；`STRICT` 模式也可使用校验和。 |
| update_compare_parallelism | int | 否 | `8` | 目标元数据查询的并行度。 |
| update_compare_bulk_threshold | int | 否 | `0` | 触发目录列表比较的候选数量；零表示禁用。 |
| backup_path | string | 条件必填 | - | `post_sync_action=BACKUP` 时的备份目录。 |
| retention_max_age | duration | 否 | - | 备份文件清理前的最大保留时间。 |
| retention_check_interval | duration | 否 | `1H` | 备份保留期的扫描间隔。 |
| null_format | string | 否 | - | 表示空值的字符串。 |
| quote_char | string | 否 | `"` | CSV 字段的引号字符。 |
| escape_char | string | 否 | - | CSV 字段的转义字符。 |
| sheet_name | string | 否 | - | 要读取的 Excel 工作表。 |
| excel_engine | enum | 否 | `POI` | Excel 读取引擎：`POI` 或 `EasyExcel`。 |
| poi_excel_max_file_size | long | 否 | `52428800` | POI 可读取的最大 Excel 文件大小（字节）。 |
| compress_codec | enum | 否 | `NONE` | 输入文件压缩格式。 |
| archive_compress_codec | enum | 否 | `NONE` | 归档文件压缩格式。 |
| xml_row_tag | string | 条件必填 | - | XML 文件的数据行标签。 |
| xml_use_attr_format | boolean | 条件必填 | - | 是否从 XML 属性读取数据。 |
| markdown_rag_metadata_enabled | boolean | 否 | `false` | 读取 Markdown 时附加 RAG 元数据。 |
| pdf_rag_metadata_enabled | boolean | 否 | `false` | 读取 PDF 时附加 RAG 元数据。 |
| filename_extension | string | 否 | - | 按文件扩展名过滤。 |
| date_format | string | 否 | `yyyy-MM-dd` | 日期解析格式。 |
| datetime_format | string | 否 | `yyyy-MM-dd HH:mm:ss` | 日期时间解析格式。 |
| time_format | string | 否 | `HH:mm:ss` | 时间解析格式。 |
| common-options | object | 否 | - | 参见 [Source Common Options](../common-options/source-common-options.md)。 |

### ADLS 配置规则

`account_name` 只能包含 3–24 个小写字母或数字。`container` 长度为 3–63 个字符，必须以小写字母或
数字开头和结尾，只能使用小写字母、数字和不连续的连字符。`endpoint_suffix` 必须是没有协议和路径的
DNS 后缀。

两种认证方式互斥：`SHARED_KEY` 要求 `account_key`，不允许设置 `tenant_id`、`client_id` 和
`client_secret`；`OAUTH_CLIENT_CREDENTIALS` 要求后三项，不允许设置 `account_key`。
`tenant_id` 必须是 GUID 或 DNS 名称。`authority_host` 必须是没有路径、查询参数和片段的 HTTPS
地址（允许末尾有一个 `/`）。

`hadoop_adls_properties` 仅用于 ABFS 参数调整。不允许 `fs.defaultFS`，以及以下前缀开头的键：
`fs.abfs`、`fs.s3`、`fs.azure.account.auth.type`、`fs.azure.account.key`、
`fs.azure.account.oauth`、`fs.azure.sas.`、`fs.azure.delegation.`、
`fs.azure.enable.delegation.token`、`fs.azure.identity.`、`fs.azure.shellkeyprovider.`。
匹配时不区分大小写；这些路由、凭据和提供程序设置由连接器管理。

解析配置时，日志会自动遮盖 `account_key` 和 `client_secret`。这只是日志遮盖，不是配置加密。
`account_key` 是 core-starter 的全局日志遮盖关键字，也会影响其他使用同名选项的连接器。

## 示例

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

使用 Microsoft Entra 客户端凭据时，将 `auth_type` 设置为 `OAUTH_CLIENT_CREDENTIALS`，并提供
`tenant_id`、`client_id` 和 `client_secret`。

## 变更日志

ADLS Gen2 文件 Source 初始文档。

<ChangeLog />
