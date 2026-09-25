import ChangeLog from '../changelog/connector-file-adls.md';

# ADLSFile

> Azure Data Lake Storage Gen2 文件 Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] 批处理
- [x] 多模态文件写入
- [x] 事务提交和 exactly-once
- [x] 多表写入
- [x] text、csv、parquet、orc、json、excel、xml、binary、canal_json、debezium_json、maxwell_json

## 描述

通过 Hadoop ABFS 将文件写入 Azure Data Lake Storage Gen2 文件系统。支持 `abfss://` 安全端点、
共享密钥和 Microsoft Entra 客户端凭据认证。

## 依赖

使用 SeaTunnel Zeta 时，ADLS runtime 模块会提供 Hadoop Azure 客户端及其依赖。使用 Spark 或
Flink 时，集群需要提供兼容的 Hadoop runtime。本连接器使用 Hadoop Azure 3.2.0 构建和测试。

## 事务提交前提

ADLS Gen2 存储账户必须启用层级命名空间（HNS），才能保证事务提交和 exactly-once 语义。提交时会将
临时目录重命名到 `path`；未启用 HNS 时，目录重命名可能逐个复制并删除 blob，故障时会留下部分输出。
`tmp_path` 必须与 `path` 位于同一容器，才能在容器内通过重命名提交。

## Sink 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| path | string | 是 | - | ADLS 文件系统中的目标目录。 |
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
| file_format_type | enum | 否 | `csv` | 输出文件格式。 |
| tmp_path | string | 否 | `/tmp/seatunnel` | 提交前使用的临时目录。 |
| custom_filename | boolean | 否 | `false` | 是否使用自定义文件名。 |
| file_name_expression | string | 否 | `${transactionId}` | 自定义文件名表达式。 |
| filename_time_format | string | 否 | `yyyy.MM.dd` | 自定义文件名中 `${now}` 的时间格式。 |
| filename_extension | string | 否 | - | 覆盖默认文件扩展名。 |
| field_delimiter | string | 否 | `\001` | text 输出的字段分隔符。 |
| row_delimiter | string | 否 | `\n` | text、CSV 和 JSON 的行分隔符。 |
| have_partition | boolean | 否 | `false` | 是否写入分区目录。 |
| partition_by | array | 否 | - | 用于生成分区目录的字段。 |
| partition_dir_expression | string | 否 | `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/` | 分区目录表达式。 |
| is_partition_field_write_in_file | boolean | 否 | `false` | 输出行中是否包含分区字段。 |
| sink_columns | array | 否 | 全部字段 | 要写入的字段。 |
| batch_size | int | 否 | `1000000` | 文件轮换前写入的最大行数。 |
| single_file_mode | boolean | 否 | `false` | 每个并行写入器只输出一个文件，不按批大小轮换。 |
| create_empty_file_when_no_data | boolean | 否 | `false` | 没有输入行时仍创建输出文件。 |
| compress_codec | enum | 否 | `NONE` | 文件压缩格式。 |
| is_enable_transaction | boolean | 否 | `true` | 是否启用事务提交。 |
| schema_save_mode | enum | 否 | `CREATE_SCHEMA_WHEN_NOT_EXIST` | 目标 Schema 处理模式。 |
| data_save_mode | enum | 否 | `APPEND_DATA` | `APPEND_DATA`、`DROP_DATA` 或 `ERROR_WHEN_DATA_EXISTS`。 |
| enable_header_write | boolean | 否 | `false` | 为 text 和 CSV 文件写入表头。 |
| encoding | string | 否 | `UTF-8` | 文本文件字符编码。 |
| xml_root_tag | string | 否 | `RECORDS` | XML 输出的根标签。 |
| xml_row_tag | string | 否 | `RECORD` | XML 输出的数据行标签。 |
| xml_use_attr_format | boolean | 否 | - | 使用属性形式写入 XML 数据。 |
| parquet_avro_write_fixed_as_int96 | array | 否 | `[]` | 将 12 字节固定值写为 Parquet INT96 的字段。 |
| parquet_avro_write_timestamp_as_int96 | boolean | 否 | `false` | 将 Parquet 时间戳写为 INT96。 |
| multi_table_sink_replica | int | 否 | `1` | 多表写入器的副本数。 |
| date_format | string | 否 | `yyyy-MM-dd` | 日期输出格式。 |
| datetime_format | string | 否 | `yyyy-MM-dd HH:mm:ss` | 日期时间输出格式。 |
| time_format | string | 否 | `HH:mm:ss` | 时间输出格式。 |
| common-options | object | 否 | - | 参见 [Sink Common Options](../common-options/sink-common-options.md)。 |

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

## 变更日志

ADLS Gen2 文件 Sink 初始文档。
<ChangeLog />
