---
sidebar_position: 1
---

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
| file_format_type | string | 是 | - | 输入文件格式。 |
| account_name | string | 是 | - | ADLS 存储账户名称。 |
| container | string | 是 | - | ADLS Gen2 文件系统（容器）名称。 |
| endpoint_suffix | string | 是 | `dfs.core.windows.net` | 存储端点后缀。 |
| auth_type | enum | 是 | `SHARED_KEY` | `SHARED_KEY` 或 `OAUTH_CLIENT_CREDENTIALS`。 |
| account_key | string | 条件必填 | - | 使用共享密钥认证时必填。 |
| tenant_id | string | 条件必填 | - | Microsoft Entra 租户 ID。 |
| client_id | string | 条件必填 | - | Microsoft Entra 应用程序 ID。 |
| client_secret | string | 条件必填 | - | Microsoft Entra 客户端密钥。 |
| authority_host | string | 否 | `https://login.microsoftonline.com` | Microsoft Entra authority 地址。 |
| hadoop_adls_properties | map | 否 | - | 额外的 Hadoop ABFS 属性。 |
| file_filter_pattern | string | 否 | - | 选择文件的正则表达式。 |
| recursive_file_scan | boolean | 否 | `true` | 是否扫描嵌套目录。 |
| parse_partition_from_path | boolean | 否 | `true` | 从 `key=value` 路径片段解析分区字段。 |
| read_columns | list | 否 | - | 要读取的字段列表。 |
| field_delimiter | string | 否 | `\001`（text）、`,`（CSV） | 文本字段分隔符。 |
| row_delimiter | string | 否 | `\n` | text、CSV、JSON 的行分隔符。 |
| skip_header_row_number | long | 否 | `0` | 跳过的表头行数。 |
| encoding | string | 否 | `UTF-8` | 输入字符编码。 |
| enable_file_split | boolean | 否 | `false` | 是否拆分支持的超大文件。 |
| file_split_size | long | 条件必填 | - | 启用文件拆分时的拆分大小。 |
| discovery_mode | enum | 否 | `ONCE` | 文件发现模式。 |
| start_mode | enum | 否 | `EARLIEST` | 连续发现的起始模式。 |
| sync_mode | enum | 否 | `FULL` | 全量或更新同步模式。 |
| post_sync_action | enum | 否 | `NONE` | 文件同步后的动作。 |
| common-options | object | 否 | - | 参见 [Source Common Options](../common-options/source-common-options.md)。 |

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
