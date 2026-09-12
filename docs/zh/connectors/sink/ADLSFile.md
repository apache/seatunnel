---
sidebar_position: 1
---

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
| authority_host | string | 否 | `https://login.microsoftonline.com` | Microsoft Entra authority 地址。 |
| hadoop_adls_properties | map | 否 | - | 额外的 Hadoop ABFS 属性。 |
| file_format_type | string | 否 | `csv` | 输出文件格式。 |
| tmp_path | string | 否 | `/tmp/seatunnel` | 提交前使用的临时目录。 |
| custom_filename | boolean | 否 | `false` | 是否使用自定义文件名。 |
| file_name_expression | string | 否 | `${transactionId}` | 自定义文件名表达式。 |
| filename_extension | string | 否 | - | 覆盖默认文件扩展名。 |
| preserve_source_filename | boolean | 否 | `false` | 保留上游文件 Source 的原始基本文件名。要求批处理、关闭 checkpoint、Sink 并行度为 `1`，支持一个或多个输入文件。 |
| have_partition | boolean | 否 | `false` | 是否写入分区目录。 |
| partition_by | array | 否 | - | 用于生成分区目录的字段。 |
| sink_columns | array | 否 | 全部字段 | 要写入的字段。 |
| batch_size | int | 否 | `1000000` | 文件轮换前写入的最大行数。 |
| compress_codec | string | 否 | `none` | 文件压缩格式。 |
| is_enable_transaction | boolean | 否 | `true` | 是否启用事务提交。 |
| schema_save_mode | enum | 否 | `CREATE_SCHEMA_WHEN_NOT_EXIST` | 目标 Schema 处理模式。 |
| data_save_mode | enum | 否 | `APPEND_DATA` | `APPEND_DATA`、`DROP_DATA` 或 `ERROR_WHEN_DATA_EXISTS`。 |
| common-options | object | 否 | - | 参见 [Sink Common Options](../common-options/sink-common-options.md)。 |

### 保留源文件名

设置 `preserve_source_filename = true` 后，每个输入文件会使用原始基本文件名写入目标目录。
单文件任务可以指定一个文件，多文件任务可以指定目录并配合 `file_filter_pattern`。Sink 并行度
必须为 `1`。例如 `orders.csv` 和 `customers.csv` 会分别写成同名文件。

该选项不能与 `custom_filename`、`single_file_mode`、`filename_extension` 或
`create_empty_file_when_no_data` 同时使用。不同源路径存在相同基本文件名时，任务会失败，避免
数据被意外合并。

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
    preserve_source_filename = true
  }
}
```

## 变更日志

ADLS Gen2 文件 Sink 初始文档。
