import ChangeLog from '../changelog/connector-file-gcs.md';

# GcsFile

> Google Cloud Storage 文件 Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] 多表 Sink
- [x] 分区输出
- [x] 保存模式
- [x] 文件格式：`text`、`csv`、`parquet`、`orc`、`json`、`excel`、`xml`、`binary`、`canal_json`、`debezium_json` 和 `maxwell_json`

## 描述

通过 Google Cloud Storage Hadoop 连接器向 GCS 写入文件。序列化、分区、文件轮转、
Checkpoint 提交、多表任务和保存模式复用 SeaTunnel 现有的 File Sink 实现。

`bucket` 必须是 `gs://my-bucket` 形式的存储桶 URI。`path` 和 `tmp_path` 是该存储桶内的
前缀，例如 `/warehouse/orders` 和 `/tmp/seatunnel/orders`。不要把对象路径写入 `bucket`。

启用事务时，Writer 首先在 `tmp_path` 下创建文件，并在提交阶段发布到 `path`。配置的身份
需要对两个前缀具有读取、创建、列举和删除对象的权限；使用驱动的原生移动操作时还需要对象移动
权限。事务提交不保证外部读取者能原子地看到
整个目录：Hadoop GCS 连接器可能通过复制和删除操作发布文件。禁用 `is_enable_transaction`
会禁用 File Sink 的事务提交路径。

## 依赖

本连接器使用 `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.33:shaded`。该依赖采用
Apache License 2.0，并以 Java 8 为目标版本。shaded GCS Hadoop 库已打包到
`connector-file-gcs` 连接器 JAR 中。Spark 和 Flink 部署必须在驱动节点和所有工作节点
提供兼容的 Hadoop 3 运行环境。

## 认证

连接器支持以下认证方式：

1. **应用默认凭据（ADC）**：不配置 `service_account_key_file`。Hadoop GCS 连接器会从
   `GOOGLE_APPLICATION_CREDENTIALS` 或 Google Cloud 运行环境绑定的服务账号获取凭据。
2. **服务账号 JSON 文件**：配置 `service_account_key_file`。该本地路径必须以相同位置
   存在于每个写入 GCS 的节点上。

显式配置的 `service_account_key_file` 优先于 `hadoop_gcs_properties` 中的同名 Hadoop
属性。不要把服务账号 JSON 内容写入任务配置或 `hadoop_gcs_properties`。

## Sink 配置项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| path | string | 是 | - | `bucket` 内的目标前缀，例如 `/warehouse/orders`。支持 `${database_name}`、`${schema_name}` 和 `${table_name}` 占位符。 |
| bucket | string | 是 | - | GCS 存储桶 URI，例如 `gs://my-bucket`。 |
| service_account_key_file | string | 否 | - | 每个工作节点上的服务账号 JSON 文件。省略时使用 ADC。 |
| hadoop_gcs_properties | map | 否 | - | 额外的 `fs.gs.*` Hadoop 属性。显式连接器配置优先。 |
| tmp_path | string | 否 | `/tmp/seatunnel` | 事务提交前使用的存储桶内临时前缀，应与 `path` 不同。 |
| file_format_type | string | 否 | `csv` | 输出文件格式。 |
| schema_save_mode | enum | 否 | `CREATE_SCHEMA_WHEN_NOT_EXIST` | 写入前如何准备目标前缀。 |
| data_save_mode | enum | 否 | `APPEND_DATA` | 写入前保留、删除或拒绝已有对象。 |
| custom_filename | boolean | 否 | `false` | 是否启用 `file_name_expression`。 |
| file_name_expression | string | 条件必填 | `${transactionId}` | `custom_filename=true` 时的文件名表达式，支持 `${transactionId}`、`${now}` 和 `${uuid}`。 |
| filename_time_format | string | 否 | `yyyy.MM.dd` | `${now}` 使用的时间格式。 |
| filename_extension | string | 否 | 按格式决定 | 自定义输出文件扩展名。 |
| have_partition | boolean | 否 | `false` | 是否写入分区目录。 |
| partition_by | list | 条件必填 | - | `have_partition=true` 时使用的分区字段。 |
| partition_dir_expression | string | 否 | `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/` | 分区目录表达式。 |
| is_partition_field_write_in_file | boolean | 否 | `false` | 是否在输出行中保留分区字段。 |
| sink_columns | list | 否 | 全部字段 | 写入输出文件的字段及顺序。 |
| is_enable_transaction | boolean | 否 | `true` | 在提交阶段发布临时文件。 |
| batch_size | int | 否 | `1000000` | 单个切分输出文件的最大行数。 |
| single_file_mode | boolean | 否 | `false` | 每个 Sink 并行任务输出一个文件，不支持开启 Checkpoint 或流模式。 |
| create_empty_file_when_no_data | boolean | 否 | `false` | 没有输入数据时创建空文件。 |
| row_delimiter | string | 条件必填 | `\n` | text、CSV 和 JSON 的行分隔符。 |
| field_delimiter | string | 条件必填 | `\001` | text 的字段分隔符。 |
| encoding | string | 条件必填 | `UTF-8` | text、CSV、JSON 和 XML 的编码。 |
| enable_header_write | boolean | 条件必填 | `false` | 为 text 或 CSV 输出写入表头。 |
| compress_codec | string | 否 | `none` | 所选格式支持的压缩编码。 |
| common-options | | 否 | - | 参见 [Sink 通用配置](../common-options/sink-common-options.md)。 |

Sink 也支持对应 SeaTunnel 文件格式定义的格式专用配置，包括 Parquet INT96 和 XML 元素配置。

当 `is_partition_field_write_in_file=true` 时，如果读取这些输出文件的 File Source 的 Schema
已包含分区字段，应设置 `parse_partition_from_path=false`，避免重复添加分区字段。

## 保存模式

`schema_save_mode` 控制目标前缀的创建：

- `RECREATE_SCHEMA`：删除并重新创建前缀。
- `CREATE_SCHEMA_WHEN_NOT_EXIST`：仅当前缀不存在时创建。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：前缀不存在时失败。
- `IGNORE`：不处理前缀。

`data_save_mode` 控制已有对象：

- `APPEND_DATA`：保留已有对象并添加新的输出文件。
- `DROP_DATA`：写入前删除 `path` 下的对象。
- `ERROR_WHEN_DATA_EXISTS`：`path` 下已有对象时失败。

这些操作仅应用于所配置存储桶中的 `path`。选择 `RECREATE_SCHEMA` 或 `DROP_DATA` 时，
GcsFile 会拒绝存储桶根路径；请改用专用前缀。

## 示例

### 使用 ADC 写入 Parquet

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/orders"
    tmp_path = "/tmp/seatunnel/orders"
    file_format_type = "parquet"
    data_save_mode = "APPEND_DATA"
  }
}
```

### 使用服务账号写入分区 CSV

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/exports/customers"
    tmp_path = "/tmp/seatunnel/customers"
    file_format_type = "csv"
    service_account_key_file = "/opt/seatunnel/keys/gcs-writer.json"
    enable_header_write = true
    have_partition = true
    partition_by = ["region"]
    hadoop_gcs_properties = {
      "fs.gs.project.id" = "my-project"
    }
  }
}
```

### 多表输出

使用表占位符把每个上游表写入独立的目标和临时前缀：

```hocon
sink {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/${database_name}/${table_name}"
    tmp_path = "/tmp/seatunnel/${database_name}/${table_name}"
    file_format_type = "parquet"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Changelog

<ChangeLog />
