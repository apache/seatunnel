import ChangeLog from '../changelog/connector-file-gcs.md';

# GcsFile

> Google Cloud Storage 文件 Source 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#multimodal)
- [x] 多表 Source
- [x] 文件格式：`text`、`csv`、`parquet`、`orc`、`json`、`excel`、`xml`、`binary`、`markdown` 和 `pdf`

## 描述

通过 Google Cloud Storage Hadoop 连接器读取 GCS 文件。格式解析、Schema 发现、列投影、文件切分和多表任务复用 SeaTunnel File Source 的现有实现。

`bucket` 必须是 `gs://my-bucket` 形式的存储桶 URI。`path` 是存储桶内的对象或前缀，例如 `/data/orders`。不要把对象路径写入 `bucket`。

## 依赖

本连接器使用 `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.33:shaded`。该依赖采用 Apache License 2.0，并以 Java 8 为目标版本。shaded GCS Hadoop 库已打包到 `connector-file-gcs` 连接器 JAR 中。Spark 和 Flink 部署必须在驱动节点和所有工作节点提供兼容的 Hadoop 3 运行环境。

## 认证

连接器支持以下认证方式：

1. **应用默认凭据（ADC）**：不配置 `service_account_key_file`。Hadoop GCS 连接器会从 `GOOGLE_APPLICATION_CREDENTIALS` 或 Google Cloud 运行环境绑定的服务账号获取凭据。
2. **服务账号 JSON 文件**：配置 `service_account_key_file`。该本地路径必须以相同位置存在于每个读取 GCS 的节点上。

显式配置的 `service_account_key_file` 优先于 `hadoop_gcs_properties` 中的同名 Hadoop 属性。

## 配置项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| path | string | 是 | - | `bucket` 内的对象或前缀路径，例如 `/data/orders`。 |
| file_format_type | string | 是 | - | 文件格式：`text`、`csv`、`parquet`、`orc`、`json`、`excel`、`xml`、`binary`、`markdown` 或 `pdf`。 |
| bucket | string | 是 | - | GCS 存储桶 URI，例如 `gs://my-bucket`。 |
| service_account_key_file | string | 否 | - | 每个工作节点上的服务账号 JSON 文件。省略时使用 ADC。 |
| hadoop_gcs_properties | map | 否 | - | 额外的 `fs.gs.*` Hadoop 属性。显式连接器配置优先。 |
| schema | config | 条件必填 | - | `text`、`json`、`excel`、`csv` 和 `xml` 格式需要配置。参见 [Schema 功能](../../introduction/concepts/schema-feature.md)。 |
| read_columns | list | 否 | - | 从数据源投影的列。 |
| field_delimiter | string | 否 | text 为 `\001`，CSV 为 `,` | text 和 CSV 的字段分隔符，`delimiter` 是其别名。 |
| row_delimiter | string | 否 | `\n` | text 文件的行分隔符。 |
| skip_header_row_number | long | 否 | `0` | 跳过 text 或 CSV 文件开头的行数。 |
| encoding | string | 否 | `UTF-8` | text、JSON、CSV 和 XML 的字符编码。 |
| parse_partition_from_path | boolean | 否 | `true` | 从 `/year=2026/month=08` 形式的路径解析分区值。 |
| recursive_file_scan | boolean | 否 | `true` | 是否递归扫描子目录。 |
| file_filter_pattern | string | 否 | - | 文件名过滤模式。 |
| filename_extension | string | 否 | - | 文件扩展名过滤，例如 `csv` 或 `.json`。 |
| compress_codec | string | 否 | `none` | 单个压缩文件的压缩编码。 |
| archive_compress_codec | string | 否 | `none` | 归档压缩编码。 |
| enable_file_split | boolean | 否 | `false` | 为未压缩的 text、CSV、JSON 和 Parquet 文件启用逻辑切分。 |
| file_split_size | long | 条件必填 | `134217728` | `enable_file_split=true` 时的切分大小，单位为字节。 |
| null_format | string | 否 | - | 表示空值的文本。 |
| quote_char | string | 否 | `"` | CSV 引号字符。 |
| escape_char | string | 否 | - | CSV 转义字符。 |
| sheet_name | string | 否 | - | 要读取的 Excel 工作表。 |
| excel_engine | string | 否 | `POI` | Excel 读取器：`POI` 或 `EasyExcel`。 |
| poi_excel_max_file_size | long | 否 | `52428800` | POI 引擎可读取的最大 Excel 文件字节数。 |
| xml_row_tag | string | 条件必填 | - | 表示一行数据的 XML 元素。 |
| xml_use_attr_format | boolean | 条件必填 | - | 是否从 XML 属性读取值。 |
| discovery_mode | string | 否 | `once` | `once` 或 `continuous`。连续发现当前要求 update 同步和 binary 格式。 |
| scan_interval | string | 否 | `10S` | 连续发现的轮询间隔。 |
| start_mode | string | 否 | `earliest` | `earliest` 处理已有文件，`latest` 从后续变更开始。 |
| sync_mode | string | 否 | `full` | `full` 或 `update`。update 当前仅支持 binary 格式。 |
| target_path | string | 条件必填 | - | `sync_mode=update` 时必填，用于按相对路径比较对象。 |
| target_hadoop_conf | map | 否 | - | 比较目标文件系统的 Hadoop 配置。 |
| update_strategy | string | 否 | `distcp` | 更新比较策略：`distcp` 或 `strict`。 |
| compare_mode | string | 否 | `len_mtime` | `len_mtime` 或 `checksum`；checksum 要求 strict 策略。 |
| update_compare_parallelism | int | 否 | `8` | 目标元数据查询并行度，范围为 1 到 64。 |
| update_compare_bulk_threshold | int | 否 | `0` | 正数表示达到阈值后使用批量目录列举；`0` 表示禁用。 |
| post_sync_action | string | 否 | `none` | 连续发现对象完成 checkpoint 后的动作：`none`、`delete` 或 `backup`。 |
| backup_path | string | 条件必填 | - | `post_sync_action=backup` 时必填，且不能与源路径重叠。 |
| retention_max_age | string | 否 | - | SeaTunnel 备份对象的最大保留时间。 |
| retention_check_interval | string | 否 | `1H` | 备份保留扫描间隔。 |
| common-options | | 否 | - | 参见 [Source 通用配置](../common-options/source-common-options.md)。 |

## 示例

### 使用 ADC 读取 Parquet

```hocon
source {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/warehouse/orders"
    file_format_type = "parquet"
  }
}
```

### 使用服务账号读取 CSV

```hocon
source {
  GcsFile {
    bucket = "gs://my-bucket"
    path = "/landing/customers"
    file_format_type = "csv"
    service_account_key_file = "/opt/seatunnel/keys/gcs-reader.json"
    skip_header_row_number = 1
    schema {
      fields {
        id = long
        name = string
      }
    }
    hadoop_gcs_properties = {
      "fs.gs.project.id" = "my-project"
    }
  }
}
```

## Changelog

<ChangeLog />
