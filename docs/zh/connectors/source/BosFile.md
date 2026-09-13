import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS 文件 Source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#multimodal)

  使用 binary 格式读写任意类型文件（视频、图片等），可将任意文件同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  一次 pollNext 读取 split 内全部数据，已读 split 会保存在 checkpoint 快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown
  - [x] pdf

## 描述

通过 BOS HDFS SDK 从百度智能云 BOS 读取文件数据。

:::tip

使用 Spark/Flink 时，需确保集群已集成 Hadoop 2.x。

使用 SeaTunnel Engine 时，Hadoop 相关 jar 已包含在 `${SEATUNNEL_HOME}/lib` 中。

使用本连接器需将 `bos-hdfs-sdk`（>= 1.0.4-community）放入 `${SEATUNNEL_HOME}/lib`。下载：[bos-hdfs-sdk-1.0.4-community.jar.zip](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip)。详见 `connector-file-bos/lib/README.md`。

:::

## 配置项

| 名称                       | 类型    | 必填 | 默认值                      |
|----------------------------|---------|------|-----------------------------|
| path                       | string  | 是   | -                           |
| file_format_type           | string  | 是   | -                           |
| bucket                     | string  | 是   | -                           |
| access_key                 | string  | 是   | -                           |
| secret_key                 | string  | 是   | -                           |
| endpoint                   | string  | 是   | -                           |
| read_columns               | list    | 否   | -                           |
| delimiter/field_delimiter  | string  | 否   | text 为 \001，csv 为 ,      |
| row_delimiter              | string  | 否   | \n                          |
| parse_partition_from_path  | boolean | 否   | true                        |
| skip_header_row_number     | long    | 否   | 0                           |
| date_format                | string  | 否   | yyyy-MM-dd                  |
| datetime_format            | string  | 否   | yyyy-MM-dd HH:mm:ss         |
| time_format                | string  | 否   | HH:mm:ss                    |
| schema                     | config  | 否   | -                           |
| sheet_name                 | string  | 否   | -                           |
| excel_engine               | string  | 否   | POI                         |
| poi_excel_max_file_size    | long    | 否   | 52428800                    |
| xml_row_tag                | string  | 否   | -                           |
| xml_use_attr_format        | boolean | 否   | -                           |
| csv_use_header_line        | boolean | 否   | false                       |
| file_filter_pattern        | string  | 否   | -                           |
| filename_extension         | string  | 否   | -                           |
| compress_codec             | string  | 否   | none                        |
| archive_compress_codec     | string  | 否   | none                        |
| encoding                   | string  | 否   | UTF-8                       |
| binary_chunk_size          | int     | 否   | 1024                        |
| binary_complete_file_mode  | boolean | 否   | false                       |
| file_filter_modified_start | string  | 否   | -                           |
| file_filter_modified_end   | string  | 否   | -                           |
| quote_char                 | string  | 否   | "                           |
| escape_char                | string  | 否   | -                           |
| recursive_file_scan        | boolean | 否   | true                        |
| sort_files_by_modification_time | boolean | 否   | false                       |

## 示例

```hocon
source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/warehouse/table/"
    file_format_type = "orc"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
  }
}
```

### 二进制文件同步

```hocon
source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/read/binary/"
    file_format_type = "binary"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
  }
}

sink {
  BosFile {
    bucket = "bos://sink-bucket"
    path = "/write/binary/"
    file_format_type = "binary"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
  }
}
```

## 变更日志

<ChangeLog />
