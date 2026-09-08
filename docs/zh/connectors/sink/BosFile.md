import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS 文件 Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 BOS HDFS SDK 将数据写入百度智能云 BOS。

:::tip

使用 Spark/Flink 时，需确保集群已集成 Hadoop 2.x。

使用 SeaTunnel Engine 时，Hadoop 相关 jar 已包含在 `${SEATUNNEL_HOME}/lib` 中。

使用本连接器需将 `bos-hdfs-sdk`（>= 1.0.4-community）放入 `${SEATUNNEL_HOME}/lib`。下载：[bos-hdfs-sdk-1.0.4-community.jar.zip](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip)。

:::

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#multimodal)

  使用 binary 格式读写任意类型文件，可将任意文件同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  默认通过 2PC 提交保证 exactly-once

- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时 flush](../../introduction/concepts/connector-v2-features.md)

- [x] 文件格式
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] canal_json
  - [x] debezium_json
  - [x] maxwell_json

## 配置项

| 名称                                  | 类型    | 必填 | 默认值                                     | 描述 |
|---------------------------------------|---------|------|--------------------------------------------|------|
| path                                  | string  | 是   | -                                          | bucket 内写入目录 |
| tmp_path                              | string  | 否   | /tmp/seatunnel                             | 先写入临时目录，再通过 mv 提交到目标目录 |
| bucket                                | string  | 是   | -                                          | BOS bucket，例如 `bos://my-bucket` |
| access_key                            | string  | 是   | -                                          | BOS Access Key |
| secret_key                            | string  | 是   | -                                          | BOS Secret Key |
| endpoint                              | string  | 是   | -                                          | BOS Endpoint，例如 `http://bj.bcebos.com` |
| custom_filename                       | boolean | 否   | false                                      | 是否自定义文件名 |
| file_name_expression                  | string  | 否   | "${transactionId}"                         | custom_filename 为 true 时使用 |
| filename_time_format                  | string  | 否   | "yyyy.MM.dd"                               | custom_filename 为 true 时使用 |
| file_format_type                      | string  | 否   | "csv"                                      | 支持 text、csv、parquet、orc、json、excel、xml、binary 等 |
| filename_extension                    | string  | 否   | -                                          | 自定义文件扩展名 |
| field_delimiter                       | string  | 否   | text 为 \001，csv 为 ,                     | text/csv 格式使用 |
| row_delimiter                         | string  | 否   | "\n"                                       | text/csv/json 格式使用 |
| have_partition                        | boolean | 否   | false                                      | 是否按分区写入 |
| partition_by                          | array   | 否   | -                                          | have_partition 为 true 时使用 |
| partition_dir_expression              | string  | 否   | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | have_partition 为 true 时使用 |
| is_partition_field_write_in_file      | boolean | 否   | false                                      | have_partition 为 true 时使用 |
| sink_columns                          | array   | 否   |                                            | 为空时写入全部字段 |
| is_enable_transaction                 | boolean | 否   | true                                       | 开启后保证写入不丢不重，文件名自动加 `${transactionId}_` 前缀 |
| batch_size                            | int     | 否   | 1000000                                    | 单文件最大行数 |
| compress_codec                        | string  | 否   | none                                       | 压缩格式 |
| xml_root_tag                          | string  | 否   | RECORDS                                    | xml 格式使用 |
| xml_row_tag                           | string  | 否   | RECORD                                     | xml 格式使用 |
| xml_use_attr_format                   | boolean | 否   | -                                          | xml 格式使用 |
| single_file_mode                      | boolean | 否   | false                                      | 每个并行度只输出一个文件 |
| create_empty_file_when_no_data        | boolean | 否   | false                                      | 上游无数据时仍生成空文件 |
| parquet_avro_write_timestamp_as_int96 | boolean | 否   | false                                      | parquet 格式使用 |
| parquet_avro_write_fixed_as_int96     | array   | 否   | -                                          | parquet 格式使用 |
| encoding                              | string  | 否   | "UTF-8"                                    | json/text/csv/xml 格式使用 |

## 示例

分区写入 text 文件：

```hocon
sink {
  BosFile {
    path = "/sink"
    bucket = "bos://sink-bucket"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    file_format_type = "text"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_enable_transaction = true
  }
}
```

简单 text 写入：

```hocon
sink {
  BosFile {
    bucket = "bos://sink-bucket"
    path = "/warehouse/table/"
    file_format_type = "text"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    row_delimiter = "\n"
    field_delimiter = ","
    is_enable_transaction = true
  }
}
```

## 变更日志

<ChangeLog />
