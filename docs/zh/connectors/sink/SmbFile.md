import ChangeLog from '../changelog/connector-file-smb.md';

# SmbFile

> SMB 文件接收连接器

## 描述

将数据输出到 SMB（Server Message Block）文件共享。

:::tip

如果使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已集成 hadoop。已测试的 hadoop 版本为 2.x。

如果使用 SeaTunnel Engine，下载和安装 SeaTunnel Engine 时会自动集成 hadoop jar。您可以检查 ${SEATUNNEL_HOME}/lib 下的 jar 包来确认。

:::

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式可以读写任何格式的文件，如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  默认使用 2PC 提交来确保 `exactly-once`

- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## 选项

| 名称                                  | 类型     | 必填 | 默认值                                     | 描述                                                                                    |
|---------------------------------------|---------|------|-------------------------------------------|----------------------------------------------------------------------------------------|
| host                                  | string  | 是   | -                                         | SMB 服务器主机地址                                                                       |
| port                                  | int     | 否   | 445                                       | SMB 服务器端口                                                                           |
| user                                  | string  | 是   | -                                         | SMB 认证用户名                                                                           |
| password                              | string  | 否   | -                                         | SMB 认证密码                                                                             |
| domain                                | string  | 否   | (空)                                      | SMB 认证域名（如 WORKGROUP）                                                              |
| share                                 | string  | 是   | -                                         | 要连接的 SMB 共享名称                                                                     |
| path                                  | string  | 是   | -                                         | 共享内的目标文件路径                                                                       |
| tmp_path                              | string  | 否   | /tmp/seatunnel                            | 结果文件将先写入临时路径，然后使用 `mv` 将临时目录提交到目标目录                                 |
| custom_filename                       | boolean | 否   | false                                     | 是否需要自定义文件名                                                                      |
| file_name_expression                  | string  | 否   | "${transactionId}"                        | 仅在 custom_filename 为 true 时使用                                                       |
| filename_time_format                  | string  | 否   | "yyyy.MM.dd"                              | 仅在 custom_filename 为 true 时使用                                                       |
| file_format_type                      | string  | 否   | "csv"                                     | 支持的文件类型：text, csv, parquet, orc, json, excel, xml, binary                          |
| filename_extension                    | string  | 否   | -                                         | 使用自定义文件扩展名覆盖默认的文件扩展名                                                              |
| field_delimiter                       | string  | 否   | text 为 '\001'，csv 为 ','                  | 仅在 file_format_type 为 text 和 csv 时使用                                              |
| row_delimiter                         | string  | 否   | "\n"                                      | 仅在 file_format_type 为 text, csv 和 json 时使用                                        |
| have_partition                        | boolean | 否   | false                                     | 是否需要处理分区                                                                          |
| partition_by                          | array   | 否   | -                                         | 仅在 have_partition 为 true 时使用                                                       |
| partition_dir_expression              | string  | 否   | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | 仅在 have_partition 为 true 时使用                                                       |
| is_partition_field_write_in_file      | boolean | 否   | false                                     | 仅在 have_partition 为 true 时使用                                                       |
| sink_columns                          | array   | 否   |                                           | 当此参数为空时，所有字段都是 sink 列                                                        |
| is_enable_transaction                 | boolean | 否   | true                                      |                                                                                        |
| batch_size                            | int     | 否   | 1000000                                   |                                                                                        |
| compress_codec                        | string  | 否   | none                                      |                                                                                        |
| common-options                        | object  | 否   | -                                         |                                                                                        |
| max_rows_in_memory                    | int     | 否   | -                                         | 仅在 file_format_type 为 excel 时使用                                                       |
| sheet_name                            | string  | 否   | Sheet${Random number}                     | 仅在 file_format_type 为 excel 时使用                                                       |
| xml_root_tag                          | string  | 否   | RECORDS                                   | 仅在 file_format 为 xml 时使用                                                              |
| xml_row_tag                           | string  | 否   | RECORD                                    | 仅在 file_format 为 xml 时使用                                                              |
| xml_use_attr_format                   | boolean | 否   | -                                         | 仅在 file_format 为 xml 时使用                                                              |
| single_file_mode                      | boolean | 否   | false                                     | 每个并行度只会输出一个文件。                                                                      |
| encoding                              | string  | 否   | UTF-8                                     | 仅在 file_format_type 为 text, json, csv, xml 时使用                                     |
| date_format                           | string  | 否   | yyyy-MM-dd                                | 日期类型格式                                                                                |
| datetime_format                       | string  | 否   | yyyy-MM-dd HH:mm:ss                       | 日期时间类型格式                                                                              |
| time_format                           | string  | 否   | HH:mm:ss                                  | 时间类型格式                                                                                |
| create_empty_file_when_no_data        | boolean | 否   | false                                     | 无数据时是否创建空文件                                                                          |
| schema_save_mode                      | string  | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST              | 已有目录处理方式                                                                          |
| data_save_mode                        | string  | 否   | APPEND_DATA                               | 已有数据处理方式                                                                          |
| enable_header_write                   | boolean | 否   | false                                     | 仅在 file_format_type 为 text, csv 时使用。false：不写表头，true：写表头                      |
| parquet_avro_write_timestamp_as_int96 | boolean | 否   | false                                     | 仅在 file_format 为 parquet 时使用                                                        |
| parquet_avro_write_fixed_as_int96     | array   | 否   | -                                         | 仅在 file_format 为 parquet 时使用                                                        |

## 示例

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

sink {
  SmbFile {
    host = "192.168.1.100"
    port = 445
    user = seatunnel
    password = pass
    domain = "WORKGROUP"
    share = "data"
    path = "/output/csv"
    tmp_path = "/tmp/seatunnel"
    file_format_type = "csv"
  }
}
```

## 更新日志

<ChangeLog />
