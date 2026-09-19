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

- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  使用二进制文件格式可以读写任何格式的文件，如视频、图片等。

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
| file_format_type                      | string  | 否   | "csv"                                     | 支持的文件类型：text, csv, parquet, orc, json, excel, xml, binary                          |
| field_delimiter                       | string  | 否   | text 为 '\001'，csv 为 ','                  | 仅在 file_format_type 为 text 和 csv 时使用                                              |
| row_delimiter                         | string  | 否   | "\n"                                      | 仅在 file_format_type 为 text, csv 和 json 时使用                                        |
| have_partition                        | boolean | 否   | false                                     | 是否需要处理分区                                                                          |
| partition_by                          | array   | 否   | -                                         | 仅在 have_partition 为 true 时使用                                                       |
| sink_columns                          | array   | 否   |                                           | 当此参数为空时，所有字段都是 sink 列                                                        |
| is_enable_transaction                 | boolean | 否   | true                                      |                                                                                        |
| batch_size                            | int     | 否   | 1000000                                   |                                                                                        |
| compress_codec                        | string  | 否   | none                                      |                                                                                        |
| common-options                        | object  | 否   | -                                         |                                                                                        |
| encoding                              | string  | 否   | UTF-8                                     | 仅在 file_format_type 为 text, json, csv, xml 时使用                                     |

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
