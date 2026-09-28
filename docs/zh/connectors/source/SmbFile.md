import ChangeLog from '../changelog/connector-file-smb.md';

# SmbFile

> SMB 文件源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式可以读写任何格式的文件，如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] markdown
  - [x] pdf

## 描述

从 SMB（Server Message Block）文件共享中读取数据。SMB 是一种网络文件共享协议，允许应用程序在远程服务器上读写文件。

:::tip

如果使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已集成 hadoop。已测试的 hadoop 版本为 2.x。

如果使用 SeaTunnel Engine，下载和安装 SeaTunnel Engine 时会自动集成 hadoop jar。您可以检查 ${SEATUNNEL_HOME}/lib 下的 jar 包来确认。

:::

## 支持的数据源信息

| 数据源   | 支持版本    |                                       依赖                                       |
|---------|-----------|---------------------------------------------------------------------------------|
| SmbFile | SMB2/SMB3 | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-file-smb) |

## 数据类型映射

文件没有特定的类型列表，我们可以通过在配置中指定 Schema 来表明相应数据需要转换为哪种 SeaTunnel 数据类型。

| SeaTunnel 数据类型 |
|-------------------|
| STRING            |
| SHORT             |
| INT               |
| BIGINT            |
| BOOLEAN           |
| DOUBLE            |
| DECIMAL           |
| FLOAT             |
| DATE              |
| TIME              |
| TIMESTAMP         |
| BYTES             |
| ARRAY             |
| MAP               |

## 源连接器选项

| 名称                       | 类型     | 必填 | 默认值                         | 描述                                                                                    |
|----------------------------|---------|------|-------------------------------|----------------------------------------------------------------------------------------|
| host                       | String  | 是   | -                             | SMB 服务器主机地址                                                                       |
| port                       | Int     | 否   | 445                           | SMB 服务器端口                                                                           |
| user                       | String  | 是   | -                             | SMB 认证用户名                                                                           |
| password                   | String  | 否   | -                             | SMB 认证密码                                                                             |
| domain                     | String  | 否   | (空)                          | SMB 认证域名（如 WORKGROUP）                                                              |
| share                      | String  | 是   | -                             | 要连接的 SMB 共享名称                                                                     |
| path                       | String  | 是   | -                             | 共享内的源文件路径                                                                        |
| file_format_type           | String  | 是   | -                             | 支持的文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf` |
| file_filter_pattern        | String  | 否   | -                             | 文件过滤模式，用于过滤文件                                                                 |
| delimiter/field_delimiter  | String  | 否   | text 为 \001，csv 为 ','        | 字段分隔符，用于告诉连接器在读取文本文件时如何分割字段                                         |
| row_delimiter              | String  | 否   | \n                            | 行分隔符，用于告诉连接器在读取文本文件时如何分割行                                             |
| parse_partition_from_path  | Boolean | 否   | true                          | 控制是否从文件路径解析分区键和值                                                            |
| date_format                | String  | 否   | yyyy-MM-dd                    | 日期类型格式                                                                             |
| datetime_format            | String  | 否   | yyyy-MM-dd HH:mm:ss          | 日期时间类型格式                                                                          |
| time_format                | String  | 否   | HH:mm:ss                      | 时间类型格式                                                                             |
| skip_header_row_number     | Long    | 否   | 0                             | 跳过前几行，仅适用于 txt 和 csv                                                           |
| schema                     | Config  | 否   | -                             | 上游数据的 schema                                                                       |
| read_columns               | List    | 否   | -                             | 数据源的读取列列表，用户可以用它实现字段投影                                                  |
| sheet_name                 | String  | 否   | -                             | 读取工作簿中的 sheet，仅在 file_format 为 excel 时使用                                     |
| xml_row_tag                | String  | 否   | -                             | 指定 XML 文件中数据行的标签名，仅在 file_format 为 xml 时使用                                  |
| xml_use_attr_format        | Boolean | 否   | -                             | 指定是否使用标签属性格式处理数据，仅在 file_format 为 xml 时使用                                  |
| compress_codec             | String  | 否   | None                          | 文件的压缩编解码器                                                                      |
| encoding                   | String  | 否   | UTF-8                         | 读取文件时使用的编码                                                                      |
| null_format                | String  | 否   | -                             | 仅在 file_format_type 为 text 时使用。定义哪些字符串可以表示为 null，例如 `\N`                  |
| filename_extension         | String  | 否   | -                             | 文件扩展名过滤，用于过滤特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`                      |
| excel_engine               | String  | 否   | POI                           | 仅在 file_format 为 excel 时使用。支持的引擎为 `POI` 和 `EasyExcel`                          |
| poi_excel_max_file_size    | Long    | 否   | 52428800                      | 仅在 file_format 为 excel 且 excel_engine 为 POI 时使用。POI 引擎可读取的最大 Excel 文件大小（默认 50 MB） |
| quote_char                 | String  | 否   | "                             | 用于包围 CSV 字段的单个字符，使包含逗号、换行或引号的字段能被正确读取                               |
| escape_char                | String  | 否   | -                             | 用于在 CSV 字段内转义引号或其他特殊字符的单个字符                                               |
| metalake_type              | String  | 否   | gravitino                     | metalake 服务类型，目前支持 `gravitino`                                                     |
| discovery_mode             | String  | 否   | once                          | 文件发现模式。支持的值：`once`（默认）、`continuous`。`continuous` 时源端持续扫描路径             |
| scan_interval              | String  | 否   | 10S                           | 仅在 `discovery_mode=continuous` 时使用。定期发现的扫描间隔                                   |
| start_mode                 | String  | 否   | earliest                      | 仅在 `discovery_mode=continuous` 时使用。支持的值：`earliest`（默认）、`latest`                |
| sync_mode                  | String  | 否   | full                          | 文件同步模式。支持的值：`full`、`update`。`update` 时仅读取新增/变更的文件（目前仅支持 binary 格式） |
| target_path                | String  | 否   | -                             | 仅在 `sync_mode=update` 时使用。用于比较的目标基本路径                                        |
| target_hadoop_conf         | Map     | 否   | -                             | 仅在 `sync_mode=update` 时使用。目标文件系统的额外 Hadoop 配置                                |
| update_strategy            | String  | 否   | distcp                        | 仅在 `sync_mode=update` 时使用。支持的值：`distcp`（默认）、`strict`                          |
| compare_mode               | String  | 否   | len_mtime                     | 仅在 `sync_mode=update` 时使用。支持的值：`len_mtime`（默认）、`checksum`                     |
| update_compare_parallelism | Int     | 否   | 8                             | 稀疏目标元数据查找的最大并行度。有效范围：1-64                                                 |
| update_compare_bulk_threshold | Int  | 否   | 0                             | 当候选计数达到阈值时切换为目录列表比较。`0` 表示禁用                                            |
| post_sync_action           | String  | 否   | none                          | `discovery_mode=continuous` 时的同步后操作。支持的值：`none`（默认）、`delete`、`backup`          |
| backup_path                | String  | 否   | -                             | `post_sync_action=backup` 时的备份目标基本路径。不能与 `path` 重叠                             |
| retention_max_age          | String  | 否   | -                             | 备份文件的可选保留期限，仅在 `post_sync_action=backup` 时有效                                  |
| retention_check_interval   | String  | 否   | 1H                            | 保留扫描间隔，仅在配置了 `post_sync_action=backup` 和 `retention_max_age` 时生效               |
| recursive_file_scan        | Boolean | 否   | true                          | 是否递归扫描子目录。如果为 `false`，将忽略子目录                                               |
| common-options             |         | 否   | -                             | 源插件通用参数，请参阅 [Source Common Options](../common-options/source-common-options.md)    |

## 如何创建 SMB 数据同步作业

以下示例演示了如何创建一个从 SMB 共享读取数据并在本地客户端打印的数据同步作业：

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  SmbFile {
    host = "192.168.1.100"
    port = 445
    user = seatunnel
    password = pass
    domain = "WORKGROUP"
    share = "data"
    path = "/reports/json"
    file_format_type = "json"
    plugin_output = "smb"
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

### 多表模式

```hocon
SmbFile {
  tables_configs = [
    {
      schema {
        table = "student"
        fields {
          name = string
          age = int
        }
      }
      path = "/data/student"
      host = "192.168.1.100"
      port = 445
      user = seatunnel
      password = pass
      share = "data"
      file_format_type = "parquet"
    },
    {
      schema {
        table = "teacher"
        fields {
          name = string
          age = int
        }
      }
      path = "/data/teacher"
      host = "192.168.1.100"
      port = 445
      user = seatunnel
      password = pass
      share = "data"
      file_format_type = "parquet"
    }
  ]
}
```

## 更新日志

<ChangeLog />
