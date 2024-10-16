# Hdfs文件

> Hdfs文件 数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)

在一次 pollNext 调用中读取分片中的所有数据。将读取的分片保存在快照中。

- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../concept/connector-v2-features.md)
- [x] 文件格式
  - [x] 文本
  - [x] CSV
  - [x] Parquet
  - [x] ORC
  - [x] JSON
  - [x] Excel

## 描述

从Hdfs文件系统中读取数据。

## 支持的数据源信息

|  数据源   |      支持的版本       |
|--------|------------------|
| Hdfs文件 | hadoop 2.x 和 3.x |

## 源选项

|            名称             |   类型    | 是否必须 |      默认值       |                                                                                                                     描述                                                                                                                      |
|---------------------------|---------|------|----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                      | string  | 是    | -              | 源文件路径。                                                                                                                                                                                                                                      |
| file_format_type          | string  | 是    | -              | 我们支持以下文件类型：`text` `json` `csv` `orc` `parquet` `excel`。请注意，最终文件名将以文件格式的后缀结束，文本文件的后缀是 `txt`。                                                                                                                                                 |
| fs.defaultFS              | string  | 是    | -              | 以 `hdfs://` 开头的 Hadoop 集群地址，例如：`hdfs://hadoopcluster`。                                                                                                                                                                                      |
| read_columns              | list    | 否    | -              | 数据源的读取列列表，用户可以使用它实现字段投影。支持的文件类型的列投影如下所示：[text,json,csv,orc,parquet,excel]。提示：如果用户在读取 `text` `json` `csv` 文件时想要使用此功能，必须配置 schema 选项。                                                                                                         |
| hdfs_site_path            | string  | 否    | -              | `hdfs-site.xml` 的路径，用于加载 namenodes 的 ha 配置。                                                                                                                                                                                                 |
| delimiter/field_delimiter | string  | 否    | \001           | 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。默认 `\001`，与 Hive 的默认分隔符相同。                                                                                                                                                                                      |
| parse_partition_from_path | boolean | 否    | true           | 控制是否从文件路径中解析分区键和值。例如，如果您从路径 `hdfs://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26` 读取文件，则来自文件的每条记录数据将添加这两个字段：[name:tyrantlucifer,age:26]。提示：不要在 schema 选项中定义分区字段。                                                          |
| date_format               | string  | 否    | yyyy-MM-dd     | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持的格式如下：`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`，默认 `yyyy-MM-dd`。日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持的格式如下：`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`，默认 `yyyy-MM-dd HH:mm:ss`。 |
| time_format               | string  | 否    | HH:mm:ss       | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持的格式如下：`HH:mm:ss` `HH:mm:ss.SSS`，默认 `HH:mm:ss`。                                                                                                                                                                  |
| remote_user               | string  | 否    | -              | 用于连接 Hadoop 的登录用户。它旨在用于 RPC 中的远程用户，不会有任何凭据。                                                                                                                                                                                                 |
| krb5_path                 | string  | 否    | /etc/krb5.conf | kerberos 的 krb5 路径。                                                                                                                                                                                                                         |
| kerberos_principal        | string  | 否    | -              | kerberos 的 principal。                                                                                                                                                                                                                       |
| kerberos_keytab_path      | string  | 否    | -              | kerberos 的 keytab 路径。                                                                                                                                                                                                                       |
| skip_header_row_number    | long    | 否    | 0              | 跳过前几行，但仅适用于 txt 和 csv。例如，设置如下：`skip_header_row_number = 2`。然后 Seatunnel 将跳过源文件中的前两行。                                                                                                                                                        |
| schema                    | config  | 否    | -              | 上游数据的模式字段。                                                                                                                                                                                                                                  |
| sheet_name                | string  | 否    | -              | 读取工作簿的表格，仅在文件格式为 excel 时使用。                                                                                                                                                                                                                 |
| compress_codec            | string  | 否    | none           | 文件的压缩编解码器。                                                                                                                                                                                                                                  |
| common-options            |         | 否    | -              | 源插件通用参数，请参阅 [源通用选项](../../../en/connector-v2/source-common-options.md) 获取详细信息。                                                                                                                                                              |

### delimiter/field_delimiter [string]

**delimiter** 参数在版本 2.3.5 后将被弃用，请改用 **field_delimiter**。

### compress_codec [string]

文件的压缩编解码器及支持的详细信息如下所示：

- txt：`lzo` `none`
- json：`lzo` `none`
- csv：`lzo` `none`
- orc/parquet：  
  自动识别压缩类型，无需额外设置。

### 提示

> 如果您使用 spark/flink，为了

使用此连接器，您必须确保您的 spark/flink 集群已经集成了 hadoop。测试过的 hadoop 版本是 2.x。如果您使用 SeaTunnel Engine，则在下载和安装 SeaTunnel Engine 时会自动集成 hadoop jar。您可以检查 `${SEATUNNEL_HOME}/lib` 下的 jar 包来确认这一点。

## 任务示例

### 简单示例:

> 此示例定义了一个 SeaTunnel 同步任务，从 Hdfs 中读取数据并将其发送到 Hdfs。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  HdfsFile {
  schema {
    fields {
      name = string
      age = int
    }
  }
  path = "/apps/hive/demo/student"
  type = "json"
  fs.defaultFS = "hdfs://namenode001"
  }
  # 如果您想获取有关如何配置 seatunnel 和查看源插件完整列表的更多信息，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果您想获取有关如何配置 seatunnel 和查看转换插件完整列表的更多信息，
    # 请访问 https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    HdfsFile {
      fs.defaultFS = "hdfs://hadoopcluster"
      path = "/tmp/hive/warehouse/test2"
      file_format = "orc"
    }
  # 如果您想获取有关如何配置 seatunnel 和查看接收器插件完整列表的更多信息，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

