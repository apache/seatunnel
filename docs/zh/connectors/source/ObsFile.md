import ChangeLog from '../changelog/connector-file-obs.md';

# ObsFile

> Obs 文件源连接器

## 支持这些引擎

> Spark
>
> Flink
>
> Seatunnel Zeta

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [多模态](../../introduction/concepts/connector-v2-features.md#multimodal)

  使用二进制文件格式读写任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次 pollNext 调用中读取分割中的所有数据。读取哪些分割将保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] markdown

## 描述

从华为云 OBS 文件系统读取数据。

如果您使用 spark/flink，为了使用此连接器，您必须确保您的 spark/flink 集群已集成 hadoop。测试的 hadoop 版本是 2.x。

如果您使用 SeaTunnel 引擎，它会在您下载和安装 SeaTunnel 引擎时自动集成 hadoop jar。您可以检查 ${SEATUNNEL_HOME}/lib 下的 jar 包来确认这一点。

我们为了支持更多文件类型做了一些权衡，所以我们使用 HDFS 协议来内部访问 OBS，此连接器需要一些 hadoop 依赖项。
它仅支持 hadoop 版本 **2.9.X+**。

## 必需的 Jar 列表

| jar | 支持的版本 | maven |
|-----|-----------|-------|
| hadoop-huaweicloud | 支持版本 >= 3.1.1.29 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/org/apache/hadoop/hadoop-huaweicloud/) |
| esdk-obs-java | 支持版本 >= 3.19.7.3 | [下载](https://repo.huaweicloud.com/artifactory/sdk_public/com/huawei/storage/esdk-obs-java/) |
| okhttp | 支持版本 >= 3.11.0 | [下载](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/) |
| okio | 支持版本 >= 1.14.0 | [下载](https://repo1.maven.org/maven2/com/squareup/okio/okio/) |

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录。
>
> 并将所有 jar 复制到 $SEATUNNEL_HOME/lib/

## 选项

| 参数名                       | 类型      | 必须 | 默认值                 | 描述                                      |
|---------------------------|---------|----|---------------------|-----------------------------------------|
| path                      | string  | 是  | -                   | 目标目录路径                                  |
| file_format_type          | string  | 是  | -                   | 文件类型                                    |
| bucket                    | string  | 是  | -                   | OBS 文件系统的桶地址，例如：`obs://obs-bucket-name` |
| access_key                | string  | 是  | -                   | OBS 文件系统的访问密钥                           |
| access_secret             | string  | 是  | -                   | OBS 文件系统的访问密钥                           |
| endpoint                  | string  | 是  | -                   | OBS 文件系统的端点                             |
| read_columns              | list    | 是  | -                   | 数据源的读取列列表                               |
| delimiter                 | string  | 否  | \001                | 字段分隔符                                   |
| row_delimiter             | string  | 否  | \n                  | 行分隔符                                    |
| parse_partition_from_path | boolean | 否  | true                | 控制是否从文件路径解析分区键和值                        |
| skip_header_row_number    | long    | 否  | 0                   | 跳过前几行，但仅适用于 txt 和 csv。                  |
| date_format               | string  | 否  | yyyy-MM-dd          | 日期类型格式                                  |
| datetime_format           | string  | 否  | yyyy-MM-dd HH:mm:ss | 日期时间类型格式                                |
| time_format               | string  | 否  | HH:mm:ss            | 时间类型格式                                  |
| quote_char                | string  | 否  | "                   | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。 |
| escape_char               | string  | 否  | -                   | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。        |


## 变更日志

<ChangeLog />

