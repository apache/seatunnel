import ChangeLog from '../changelog/connector-file-s3.md';

# S3File

> S3文件数据源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [多模态](../../introduction/concepts/connector-v2-features.md#多模态multimodal)

  使用二进制文件格式读取和写入任何格式的文件，例如视频、图片等。简而言之，任何文件都可以同步到目标位置。

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

  在一次pollNext调用中读取分片中的所有数据。将读取的分片保存在快照中。

- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义的分片](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式类型
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

从aws s3文件系统读取数据。

## 支持的数据源信息

| 数据源 | 支持的版本 |
|------------|--------------------|
| S3         | current            |

## 依赖

> 如果您使用spark/flink，为了使用此连接器，您必须确保您的spark/flink集群已经集成了hadoop。测试过的hadoop版本是2.x。<br/>
>
> 如果您使用SeaTunnel Zeta，它在您下载和安装SeaTunnel Zeta时会自动集成hadoop jar。您可以检查${SEATUNNEL_HOME}/lib下的jar包来确认这一点。<br/>
> 要使用此连接器，您需要将hadoop-aws-3.1.4.jar和aws-java-sdk-bundle-1.12.692.jar放在${SEATUNNEL_HOME}/lib目录中。

## 数据类型映射

数据类型映射与正在读取的文件类型相关，我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml`

### JSON文件类型

如果您将文件类型指定为`json`，您还应该指定schema选项来告诉连接器如何将数据解析为您想要的行。

例如：

上游数据如下：

```json

{"code":  200, "data":  "get success", "success":  true}

```

您也可以在一个文件中保存多条数据，并用换行符分隔：

```json lines

{"code":  200, "data":  "get success", "success":  true}
{"code":  300, "data":  "get failed", "success":  false}

```

您应该按如下方式指定schema：

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器将生成如下数据：

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

### 文本或CSV文件类型

如果您将`file_format_type`设置为`text`、`excel`、`csv`、`xml`。那么需要设置`schema`字段来告诉连接器如何将数据解析为行。

如果您设置了`schema`字段，您还应该设置选项`field_delimiter`，除非`file_format_type`是`csv`、`xml`、`excel`

您可以按如下方式设置schema和分隔符：

```hocon

field_delimiter = "#"
schema {
    fields {
        name = string
        age = int
        gender = string 
    }
}

```

连接器将生成如下数据：

|     name      | age | gender |
|---------------|-----|--------|
| tyrantlucifer | 26  | male   |

### Orc文件类型

如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。

| Orc数据类型                          | SeaTunnel数据类型                 |
|----------------------------------|-------------------------------|
| BOOLEAN                          | BOOLEAN                       |
| INT                              | INT                           |
| BYTE                             | BYTE                          |
| SHORT                            | SHORT                         |
| LONG                             | LONG                          |
| FLOAT                            | FLOAT                         |
| DOUBLE                           | DOUBLE                        |
| BINARY                           | BINARY                        |
| STRING<br/>VARCHAR<br/>CHAR<br/> | STRING                        |
| DATE                             | LOCAL_DATE_TYPE               |
| TIMESTAMP                        | LOCAL_DATE_TIME_TYPE          |
| DECIMAL                          | DECIMAL                       |
| LIST(STRING)                     | STRING_ARRAY_TYPE             |
| LIST(BOOLEAN)                    | BOOLEAN_ARRAY_TYPE            |
| LIST(TINYINT)                    | BYTE_ARRAY_TYPE               |
| LIST(SMALLINT)                   | SHORT_ARRAY_TYPE              |
| LIST(INT)                        | INT_ARRAY_TYPE                |
| LIST(BIGINT)                     | LONG_ARRAY_TYPE               |
| LIST(FLOAT)                      | FLOAT_ARRAY_TYPE              |
| LIST(DOUBLE)                     | DOUBLE_ARRAY_TYPE             |
| Map<K,V>                         | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT                           | SeaTunnelRowType              |

### Parquet文件类型

如果您将文件类型指定为`parquet` `orc`，则不需要schema选项，连接器可以自动找到上游数据的schema。

| Parquet数据类型          | SeaTunnel数据类型                 |
|----------------------|-------------------------------|
| INT_8                | BYTE                          |
| INT_16               | SHORT                         |
| DATE                 | DATE                          |
| TIMESTAMP_MILLIS     | TIMESTAMP                     |
| INT64                | LONG                          |
| INT96                | TIMESTAMP                     |
| BINARY               | BYTES                         |
| FLOAT                | FLOAT                         |
| DOUBLE               | DOUBLE                        |
| BOOLEAN              | BOOLEAN                       |
| FIXED_LEN_BYTE_ARRAY | TIMESTAMP<br/> DECIMAL        |
| DECIMAL              | DECIMAL                       |
| LIST(STRING)         | STRING_ARRAY_TYPE             |
| LIST(BOOLEAN)        | BOOLEAN_ARRAY_TYPE            |
| LIST(TINYINT)        | BYTE_ARRAY_TYPE               |
| LIST(SMALLINT)       | SHORT_ARRAY_TYPE              |
| LIST(INT)            | INT_ARRAY_TYPE                |
| LIST(BIGINT)         | LONG_ARRAY_TYPE               |
| LIST(FLOAT)          | FLOAT_ARRAY_TYPE              |
| LIST(DOUBLE)         | DOUBLE_ARRAY_TYPE             |
| Map<K,V>             | MapType，K和V的类型将转换为SeaTunnel类型 |
| STRUCT               | SeaTunnelRowType              |

## 选项

| 名称                              | 类型      | 是否必需 | 默认值                                                   | 描述                                                                                                                                                                                                                                                                                                                    |
|---------------------------------|---------|------|-------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                            | string  | 是    | -                                                     | 需要读取的s3路径，可以有子路径，但子路径需要满足一定的格式要求。具体要求可以参考"parse_partition_from_path"选项                                                                                                                                                                                                                                                |
| file_format_type                | string  | 是    | -                                                     | 文件类型，支持以下文件类型：`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`                                                                                                                                                                                                                                   |
| bucket                          | string  | 是    | -                                                     | s3文件系统的bucket地址，例如：`s3n://seatunnel-test`，如果您使用`s3a`协议，此参数应为`s3a://seatunnel-test`。                                                                                                                                                                                                                                   |
| fs.s3a.endpoint                 | string  | 是    | -                                                     | fs s3a端点                                                                                                                                                                                                                                                                                                              |
| fs.s3a.aws.credentials.provider | string  | 是    | com.amazonaws.auth.InstanceProfileCredentialsProvider | 透传给 Hadoop 的 S3A 凭据提供程序的全限定类名。除了两个常用值 `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`（使用静态 `access_key`/`secret_key`）和 `com.amazonaws.auth.InstanceProfileCredentialsProvider`（默认值）之外，任何 classpath 上可用的 S3A 凭据提供程序类都可以使用，例如基于容器的 `com.amazonaws.auth.ContainerCredentialsProvider` 或自定义提供程序。该类必须实现 `com.amazonaws.auth.AWSCredentialsProvider` 接口，并提供 Hadoop 3.1.4 支持的任一创建方式：公共 `(java.net.URI, org.apache.hadoop.conf.Configuration)` 构造器、公共 `(org.apache.hadoop.conf.Configuration)` 构造器、返回 `AWSCredentialsProvider` 的公共静态无参 `getInstance()` 工厂方法，或公共无参构造器。支持 Hadoop 风格的逗号或换行分隔的提供程序链，且每个类都会被独立校验。该提供程序 jar 必须存在于**每个**集群节点的运行时 classpath 中（例如放在 `${SEATUNNEL_HOME}/lib` 下），而不仅仅是提交作业的节点。共享/多租户集群的运维者请注意：此选项允许作业编写者按类名加载类，因此请相应地限制作业提交权限。有关凭据提供程序的更多信息，您可以查看[Hadoop AWS文档](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Simple_name.2Fsecret_credentials_with_SimpleAWSCredentialsProvider.2A) |
| read_columns                    | list    | 否    | -                                                     | 数据源的读取列列表，用户可以使用它来实现字段投影。支持列投影的文件类型如下所示：`text` `csv` `parquet` `orc` `json` `excel` `xml`。如果用户想在读取`text` `json` `csv`文件时使用此功能，必须配置"schema"选项。                                                                                                                                                                         |
| access_key                      | string  | 否    | -                                                     | 仅在`fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`时使用                                                                                                                                                                                                                        |
| secret_key                      | string  | 否    | -                                                     | 仅在`fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`时使用                                                                                                                                                                                                                        |
| hadoop_s3_properties            | map     | 否    | -                                                     | 如果您需要添加其他选项，可以在此处添加并参考此[链接](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)                                                                                                                                                                                                             |
| delimiter/field_delimiter       | string  | 否    | \001                                                  | 字段分隔符，用于告诉连接器在读取文本文件时如何切分字段。默认`\001`，与hive的默认分隔符相同。                                                                                                                                                                                                                                                                   |
| row_delimiter                   | string  | 否    | \n                                                    | 行分隔符，用于告诉连接器在读取文本文件时如何切分行。默认`\n`。                                                                                                                                                                                                                                                                                     |                                                                                                                                                                                                                                                                               |
| parse_partition_from_path       | boolean | 否    | true                                                  | 控制是否从文件路径解析分区键和值。例如，如果您从路径`s3n://hadoop-cluster/tmp/seatunnel/parquet/name=tyrantlucifer/age=26`读取文件。文件中的每条记录数据都将添加这两个字段：name="tyrantlucifer"，age=16                                                                                                                                                                  |
| date_format                     | string  | 否    | yyyy-MM-dd                                            | 日期类型格式，用于告诉连接器如何将字符串转换为日期，支持以下格式：`yyyy-MM-dd` `yyyy.MM.dd` `yyyy/MM/dd`。默认`yyyy-MM-dd`                                                                                                                                                                                                                                |
| datetime_format                 | string  | 否    | yyyy-MM-dd HH:mm:ss                                   | 日期时间类型格式，用于告诉连接器如何将字符串转换为日期时间，支持以下格式：`yyyy-MM-dd HH:mm:ss` `yyyy.MM.dd HH:mm:ss` `yyyy/MM/dd HH:mm:ss` `yyyyMMddHHmmss`                                                                                                                                                                                               |
| time_format                     | string  | 否    | HH:mm:ss                                              | 时间类型格式，用于告诉连接器如何将字符串转换为时间，支持以下格式：`HH:mm:ss` `HH:mm:ss.SSS`                                                                                                                                                                                                                                                            |
| skip_header_row_number          | long    | 否    | 0                                                     | 跳过前几行，但仅适用于txt和csv。例如，设置如下：`skip_header_row_number = 2`。然后SeaTunnel将跳过源文件的前2行                                                                                                                                                                                                                                         |
| csv_use_header_line             | boolean | 否    | false                                                 | 是否使用标题行来解析文件，仅在file_format为`csv`且文件包含符合RFC 4180的标题行时使用                                                                                                                                                                                                                                                                |
| schema                          | config  | 否    | -                                                     | 上游数据的schema。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                                                                                                                                                                                                                                                          |
| sheet_name                      | string  | 否    | -                                                     | 读取工作簿的工作表，仅在file_format为excel时使用。                                                                                                                                                                                                                                                                                     |
| excel_engine                    | string  | 否    | POI                                                   | 仅在 `file_format` 为 excel 时使用。支持的引擎包括 `POI` 和 `EasyExcel`。                                                                                                                                                                                                                                                                                       |
| poi_excel_max_file_size         | long    | 否    | 52428800                                              | 仅在 `file_format` 为 excel 且 `excel_engine` 为 POI 时使用。POI 引擎允许读取的最大 Excel 文件大小（默认 50 MB）。                                                                                                                                                                                                                                                                                       |
| xml_row_tag                     | string  | 否    | -                                                     | 指定XML文件中数据行的标签名称，仅对XML文件有效。                                                                                                                                                                                                                                                                                           |
| xml_use_attr_format             | boolean | 否    | -                                                     | 指定是否使用标签属性格式处理数据，仅对XML文件有效。                                                                                                                                                                                                                                                                                           |
| compress_codec                  | string  | 否    | none                                                  |                                                                                                                                                                                                                                                                                                                       |
| archive_compress_codec          | string  | 否    | none                                                  |                                                                                                                                                                                                                                                                                                                       |
| enable_file_split               | boolean | 否    | false                                                 | 开启大文件拆分以提升并行度。仅支持 `text`/`csv`/`json`/`parquet` 且非压缩格式（`compress_codec=none` 且 `archive_compress_codec=none`）。                                                                                 |
| file_split_size                 | long    | 否    | 134217728                                             | `enable_file_split=true` 时生效，单位字节。`text`/`csv`/`json` 按 `file_split_size` 拆分并对齐到下一个 `row_delimiter`；`parquet` 以 RowGroup 为拆分单位，不会切开 RowGroup。                                                |
| encoding                        | string  | 否    | UTF-8                                                 |                                                                                                                                                                                                                                                                                                                       |
| null_format                     | string  | 否    | -                                                     | 仅在file_format_type为text时使用。null_format用于定义哪些字符串可以表示为null。例如：`\N`                                                                                                                                                                                                                                                      |
| binary_chunk_size               | int     | 否    | 1024                                                  | 仅在file_format_type为binary时使用。读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。                                                                                                                                                                                                                                  |
| binary_complete_file_mode       | boolean | 否    | false                                                 | 仅在file_format_type为binary时使用。是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。                                                                                                                                                                                                                                      |
| discovery_mode                  | string  | 否    | once                                                  | 文件发现模式，支持 `once`（默认）和 `continuous`。continuous 模式会定期扫描路径并处理新增或变更对象，目前需要同时配置 `sync_mode=update` 和 `file_format_type=binary`。 |
| scan_interval                   | string  | 否    | 10S                                                   | `discovery_mode=continuous` 的轮询间隔，支持 `10S` 等简写和 `PT10S` 等 ISO-8601 格式。 |
| start_mode                      | string  | 否    | earliest                                              | 持续发现的初始扫描方式。`earliest` 会处理已有文件，`latest` 会忽略作业启动时已有文件，仅处理后续新增或变更。 |
| sync_mode                       | string  | 否    | full                                                  | 文件同步模式。`update` 会将源对象与 `target_path` 对比，只读取新增或变更对象，目前仅支持 binary 格式。 |
| target_path                     | string  | 否    | -                                                     | `sync_mode=update` 时必填，用于按相对路径进行对比，通常应与 sink 的 `path` 一致。 |
| target_hadoop_conf              | map     | 否    | -                                                     | `sync_mode=update` 时可选的目标文件系统 Hadoop 配置。 |
| update_strategy                 | string  | 否    | distcp                                                | update 模式的对比策略，支持 `distcp` 和 `strict`。 |
| compare_mode                    | string  | 否    | len_mtime                                             | update 模式的对比方式，支持 `len_mtime` 和 `checksum`；checksum 仅能与 `update_strategy=strict` 一起使用。 |
| update_compare_parallelism      | int     | 否    | 8                                                     | 目标对象元数据查询的最大并发数，有效范围为 `1-64`。 |
| update_compare_bulk_threshold   | int     | 否    | 0                                                     | 同一目标父目录的候选数达到正数阈值时改用一次目录枚举；`0` 表示关闭自动批量枚举。 |
| post_sync_action                | string  | 否    | none                                                  | 持续发现对象完成 checkpoint 后的可选动作，支持 `none`、`delete` 和 `backup`。 |
| backup_path                     | string  | 否    | -                                                     | `post_sync_action=backup` 时必填，且备份路径不能与源 `path` 重叠。 |
| retention_max_age               | string  | 否    | -                                                     | `backup_path` 中 SeaTunnel 备份对象的可选最大保留时间。 |
| retention_check_interval        | string  | 否    | 1H                                                    | 配置备份保留策略时的清理扫描间隔。 |
| file_filter_pattern             | string  | 否    |                                                       | 过滤模式，用于过滤文件。                                                                                                                                                                                                                                                                                                          |
| filename_extension              | string  | 否    | -                                                     | 过滤文件名扩展名，用于过滤具有特定扩展名的文件。例如：`csv` `.txt` `json` `.xml`。                                                                                                                                                                                                                                                                |
| common-options                  |         | 否    | -                                                     | 数据源插件通用参数，请参考[数据源通用选项](../common-options/source-common-options.md)了解详情。                                                                                                                                                                                                                                                              |
| quote_char                      | string  | 否    | "                                                     | 用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。                                                                                                                                                                                                                                                                               |
| escape_char                     | string  | 否    | -                                                     | 用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。                                                                                                                                                                                                                                                                                      |
| metalake_type                   | string  | 否    | gravitino                                            | Metalake 服务类型，目前支持 `gravitino`。                                                                                                                                                                                                                                                 |
| recursive_file_scan             | boolean | 否    | true                                                  | 是否递归扫描子目录。 如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。                                                                                                                                                                                                                                                                          |
| sort_files_by_modification_time | boolean | 否 | false               | 是否按修改时间降序排序文件。启用此选项后，在读取不断演化的 schema 时可确保 schema 推断使用最新的文件。                                                                                                                      |

### delimiter/field_delimiter [string]

**delimiter**参数将在2.3.5版本后弃用，请使用**field_delimiter**代替。

### row_delimiter [string]

仅在 file_format 为 text 时需要配置。

行分隔符，用于告诉连接器如何分割行。

默认 `\n`。

### quote_char [string]

用于包裹 CSV 字段的单字符，可保证包含逗号、换行符或引号的字段被正确解析。

### escape_char [string]

用于在 CSV 字段内转义引号或其他特殊字符，使其不会结束字段。

### recursive_file_scan [boolean]

是否递归扫描子目录。
如果设置为 `false`，将忽略子目录，仅扫描指定路径下的文件。

### sort_files_by_modification_time [boolean]

是否按修改时间降序排序文件。默认值为 `false`。
启用后，文件将按修改时间排序（最新的在前）。适用于以下场景：
- 读取具有不断演化的 schema 的文件，且希望 schema 推断使用最新的文件
- 需要按时间顺序处理文件

### file_filter_pattern [string]

文件过滤模式，用于过滤文件。若只想根据文件名称筛选，则直接写文件名称的正则；若同时想根据文件目录进行过滤，则表达式以`path`起始。

该模式遵循标准正则表达式。详情请参考 [正则表达式](https://en.wikipedia.org/wiki/Regular_expression)。
以下是一些示例。

若`path`为`/data/seatunnel`,且文件结构示例：
```
/data/seatunnel/20241001/report.txt
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
/data/seatunnel/20241012/logo.png
```
匹配规则示例：

**示例1**：*匹配所有.txt文件*，正则表达式：
```
.*.txt
```
此示例匹配的结果是：
```
/data/seatunnel/20241001/report.txt
```
**示例2**：*匹配所有以abc开头的文件*，正则表达式：
```
abc.*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
```
**示例3**：*匹配20241007文件夹下所有以 abc 开头的文件，且第四个字符为 h 或 g*，正则表达式：
```
/data/seatunnel/20241007/abc[h,g].*
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
```
**示例4**：*匹配以202410开头的第三级文件夹和以.csv结尾的文件*，正则表达式：
```
/data/seatunnel/202410\d*/.*.csv
```
此示例匹配的结果是：
```
/data/seatunnel/20241007/abch202410.csv
/data/seatunnel/20241002/abcg202410.csv
/data/seatunnel/20241005/old_data.csv
```

### enable_file_split [boolean]

开启大文件拆分功能，默认 false。仅支持 `csv`/`text`/`json`/`parquet` 且非压缩格式（`compress_codec=none` 且 `archive_compress_codec=none`）。

- `text`/`csv`/`json`：按 `file_split_size` 拆分并对齐到下一个 `row_delimiter`，避免切开一行/一条记录。
- `parquet`：以 RowGroup 为逻辑拆分单位，不会切开 RowGroup。

**使用建议**
- 适合：读取少量大文件，并希望通过更高并行度提升吞吐。
- 不建议：读取大量小文件，或并行度较低的场景（拆分会带来额外的枚举/调度开销）。

**限制说明**
- 不支持压缩文件（`compress_codec` != `none`）或归档文件（`archive_compress_codec` != `none`），会自动回退为不拆分，并打印 WARN 日志提示。
- 对于 `text`/`csv`/`json`，实际 split 的大小可能略大于 `file_split_size`（因为需要对齐到下一个 `row_delimiter`）。
- 对于 `json`，仅支持 JSON Lines（每行一个 JSON 对象）的切分读取。
- 启用切分后，数据全局顺序不保证（split 可能并行处理导致输出顺序交错）。如需严格有序，请设置 `parallelism=1` 或关闭切分。

### file_split_size [long]

`enable_file_split=true` 时生效，单位字节。默认 128MB（134217728）。

**调优建议**
- 建议从默认值（128MB）开始：如果并行度未充分利用可适当调小；如果 split 数量过多可适当调大。
- 经验公式：`file_split_size ≈ file_size / 期望并行度`。

### compress_codec [string]

文件的压缩编解码器，支持的详细信息如下所示：

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:
  自动识别压缩类型，无需额外设置。

### archive_compress_codec [string]

归档文件的压缩编解码器，支持的详细信息如下所示：

| archive_compress_codec | file_format | archive_compress_suffix |
|------------------------|------------|-------------------------|
| ZIP                    | txt,json,excel,xml | .zip                    |
| TAR                    | txt,json,excel,xml | .tar                    |
| TAR_GZ                 | txt,json,excel,xml | .tar.gz                 |
| GZ                     | txt,json,excel,xml | .gz                     |
| NONE                   | all                | .*                      |

注意：gz压缩的excel文件需要压缩原始文件或指定文件后缀，例如e2e.xls ->e2e_test.xls.gz

### encoding [string]

仅在file_format_type为json、text、csv、xml时使用。
要读取的文件的编码。此参数将由`Charset.forName(encoding)`解析。

### binary_chunk_size [int]

仅在file_format_type为binary时使用。

读取二进制文件的块大小（以字节为单位）。默认为1024字节。较大的值可能会提高大文件的性能，但会使用更多内存。

### binary_complete_file_mode [boolean]

仅在file_format_type为binary时使用。

是否将完整文件作为单个块读取，而不是分割成块。启用时，整个文件内容将一次性读入内存。默认为false。

### file_format_type [string]

文件类型，支持以下文件类型：

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary` `markdown` `pdf`

:::caution

出于安全考虑(XXE 加固), 包含 `<!DOCTYPE ...>` 声明的 XML 文件(`file_format_type = xml`)——即使是仅定义内部实体、不引用外部资源的良性声明——现在会被拒绝并抛出 `FILE_READ_FAILED` 错误。该行为没有配置项可以恢复为旧版本的处理方式。如果您的 XML 文件由某些工具导出并带有 `DOCTYPE` 头，请在使用 SeaTunnel 读取前将其移除或做预处理。

:::

如果您将文件类型指定为 `markdown`，SeaTunnel 可以解析 markdown 文件并提取结构化数据。
markdown 解析器提取各种元素，包括标题、段落、列表、代码块、表格等。
每个提取出的元素都会转换为一条文档元素结构化记录，schema 如下：
- `element_id`：元素的唯一标识符
- `element_type`：元素类型（Heading、Paragraph、ListItem 等）
- `heading_level`：标题级别（1-6，非标题元素为 null）
- `text`：元素的文本内容
- `page_number`：页码（默认：1）
- `position_index`：文档中的位置索引
- `parent_id`：父元素的 ID
- `child_ids`：子元素 ID 的逗号分隔列表

当 `markdown_rag_metadata_enabled` 或 `pdf_rag_metadata_enabled` 设置为 `true` 时，SeaTunnel 会针对对应文件类型在 `child_ids` 之后追加以下 RAG 元数据字段：
- `source_uri`：源文件路径或 URI
- `document_id`：由 `source_uri` 派生的稳定文档标识符
- `chunk_id`：由文档标识、chunk 顺序和内容哈希派生的稳定 chunk 标识符
- `chunk_index`：解析后文档中的一基 chunk 顺序
- `content_hash`：已输出 `text` 值的 SHA-256 哈希

启用该选项并读取有界 Markdown 文件时，source enumerator 会使用相同的 `document_id` 哈希分配整文件 split，使同一文档派生的所有行留在同一个 source 路由 bucket 中。禁用该选项时，默认的轮询 split 分配行为保持不变。

该选项默认值为 `false`，因此只有显式启用后才会改变原始 Markdown schema。

当 `markdown_rag_metadata_enabled=true` 时，每个 Markdown 行还会在 row options 中携带四个 Knowledge Sync 逻辑元数据值，source 也会在 metadata schema 中声明相同 Key：

- `SourceUri`：不含凭据的逻辑来源路径或 URI
- `DocumentId`：`doc_` 加逻辑 `SourceUri` 的 UTF-8 字节的小写 SHA-256
- `DocumentHash`：UTF-8 解码前实际读取到的精确来源字节的小写 SHA-256
- `ChunkHash`：当前 Markdown 输出行 `text` 的 UTF-8 字节的小写 SHA-256（null 按空字符串处理）；其值等于物理 `content_hash`

本地路径和有效 `file:` URI 沿用现有的本地路径归一化。对于分层远程 URI，逻辑 `SourceUri` 保留 scheme、host、显式端口和 path，移除 user info、完整 query 和 fragment，并将 scheme 与 host 转为小写。仅通过 query 区分资源时，必须改用稳定且不敏感的 path。

五个物理 RAG 字段、现有计算公式和路由行为均保持不变。因此，对于带签名或凭据的远程 URI，逻辑与物理 `document_id` 可能不同。请通过 [Metadata transform](../../transforms/metadata.md) 将逻辑 `SourceUri` 和 `DocumentId` 投影到 `ks_source_uri`、`ks_document_id` 等不冲突的别名。

逻辑 `ChunkHash` 只描述 Markdown source 直接输出的当前行。如果下游 transform 修改文本或把一行展开为多个 chunk，则必须在 lifecycle sink 前重新计算最终 `ChunkHash`、`ChunkId` 和 `ChunkIndex`。该 bridge 不实现增量比较、writer affinity、过期 chunk 删除或 tombstone。

注意：Markdown 格式仅支持读取，不支持写入。

如果您将文件类型指定为 `pdf`，SeaTunnel 可以解析 PDF 文件并提取结构化的文档元素。
PDF 使用与上文相同的文档元素 schema。
对于 PDF 输入，启用 `pdf_rag_metadata_enabled` 即可追加上文所述的 RAG 元数据字段。

PDF 特有的解析行为如下：

- **有大纲**：提取 `heading`（标题）、`paragraph`（段落）、`image`（图片）和 `link`（链接）元素。标题从大纲结构中派生，元素按照文档的逻辑结构组织为父子层级关系。
- **无大纲**：仅提取 `paragraph`（段落）和 `image`（图片）元素，以扁平结构呈现，不包含层级关系。
- `element_type` 在 PDF 场景下可能为 `heading`、`paragraph`、`image` 或 `link`。

注意：仅支持单栏（从上到下）PDF 布局。不支持多栏布局（例如并排的双栏文档），可能会产生不正确的文本顺序。

### schema [config]

仅在文件格式类型为 text、json、excel、xml 或 csv（或其他无法从元数据中读取 schema 的格式）时需要配置。

上游数据的 schema 信息。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

#### metadata_table_id [string]

元数据服务中的表标识符，用于获取表结构。对于 Gravitino，格式应为 `{catalog}.{database}.{table}`，例如 `mysql-catalog.test_db.users`。

当指定此参数时，连接器将从外部元数据服务获取表结构，而不是使用手动定义的 `columns`。

> 当使用 Gravitino 作为元数据源时，Gravitino 的列类型会自动转换为 SeaTunnel 数据类型。详细的类型映射信息请参考 [Gravitino 类型映射](../../introduction/concepts/gravitino-type-mapping.md)。

更多信息请参考 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

## 持续发现

`discovery_mode=continuous` 会让流式作业保持运行，并定期轮询 S3 中的新增或变更对象。该模式使用现有文件对比逻辑，不消费 S3 事件通知，也不会为对象删除或覆盖生成 changelog 行。

持续发现目前需要同时配置 `file_format_type="binary"` 和 `sync_mode="update"`。`target_path` 应与 sink 的基础 `path` 一致，以便源端跳过未变化对象。默认的 `discovery_mode="once"` 会保持原有有界读取行为。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  S3File {
    path = "/watch/source"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint = "s3.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "binary"

    discovery_mode = "continuous"
    scan_interval = "10S"
    start_mode = "earliest"
    sync_mode = "update"
    target_path = "/watch/target"
  }
}

sink {
  S3File {
    path = "/watch/target"
    tmp_path = "/watch/tmp"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint = "s3.amazonaws.com"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "binary"
  }
}
```

## 示例

1. 在此示例中，我们从s3路径`s3a://seatunnel-test/seatunnel/text`读取数据，此路径中的文件类型是orc。
   我们使用`org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`进行身份验证，因此需要`access_key`和`secret_key`。
   文件中的所有列都将被读取并发送到接收器。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    file_format_type = "orc"
  }
}

transform {
  # 如果您想获取有关如何配置seatunnel和查看转换插件完整列表的更多信息，
    # 请访问 https://seatunnel.apache.org/docs/transforms
}

sink {
  Console {}
}
```

2. 使用`InstanceProfileCredentialsProvider`进行身份验证
   S3中的文件类型是json，因此需要配置schema选项。

```hocon

  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    schema {
      fields {
        id = int
        name = string
      }
    }
  }

```

3. 使用`InstanceProfileCredentialsProvider`进行身份验证
   S3中的文件类型是json，有五个字段（`id`、`name`、`age`、`sex`、`type`），因此需要配置schema选项。
   在此作业中，我们只需要将`id`和`name`列发送到mysql。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    read_columns = ["id", "name"]
    schema {
      fields {
        id = int
        name = string
        age = int
        sex = int
        type = string
      }
    }
  }
}

transform {
  # 如果您想获取有关如何配置seatunnel和查看转换插件完整列表的更多信息，
    # 请访问 https://seatunnel.apache.org/docs/transforms
}

sink {
  Console {}
}
```

### 过滤文件

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  S3File {
    path = "/seatunnel/json"
    bucket = "s3a://seatunnel-test"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "json"
    read_columns = ["id", "name"]
    // 文件示例 abcD2024.csv
    file_filter_pattern = "abc[DX]*.*"
  }
}

sink {
  Console {
  }
}
```

### 使用 STS AssumeRole 读取（跨账号或联邦身份）

如果作业需要读取另一个 AWS 账号拥有的 bucket，或者通过 STS 切换 IAM 角色，可以把 AssumeRole 颁发的临时会话凭证通过 `hadoop_s3_properties` 传给连接器，并配合自定义凭据 provider 使用。

```hocon
source {
  S3File {
    path = "/cross-account/prefix"
    bucket = "s3a://target-bucket"
    fs.s3a.endpoint = "s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider = "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
    hadoop_s3_properties = {
      "fs.s3a.access.key"     = "<assumed-role-access-key>"
      "fs.s3a.secret.key"     = "<assumed-role-secret-key>"
      "fs.s3a.session.token"  = "<assumed-role-session-token>"
    }
    file_format_type = "parquet"
  }
}
```

对于 AWS SSO / Profile 这类 provider，只需要把 provider 类换成 `com.amazonaws.auth.profile.ProfileCredentialsProvider`，并在 `hadoop_s3_properties` 里配置 `fs.s3a.profile`、`fs.s3a.credentialsFile` 等 provider 特定键。完整的 `fs.s3a.*` 键集合参见 [Hadoop AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html) 文档。

## 变更日志

<ChangeLog />
