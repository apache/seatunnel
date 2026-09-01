import ChangeLog from '../changelog/connector-s3-redshift.md';

# S3Redshift

> S3Redshift 的作用是将数据写入 S3，然后使用 Redshift 的 COPY 命令将数据从 S3 导入 Redshift。

## 描述

将数据输出到 AWS Redshift。

> 提示：
>
> 我们基于 [S3File](S3File.md) 来实现这个连接器。因此，您可以使用与 S3File 相同的配置。
> 为了支持更多的文件类型，我们进行了一些权衡，因此我们使用 HDFS 协议对 S3 进行内部访问，而这个连接器需要一些 hadoop 依赖。
> 它只支持 hadoop 版本 **2.6.5+**。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

默认情况下，我们使用 2PC commit 来确保“精确一次”。

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 参数

|               名称               |  类型   | 是否必填 |                       默认值                       | 描述 |
|----------------------------------|---------|----------|-----------------------------------------------------------|------|
| jdbc_url                         | string  | 是      | -                                                         | 连接 Redshift 数据库的 JDBC URL，例如 `jdbc:redshift://your-cluster.region.redshift.amazonaws.com:5439/your_database`。 |
| jdbc_user                        | string  | 是      | -                                                         | 连接 Redshift 数据库的用户名。 |
| jdbc_password                    | string  | 是      | -                                                         | 连接 Redshift 数据库的密码。 |
| execute_sql                      | string  | 是      | -                                                         | 数据写入 S3 之后要执行的 SQL，通常是一条 Redshift `COPY` 命令（详见下方 `### execute_sql` 中必须包含的占位符）。 |
| path                             | string  | 是      | -                                                         | bucket 下的目标目录路径，连接器会通过 `${path}` 占位符把实际写入路径追加到 `execute_sql` 中。 |
| bucket                           | string  | 是      | -                                                         | S3 文件系统的 bucket 地址，例如 `s3a://seatunnel-test`。使用 Hadoop 读写时建议使用 `s3a` 协议。 |
| access_key                       | string  | 否       | -                                                         | S3 文件系统的 access key。如果未配置，需要正确配置 Hadoop 凭据链，请参考 [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)。 |
| access_secret                    | string  | 否       | -                                                         | S3 文件系统的 access secret。如果未配置，需要正确配置 Hadoop 凭据链，请参考 [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)。 |
| hadoop_s3_properties             | map     | 否       | -                                                         | 额外的 Hadoop S3A / Hadoop-AWS 选项，可以用来设置 `fs.s3a.aws.credentials.provider` 等。请参考 [Hadoop-AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)。 |
| file_name_expression             | string  | 否       | "${transactionId}"                                        | 在 `path` 下追加的文件名表达式，可使用 `${now}` 或 `${uuid}` 注入时间或 UUID。`is_enable_transaction = true` 时会自动在文件名前添加 `${transactionId}_`。 |
| file_format_type                 | string  | 否       | "text"                                                    | 写入 S3 的文件格式，支持 `text`、`csv`、`parquet`、`orc`、`json`。最终文件名会带相应后缀（例如 `text` 是 `txt`）。 |
| filename_time_format             | string  | 否       | "yyyy.MM.dd"                                              | 解析 `file_name_expression` 中 `${now}` 的时间格式，详见 [Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)。 |
| field_delimiter                  | string  | 否       | '\001'                                                    | `text` 和 `csv` 文件的列分隔符。 |
| row_delimiter                    | string  | 否       | "\n"                                                      | `text` 和 `csv` 文件的行分隔符。 |
| partition_by                     | array   | 否       | -                                                         | 按指定的上游字段对数据进行分区，分区目录由 `partition_dir_expression` 推导。 |
| partition_dir_expression         | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"                | 根据 `partition_by` 字段生成分区目录的表达式。 |
| is_partition_field_write_in_file | boolean | 否       | false                                                     | 当为 `true` 时，分区字段及其值会写入数据文件。Hive 风格的数据文件请设为 `false`。 |
| sink_columns                     | array   | 否       | 当此参数为空时，所有字段都是 sink 列                          | 需要写入文件的列，字段顺序决定文件实际写入顺序。 |
| is_enable_transaction            | boolean | 否       | true                                                      | 为 `true` 时，连接器保证数据写入目标目录时不丢失、不重复。目前只支持 `true`。 |
| batch_size                       | int     | 否       | 1000000                                                   | 单个文件的最大行数。在 SeaTunnel Zeta 引擎中，每文件的行数由 `batch_size` 与 `checkpoint.interval` 共同决定。 |
| common-options                   |         | 否       | -                                                         | Sink 插件通用参数，详情请参考 [Sink 通用选项](../common-options/sink-common-options.md)。 |

### jdbc_url

连接到Redshift数据库的JDBC URL。

### jdbc_user

连接到Redshift数据库的用户名。

### jdbc_password

连接到Redshift数据库的密码。

### execute_sql

数据写入S3后要执行的SQL。

示例:

```sql

COPY target_table FROM 's3://yourbucket${path}' IAM_ROLE 'arn:XXX' REGION 'your region' format as json 'auto';
```

`target_table`是Redshift中的表名。

`${path}`是写入S3的文件的路径。请确认您的sql包含此变量。并且不需要替换它。我们将在执行sql时替换它。
IAM_ROLE是有权访问S3的角色。
format是写入S3的文件的格式。请确认此格式与您在配置中设置的文件格式相同。

请参阅[Redshift COPY](https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html)了解更多详情。

请确认该角色有权访问S3。
### path [string]

目标目录路径是必填项。

### bucket [string]

s3文件系统的bucket地址，例如：`s3n://seatunnel-test`，如果使用`s3a`协议，则此参数应为`s3a://seatunnel-test`。

### access_key [string]

s3文件系统的access_key。如果未设置此参数，请确认凭据提供程序链可以正确进行身份验证，您可以检查这个[hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### access_secret [string]

s3文件系统的access_secret。如果未设置此参数，请确认凭据提供程序链可以正确进行身份验证，您可以检查这个[hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### hadoop_s3_properties [map]

如果您需要添加其他选项，可以在此处添加并参考[Hadoop-AWS](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
  "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
 }
```

### file_name_expression [string]

`file_name_expression`描述了将在`path`中创建的文件表达式。我们可以在`file_name_expression`中添加变量`${now}`或`${uuid}`，类似于`test_${uuid}_${now}`，
`${now}`表示当前时间，其格式可以通过指定选项`filename_time_format`来定义。
请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。

### file_format_type [string]

我们支持以下文件类型：

`text` `csv` `parquet` `orc` `json`

请注意，最终文件名将以file_format_type的后缀结尾，文本文件的后缀为“txt”。

### filename_time_format [string]

当`file_name_expression`参数中的格式为`xxxx-${now}`时，`filename_time_format`可以指定路径的时间格式，默认值为`yyyy.MM.dd`。常用的时间格式如下：

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

请参阅[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)了解详细的时间格式语法。

### field_delimiter [string]

数据行中列之间的分隔符。仅被“text”和“csv”文件格式需要。

### row_delimiter [string]

文件中行之间的分隔符。仅被“text”和“csv”文件格式需要。

### partition_by [array]

基于选定字段对数据进行分区

### partition_dir_expression [string]

如果指定了`partition_by`，我们将根据分区信息生成相应的分区目录，并将最终文件放置在分区目录中。

默认的`partition_dir_expression`是 `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`。`k0`是第一个分区字段，`v0`是第一个划分字段的值。

### is_partition_field_write_in_file [boolean]

如果`is_partition_field_write_in_file`为`true`，则分区字段及其值将写入数据文件。

例如，如果你想写一个Hive数据文件，它的值应该是“false”。

### sink_columns [array]

哪些列需要写入文件，默认值是从“Transform”或“Source”获取的所有列。
字段的顺序决定了文件实际写入的顺序。

### is_enable_transaction [boolean]

如果`is_enable_transaction`为true，我们将确保数据在写入目标目录时不会丢失或重复。
请注意，如果`is_enable_transaction`为`true`，我们将自动添加`${transactionId}_`在文件的开头。
现在只支持“true”。

### batch_size [int]

文件中的最大行数。对于SeaTunnel引擎，文件中的行数由“batch_size”和“checkpoint.interval”共同决定。如果“checkpoint.interval”的值足够大，sink writer将在文件中写入行，直到文件中的行大于“batch_size”。如果“checkpoint.interval”较小，则接收器写入程序将在新的检查点触发时创建一个新文件。

### common options

Sink插件常用参数，请参考[Sink Common Options](../common-options/sink-common-options.md)了解详细信息。

## 示例

用于 text 文件格式

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' removequotes emptyasnull blanksasnull maxerror 100 delimiter '|' ;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/text"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "text"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

用于 parquet 文件格式

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' format as PARQUET;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/parquet"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "parquet"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

用于 orc 文件格式

```hocon

  S3Redshift {
    jdbc_url = "jdbc:redshift://xxx.amazonaws.com.cn:5439/xxx"
    jdbc_user = "xxx"
    jdbc_password = "xxxx"
    execute_sql="COPY table_name FROM 's3://test${path}' IAM_ROLE 'arn:aws-cn:iam::xxx' REGION 'cn-north-1' format as ORC;"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/orc"
    row_delimiter="\n"
    partition_dir_expression="${k0}=${v0}"
    is_partition_field_write_in_file=true
    file_name_expression="${transactionId}_${now}"
    file_format_type = "orc"
    filename_time_format="yyyy.MM.dd"
    is_enable_transaction=true
    hadoop_s3_properties {
       "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    }
  }

```

## 变更日志

<ChangeLog />
