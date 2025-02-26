# S3Redshift

>S3Redshift的作用是将数据写入S3，然后使用Redshift的COPY命令将数据从S3导入Redshift。

## 描述

将数据输出到AWS Redshift。

>提示：

>我们基于[S3File]（S3File.md）来实现这个连接器。因此，您可以使用与S3File相同的配置。
>为了支持更多的文件类型，我们进行了一些权衡，因此我们使用HDFS协议对S3进行内部访问，而这个连接器需要一些hadoop依赖。
>它只支持hadoop版本**2.6.5+**。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)

默认情况下，我们使用2PC commit来确保“精确一次”`

- [x] 文件格式类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## 参数

|               名称               |  类型   | 是否必填 |                       默认值                       |
|----------------------------------|---------|----------|-----------------------------------------------------------|
| jdbc_url                         | string  | 是      | -                                                         |
| jdbc_user                        | string  | 是      | -                                                         |
| jdbc_password                    | string  | 是      | -                                                         |
| execute_sql                      | string  | 是      | -                                                         |
| path                             | string  | 是      | -                                                         |
| bucket                           | string  | 是      | -                                                         |
| access_key                       | string  | 否       | -                                                         |
| access_secret                    | string  | 否       | -                                                         |
| hadoop_s3_properties             | map     | 否       | -                                                         |
| file_name_expression             | string  | 否       | "${transactionId}"                                        |
| file_format_type                 | string  | 否       | "text"                                                    |
| filename_time_format             | string  | 否       | "yyyy.MM.dd"                                              |
| field_delimiter                  | string  | 否       | '\001'                                                    |
| row_delimiter                    | string  | 否       | "\n"                                                      |
| partition_by                     | array   | 否       | -                                                         |
| partition_dir_expression         | string  | 否       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"                |
| is_partition_field_write_in_file | boolean | 否       | false                                                     |
| sink_columns                     | array   | 否       | 当此参数为空时，所有字段都是sink列 |
| is_enable_transaction            | boolean | 否       | true                                                      |
| batch_size                       | int     | 否       | 1000000                                                   |
| common-options                   |         | 否       | -                                                         |

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

Sink插件常用参数，请参考[Sink Common Options]（../sink-common-options.md）了解详细信息。

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

### 2.3.0-beta 2022-10-20

