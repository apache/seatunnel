# S3File

> S3 file sink connector

## Description

Output data to aws s3 file system.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

To use this connector you need put hadoop-aws-3.1.4.jar and aws-java-sdk-bundle-1.11.271.jar in ${SEATUNNEL_HOME}/lib dir.

:::

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel

## Options

|               name               |  type   | required |                     default value                     |                                                remarks                                                 |
|----------------------------------|---------|----------|-------------------------------------------------------|--------------------------------------------------------------------------------------------------------|
| path                             | string  | yes      | -                                                     |                                                                                                        |
| bucket                           | string  | yes      | -                                                     |                                                                                                        |
| fs.s3a.endpoint                  | string  | yes      | -                                                     |                                                                                                        |
| fs.s3a.aws.credentials.provider  | string  | yes      | com.amazonaws.auth.InstanceProfileCredentialsProvider |                                                                                                        |
| access_key                       | string  | no       | -                                                     | Only used when fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider |
| access_secret                    | string  | no       | -                                                     | Only used when fs.s3a.aws.credentials.provider = org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider |
| custom_filename                  | boolean | no       | false                                                 | Whether you need custom the filename                                                                   |
| file_name_expression             | string  | no       | "${transactionId}"                                    | Only used when custom_filename is true                                                                 |
| filename_time_format             | string  | no       | "yyyy.MM.dd"                                          | Only used when custom_filename is true                                                                 |
| file_format_type                 | string  | no       | "csv"                                                 |                                                                                                        |
| field_delimiter                  | string  | no       | '\001'                                                | Only used when file_format_type is text                                                                |
| row_delimiter                    | string  | no       | "\n"                                                  | Only used when file_format_type is text                                                                |
| have_partition                   | boolean | no       | false                                                 | Whether you need processing partitions.                                                                |
| partition_by                     | array   | no       | -                                                     | Only used then have_partition is true                                                                  |
| partition_dir_expression         | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"            | Only used then have_partition is true                                                                  |
| is_partition_field_write_in_file | boolean | no       | false                                                 | Only used then have_partition is true                                                                  |
| sink_columns                     | array   | no       |                                                       | When this parameter is empty, all fields are sink columns                                              |
| is_enable_transaction            | boolean | no       | true                                                  |                                                                                                        |
| batch_size                       | int     | no       | 1000000                                               |                                                                                                        |
| compress_codec                   | string  | no       | none                                                  |                                                                                                        |
| common-options                   | object  | no       | -                                                     |                                                                                                        |
| max_rows_in_memory               | int     | no       | -                                                     | Only used when file_format_type is excel.                                                              |
| sheet_name                       | string  | no       | Sheet${Random number}                                 | Only used when file_format_type is excel.                                                              |

### path [string]

The target dir path is required.

### bucket [string]

The bucket address of s3 file system, for example: `s3n://seatunnel-test`, if you use `s3a` protocol, this parameter should be `s3a://seatunnel-test`.

### fs.s3a.endpoint [string]

fs s3a endpoint

### fs.s3a.aws.credentials.provider [string]

The way to authenticate s3a. We only support `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider` and `com.amazonaws.auth.InstanceProfileCredentialsProvider` now.

More information about the credential provider you can see [Hadoop AWS Document](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html#Simple_name.2Fsecret_credentials_with_SimpleAWSCredentialsProvider.2A)

### access_key [string]

The access key of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### access_secret [string]

The access secret of s3 file system. If this parameter is not set, please confirm that the credential provider chain can be authenticated correctly, you could check this [hadoop-aws](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

### hadoop_s3_properties [map]

If you need to add a other option, you could add it here and refer to this [link](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html)

```
hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
   }
```

### custom_filename [boolean]

Whether custom the filename

### file_name_expression [string]

Only used when `custom_filename` is `true`

`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

### filename_time_format [string]

Only used when `custom_filename` is `true`

When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

We supported as the following file types:

`text` `json` `csv` `orc` `parquet` `excel`

Please note that, The final file name will end with the file_format_type's suffix, the suffix of the text file is `txt`.

### field_delimiter [string]

The separator between columns in a row of data. Only needed by `text` file format.

### row_delimiter [string]

The separator between rows in a file. Only needed by `text` file format.

### have_partition [boolean]

Whether you need processing partitions.

### partition_by [array]

Only used when `have_partition` is `true`.

Partition data based on selected fields.

### partition_dir_expression [string]

Only used when `have_partition` is `true`.

If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.

Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

### is_partition_field_write_in_file [boolean]

Only used when `have_partition` is `true`.

If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.

For example, if you want to write a Hive Data File, Its value should be `false`.

### sink_columns [array]

Which columns need be written to file, default value is all the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

Only support `true` now.

### batch_size [int]

The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

Tips: excel type does not support any compression format

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

### max_rows_in_memory [int]

When File Format is Excel,The maximum number of data items that can be cached in the memory.

### sheet_name [string]

Writer the sheet of the workbook

## Example

For text file format with `have_partition` and `custom_filename` and `sink_columns` and `com.amazonaws.auth.InstanceProfileCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/text"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="com.amazonaws.auth.InstanceProfileCredentialsProvider"
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name","age"]
    is_enable_transaction=true
    hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
    }
  }

```

For parquet file format simple config with `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/parquet"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "parquet"
    hadoop_s3_properties {
      "fs.s3a.buffer.dir" = "/data/st_test/s3a"
      "fs.s3a.fast.upload.buffer" = "disk"
    }
  }

```

For orc file format simple config with `org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider`

```hocon

  S3File {
    bucket = "s3a://seatunnel-test"
    tmp_path = "/tmp/seatunnel"
    path="/seatunnel/orc"
    fs.s3a.endpoint="s3.cn-north-1.amazonaws.com.cn"
    fs.s3a.aws.credentials.provider="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    access_key = "xxxxxxxxxxxxxxxxx"
    secret_key = "xxxxxxxxxxxxxxxxx"
    file_format_type = "orc"
  }

```

## Changelog

### 2.3.0-beta 2022-10-20

- Add S3File Sink Connector

### 2.3.0 2022-12-30

- [BugFix] Fixed the following bugs that failed to write data to files ([3258](https://github.com/apache/seatunnel/pull/3258))
  - When field from upstream is null it will throw NullPointerException
  - Sink columns mapping failed
  - When restore writer from states getting transaction directly failed
- [Feature] Support S3A protocol ([3632](https://github.com/apache/seatunnel/pull/3632))
  - Allow user to add additional hadoop-s3 parameters
  - Allow the use of the s3a protocol
  - Decouple hadoop-aws dependencies
- [Improve] Support setting batch size for every file ([3625](https://github.com/apache/seatunnel/pull/3625))
- [Feature]Set S3 AK to optional ([3688](https://github.com/apache/seatunnel/pull/))

### Next version

- ​	[Improve] Support file compress ([3899](https://github.com/apache/seatunnel/pull/3899))

