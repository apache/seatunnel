# SftpFile

> Sftp file sink connector

## Description

Output data to Sftp .

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] file format
    - [x] text
    - [x] csv
    - [x] parquet
    - [x] orc
    - [x] json

##  Options

| name                             | type    | required | default value                                             |
|----------------------------------|---------|----------|-----------------------------------------------------------|
| host                             | string  | yes      | -                                                         |
| port                             | int     | yes      | -                                                         |
| username                         | string  | yes      | -                                                         |
| password                         | string  | yes      | -                                                         |
| path                             | string  | yes      | -                                                         |
| file_name_expression             | string  | no       | "${transactionId}"                                        |
| file_format                      | string  | no       | "text"                                                    |
| filename_time_format             | string  | no       | "yyyy.MM.dd"                                              |
| field_delimiter                  | string  | no       | '\001'                                                    |
| row_delimiter                    | string  | no       | "\n"                                                      |
| partition_by                     | array   | no       | -                                                         |
| partition_dir_expression         | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/"                |
| is_partition_field_write_in_file | boolean | no       | false                                                     |
| sink_columns                     | array   | no       | When this parameter is empty, all fields are sink columns |
| is_enable_transaction            | boolean | no       | true                                                      |
| batch_size                       | int     | no       | 1000000                                                   |
| common-options                   |         | no       | -                                                         |

### host [string]

The target sftp host is required

### port [int]

The target sftp port is required

### username [string]

The target sftp username is required

### password [string]

The target sftp password is required

### path [string]

The target dir path is required.

### file_name_expression [string]

`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

### file_format [string]

We supported as the following file types:

`text` `json` `csv` `orc` `parquet`

Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.

### filename_time_format [string]

When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol | Description        |
| ------ | ------------------ |
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |


### field_delimiter [string]

The separator between columns in a row of data. Only needed by `text` file format.

### row_delimiter [string]

The separator between rows in a file. Only needed by `text` and `csv` file format.

### partition_by [array]

Partition data based on selected fields

### partition_dir_expression [string]

If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.

Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

### is_partition_field_write_in_file [boolean]

If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.

For example, if you want to write a Hive Data File, Its value should be `false`.

### sink_columns [array]

Which columns need be wrote to file, default value is all the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

Only support `true` now.

### batch_size [int]

The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Example

For text file format

```bash

SftpFile {
    host = "xxx.xxx.xxx.xxx"
    port  =22
    username = "username"
    password = "password"
    path = "/data/sftp"
    field_delimiter = "\t"
    row_delimiter = "\n"
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_name_expression = "${transactionId}_${now}"
    file_format = "text"
    sink_columns = ["name","age"]
    filename_time_format = "yyyy.MM.dd"
    is_enable_transaction = true
}

```

## Changelog

### Next version

- Add SftpFile Sink Connector
- [BugFix] Fixed the following bugs that failed to write data to files ([3258](https://github.com/apache/incubator-seatunnel/pull/3258))
  - When field from upstream is null it will throw NullPointerException
  - Sink columns mapping failed
  - When restore writer from states getting transaction directly failed

- [Improve] Support setting batch size for every file ([3625](https://github.com/apache/incubator-seatunnel/pull/3625))