# OssJindoFile

> OssJindo file sink connector

## Description

Output data to oss file system using jindo api.

:::tip

You need to download [jindosdk-4.6.1.tar.gz](https://jindodata-binary.oss-cn-shanghai.aliyuncs.com/release/4.6.1/jindosdk-4.6.1.tar.gz)
and then unzip it, copy jindo-sdk-4.6.1.jar and jindo-core-4.6.1.jar from lib to ${SEATUNNEL_HOME}/lib.

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OSS and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

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
  - [x] xml
  - [x] binary

## Options

| Name                                  | Type    | Required | Default                                    | Description                                                                                                                                                            |
|---------------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | yes      | -                                          |                                                                                                                                                                        |
| tmp_path                              | string  | no       | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a OSS dir.                                                      |
| bucket                                | string  | yes      | -                                          |                                                                                                                                                                        |
| access_key                            | string  | yes      | -                                          |                                                                                                                                                                        |
| access_secret                         | string  | yes      | -                                          |                                                                                                                                                                        |
| endpoint                              | string  | yes      | -                                          |                                                                                                                                                                        |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename                                                                                                                                   |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when custom_filename is true                                                                                                                                 |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when custom_filename is true                                                                                                                                 |
| file_format_type                      | string  | no       | "csv"                                      |                                                                                                                                                                        |
| field_delimiter                       | string  | no       | '\001'                                     | Only used when file_format_type is text                                                                                                                                |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format_type is text                                                                                                                                |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                |
| partition_by                          | array   | no       | -                                          | Only used then have_partition is true                                                                                                                                  |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used then have_partition is true                                                                                                                                  |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used then have_partition is true                                                                                                                                  |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns                                                                                                              |
| is_enable_transaction                 | boolean | no       | true                                       |                                                                                                                                                                        |
| batch_size                            | int     | no       | 1000000                                    |                                                                                                                                                                        |
| compress_codec                        | string  | no       | none                                       |                                                                                                                                                                        |
| common-options                        | object  | no       | -                                          |                                                                                                                                                                        |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when file_format_type is excel.                                                                                                                              |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when file_format_type is excel.                                                                                                                              |
| csv_string_quote_mode                 | enum    | no       | MINIMAL                                    | Only used when file_format is csv.                                                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml.                                                                                                                                     |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml.                                                                                                                                     |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml.                                                                                                                                     |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix. |
| create_empty_file_when_no_data        | boolean | no       | false                                      | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                      |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                                                 |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when file_format_type is json,text,csv,xml.                                                                                                                  |

### path [string]

The target dir path is required.

### bucket [string]

The bucket address of oss file system, for example: `oss://tyrantlucifer-image-bed`

### access_key [string]

The access key of oss file system.

### access_secret [string]

The access secret of oss file system.

### endpoint [string]

The endpoint of oss file system.

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

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

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

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

### max_rows_in_memory [int]

When File Format is Excel,The maximum number of data items that can be cached in the memory.

### sheet_name [string]

Writer the sheet of the workbook

### csv_string_quote_mode [string]

When File Format is CSV,The string quote mode of CSV.

- ALL: All String fields will be quoted.
- MINIMAL: Quotes fields which contain special characters such as a the field delimiter, quote character or any of the characters in the line separator string.
- NONE: Never quotes fields. When the delimiter occurs in data, the printer prefixes it with the escape character. If the escape character is not set, format validation throws an exception.

### xml_root_tag [string]

Specifies the tag name of the root element within the XML file.

### xml_row_tag [string]

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Specifies Whether to process data using the tag attribute format.

### parquet_avro_write_timestamp_as_int96 [boolean]

Support writing Parquet INT96 from a timestamp, only valid for parquet files.

### parquet_avro_write_fixed_as_int96 [array]

Support writing Parquet INT96 from a 12-byte field, only valid for parquet files.

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to write. This param will be parsed by `Charset.forName(encoding)`.

## Example

For text file format with `have_partition` and `custom_filename` and `sink_columns`

```hocon

  OssJindoFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
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
    is_enable_transaction = true
  }

```

For parquet file format with `sink_columns`

```hocon

  OssJindoFile {
    path = "/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }

```

For orc file format simple config

```bash

  OssJindoFile {
    path="/seatunnel/sink"
    bucket = "oss://tyrantlucifer-image-bed"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "oss-cn-beijing.aliyuncs.com"
    file_format_type = "orc"
  }

```

## Changelog

### 2.3.0 2022-12-30

- Add OSS Jindo File Sink Connector

### Next version

- [Improve] Support file compress ([3899](https://github.com/apache/seatunnel/pull/3899))

