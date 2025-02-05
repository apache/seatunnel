# ObsFile

> Obs file sink connector

## Support those engines

> Spark
>
> Flink
>
> Seatunnel Zeta

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

## Description

Output data to huawei cloud obs file system.

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

We made some trade-offs in order to support more file types, so we used the HDFS protocol for internal access to OBS and this connector need some hadoop dependencies.
It only supports hadoop version **2.9.X+**.

## Required Jar List

|        jar         |     supported versions      | maven                                                                                                 |
|--------------------|-----------------------------|-------------------------------------------------------------------------------------------------------|
| hadoop-huaweicloud | support version >= 3.1.1.29 | [Download](https://repo.huaweicloud.com/artifactory/sdk_public/org/apache/hadoop/hadoop-huaweicloud/) |
| esdk-obs-java      | support version >= 3.19.7.3 | [Download](https://repo.huaweicloud.com/artifactory/sdk_public/com/huawei/storage/esdk-obs-java/)     |
| okhttp             | support version >= 3.11.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okhttp3/okhttp/)                               |
| okio               | support version >= 1.14.0   | [Download](https://repo1.maven.org/maven2/com/squareup/okio/okio/)                                    |

> Please download the support list corresponding to 'Maven' and copy them to the '$SEATUNNEL_HOME/plugins/jdbc/lib/' working directory.
>
> And copy all jars to $SEATUNNEL_HOME/lib/

## Options

| name                             | type    | required | default                                    | description                                                                                                                                                            |
|----------------------------------|---------|----------|--------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                             | string  | yes      | -                                          | The target dir path.                                                                                                                                                   |
| bucket                           | string  | yes      | -                                          | The bucket address of obs file system, for example: `obs://obs-bucket-name`.                                                                                           |
| access_key                       | string  | yes      | -                                          | The access key of obs file system.                                                                                                                                     |
| access_secret                    | string  | yes      | -                                          | The access secret of obs file system.                                                                                                                                  |
| endpoint                         | string  | yes      | -                                          | The endpoint of obs file system.                                                                                                                                       |
| custom_filename                  | boolean | no       | false                                      | Whether you need custom the filename.                                                                                                                                  |
| file_name_expression             | string  | no       | "${transactionId}"                         | Describes the file expression which will be created into the `path`. Only used when custom_filename is true. [Tips](#file_name_expression)                             |
| filename_time_format             | string  | no       | "yyyy.MM.dd"                               | Specify the time format of the `path`. Only used when custom_filename is true. [Tips](#filename_time_format)                                                           |
| file_format_type                 | string  | no       | "csv"                                      | Supported file types. [Tips](#file_format_type)                                                                                                                        |
| field_delimiter                  | string  | no       | '\001'                                     | The separator between columns in a row of data.Only used when file_format is text.                                                                                     |
| row_delimiter                    | string  | no       | "\n"                                       | The separator between rows in a file. Only needed by `text` file format.                                                                                               |
| have_partition                   | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                |
| partition_by                     | array   | no       | -                                          | Partition data based on selected fields. Only used then have_partition is true.                                                                                        |
| partition_dir_expression         | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used then have_partition is true.[Tips](#partition_dir_expression)                                                                                                |
| is_partition_field_write_in_file | boolean | no       | false                                      | Only used then have_partition is true.[Tips](#is_partition_field_write_in_file)                                                                                        |
| sink_columns                     | array   | no       |                                            | When this parameter is empty, all fields are sink columns.[Tips](#sink_columns)                                                                                        |
| is_enable_transaction            | boolean | no       | true                                       | [Tips](#is_enable_transaction)                                                                                                                                         |
| batch_size                       | int     | no       | 1000000                                    | [Tips](#batch_size)                                                                                                                                                    |
| single_file_mode                 | boolean | no       | false                                      | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix. |
| create_empty_file_when_no_data   | boolean | no       | false                                      | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                      |
| compress_codec                   | string  | no       | none                                       | [Tips](#compress_codec)                                                                                                                                                |
| common-options                   | object  | no       | -                                          | [Tips](#common_options)                                                                                                                                                |
| max_rows_in_memory               | int     | no       | -                                          | When File Format is Excel,The maximum number of data items that can be cached in the memory.Only used when file_format is excel.                                       |
| sheet_name                       | string  | no       | Sheet${Random number}                      | Writer the sheet of the workbook. Only used when file_format is excel.                                                                                                 |

### Tips

#### <span id="file_name_expression"> file_name_expression </span>

> Only used when `custom_filename` is `true`
>
> `file_name_expression` describes the file expression which will be created into the `path`.
>
> We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
>
> `${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

#### <span id="filename_time_format"> filename_time_format </span>

> Only used when `custom_filename` is `true`
>
> When the format in the `file_name_expression` parameter is `xxxx-${now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

#### <span id="file_format_type"> file_format_type </span>

> We supported as the following file types:
>
> `text` `json` `csv` `orc` `parquet` `excel`

Please note that, The final file name will end with the file_format's suffix, the suffix of the text file is `txt`.

#### <span id="partition_dir_expression"> partition_dir_expression </span>

> Only used when `have_partition` is `true`.
>
> If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.
>
> Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

#### <span id="is_partition_field_write_in_file"> is_partition_field_write_in_file </span>

> Only used when `have_partition` is `true`.
>
> If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.
>
> For example, if you want to write a Hive Data File, Its value should be `false`.

#### <span id="sink_columns"> sink_columns </span>

> Which columns need be written to file, default value is all the columns get from `Transform` or `Source`.
> The order of the fields determines the order in which the file is actually written.

#### <span id="is_enable_transaction"> is_enable_transaction </span>

> If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.
>
> Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file. Only support `true` now.

#### <span id="batch_size"> batch_size </span>

> The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

#### <span id="compress_codec"> compress_codec </span>

> The compress codec of files and the details that supported as the following shown:
>
> - txt: `lzo` `none`
> - json: `lzo` `none`
> - csv: `lzo` `none`
> - orc: `lzo` `snappy` `lz4` `zlib` `none`
> - parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

Please note that excel type does not support any compression format

#### <span id="common_options"> common options </span>

> Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

## Task Example

### text file

> For text file format with `have_partition` and `custom_filename` and `sink_columns`

```hocon

  ObsFile {
    path="/seatunnel/text"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
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

### parquet file

> For parquet file format with `have_partition` and `sink_columns`

```hocon

  ObsFile {
    path = "/seatunnel/parquet"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxxxxxxxx"
    endpoint = "obs.xxxxxx.myhuaweicloud.com"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }

```

### orc file

> For orc file format simple config

```hocon

  ObsFile {
    path="/seatunnel/orc"
    bucket = "obs://obs-bucket-name"
    access_key = "xxxxxxxxxxx"
    access_secret = "xxxxxxxxxxx"
    endpoint = "obs.xxxxx.myhuaweicloud.com"
    file_format_type = "orc"
  }

```

### json file

> For json file format simple config

```hcocn

   ObsFile {
       path = "/seatunnel/json"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "json"
   }

```

### excel file

> For excel file format simple config

```hcocn

   ObsFile {
       path = "/seatunnel/excel"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "excel"
   }

```

### csv file

> For csv file format simple config

```hcocn

   ObsFile {
       path = "/seatunnel/csv"
       bucket = "obs://obs-bucket-name"
       access_key = "xxxxxxxxxxx"
       access_secret = "xxxxxxxxxxx"
       endpoint = "obs.xxxxx.myhuaweicloud.com"
       file_format_type = "csv"
   }

```

## Changelog

### next version

- Add Obs Sink Connector

