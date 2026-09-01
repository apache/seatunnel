import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Output data to Baidu Cloud BOS (Baidu Object Storage) via the BOS HDFS SDK.

:::tip

If you use Spark/Flink, in order to use this connector you must ensure your Spark/Flink cluster already integrated Hadoop. The tested Hadoop version is 2.x.

If you use SeaTunnel Engine, Hadoop jars are bundled under `${SEATUNNEL_HOME}/lib`.

To use this connector you need to put `bos-hdfs-sdk` (>= 1.0.4-community) into `${SEATUNNEL_HOME}/lib`. Download: [bos-hdfs-sdk-1.0.4-community.jar.zip](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip).

:::

## Key Features

- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  By default, we use 2PC commit to ensure `exactly-once`

- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary
  - [x] canal_json
  - [x] debezium_json
  - [x] maxwell_json

## Options

| Name                                  | Type    | Required | Default                                    | Description                                                                                                                                                                     |
|---------------------------------------|---------|----------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | yes      | -                                          | The target directory the sink writes to inside the bucket.                                                                                                                       |
| tmp_path                              | string  | no       | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Needs a BOS dir.                                                                |
| bucket                                | string  | yes      | -                                          | The BOS bucket address, for example `bos://my-bucket`.                                                                                                                          |
| access_key                            | string  | yes      | -                                          | The Baidu Cloud BOS access key.                                                                                                                                                  |
| secret_key                            | string  | yes      | -                                          | The Baidu Cloud BOS secret key.                                                                                                                                                 |
| endpoint                              | string  | yes      | -                                          | The BOS endpoint, for example `http://bj.bcebos.com`.                                                                                                                            |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename.                                                                                                                                            |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when custom_filename is true.                                                                                                                                          |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when custom_filename is true.                                                                                                                                          |
| file_format_type                      | string  | no       | "csv"                                      | File format type, supported: `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `canal_json`, `debezium_json`, `maxwell_json`.                                    |
| filename_extension                    | string  | no       | -                                          | Override the default file name extensions with custom file name extensions. E.g. `.xml`, `.json`, `dat`, `.customtype`                                                          |
| field_delimiter                       | string  | no       | '\001' for text and ',' for csv            | Only used when file_format_type is text and csv.                                                                                                                                |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format_type is `text`, `csv` and `json`.                                                                                                                    |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                         |
| partition_by                          | array   | no       | -                                          | Only used when have_partition is true.                                                                                                                                          |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used when have_partition is true.                                                                                                                                          |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used when have_partition is true.                                                                                                                                          |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns.                                                                                                                      |
| is_enable_transaction                 | boolean | no       | true                                       | If `true`, data will not be lost or duplicated when written to the target directory. When `true`, `${transactionId}_` is automatically prefixed to the file name.                |
| batch_size                            | int     | no       | 1000000                                    | The maximum number of rows in a file. For SeaTunnel Engine the file row count is jointly decided by `batch_size` and `checkpoint.interval`.                                       |
| compress_codec                        | string  | no       | none                                       | The compress codec of files. Excel does not support any compression format.                                                                                                     |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml.                                                                                                                                              |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml.                                                                                                                                              |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml.                                                                                                                                              |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism will only output one file. When this parameter is turned on, batch_size will not take effect. The output file name does not have a file block suffix.          |
| create_empty_file_when_no_data        | boolean | no       | false                                      | When there is no data synchronization upstream, the corresponding data files are still generated.                                                                               |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                                                          |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                                                          |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when file_format_type is json,text,csv,xml.                                                                                                                           |
| common-options                        | object  | no       | -                                          | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                      |

## Example

For text file format with `have_partition`, `custom_filename` and `sink_columns`:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  BosFile {
    path = "/sink"
    bucket = "bos://sink-bucket"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
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
    sink_columns = ["name", "age"]
    is_enable_transaction = true
  }
}
```

For parquet file format:

```hocon
sink {
  BosFile {
    path = "/sink"
    bucket = "bos://sink-bucket"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    file_format_type = "parquet"
    is_enable_transaction = true
  }
}
```

Simple text sink:

```hocon
sink {
  BosFile {
    bucket = "bos://sink-bucket"
    path = "/warehouse/table/"
    file_format_type = "text"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    row_delimiter = "\n"
    field_delimiter = ","
    is_enable_transaction = true
  }
}
```

## Changelog

<ChangeLog />
