import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS file source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [multimodal](../../introduction/concepts/connector-v2-features.md#multimodal)

  Use binary file format to read and write files in any format, such as videos, pictures, etc. In short, any files can be synchronized to the target place.

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)

  Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
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

## Description

Read data from Baidu Cloud BOS (Baidu Object Storage) via the BOS HDFS SDK.

:::tip

If you use Spark/Flink, in order to use this connector you must ensure your Spark/Flink cluster already integrated Hadoop. The tested Hadoop version is 2.x.

If you use SeaTunnel Engine, Hadoop jars are bundled under `${SEATUNNEL_HOME}/lib`.

To use this connector you need to put `bos-hdfs-sdk` (>= 1.0.4-community) into `${SEATUNNEL_HOME}/lib`. Download: [bos-hdfs-sdk-1.0.4-community.jar.zip](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip). See `connector-file-bos/lib/README.md` for details.

:::

## Options

| name                       | type    | required | default value               |
|----------------------------|---------|----------|-----------------------------|
| path                       | string  | yes      | -                           |
| file_format_type           | string  | yes      | -                           |
| bucket                     | string  | yes      | -                           |
| access_key                 | string  | yes      | -                           |
| secret_key                 | string  | yes      | -                           |
| endpoint                   | string  | yes      | -                           |
| read_columns               | list    | no       | -                           |
| delimiter/field_delimiter  | string  | no       | \001 for text and , for csv |
| row_delimiter              | string  | no       | \n                          |
| parse_partition_from_path  | boolean | no       | true                        |
| skip_header_row_number     | long    | no       | 0                           |
| date_format                | string  | no       | yyyy-MM-dd                  |
| datetime_format            | string  | no       | yyyy-MM-dd HH:mm:ss         |
| time_format                | string  | no       | HH:mm:ss                    |
| schema                     | config  | no       | -                           |
| sheet_name                 | string  | no       | -                           |
| excel_engine               | string  | no       | POI                         |
| poi_excel_max_file_size    | long    | no       | 52428800                    |
| xml_row_tag                | string  | no       | -                           |
| xml_use_attr_format        | boolean | no       | -                           |
| csv_use_header_line        | boolean | no       | false                       |
| file_filter_pattern        | string  | no       | -                           |
| filename_extension         | string  | no       | -                           |
| compress_codec             | string  | no       | none                        |
| archive_compress_codec     | string  | no       | none                        |
| encoding                   | string  | no       | UTF-8                       |
| binary_chunk_size          | int     | no       | 1024                        |
| binary_complete_file_mode  | boolean | no       | false                       |
| common-options             |         | no       | -                           |
| file_filter_modified_start | string  | no       | -                           |
| file_filter_modified_end   | string  | no       | -                           |
| quote_char                 | string  | no       | "                           |
| escape_char                | string  | no       | -                           |
| recursive_file_scan        | boolean | no       | true                        |
| sort_files_by_modification_time | boolean | no       | false                       |

### path [string]

The source file path under the bucket.

### bucket [string]

The BOS bucket address, for example `bos://my-bucket`.

### access_key [string]

The Baidu Cloud BOS access key.

### secret_key [string]

The Baidu Cloud BOS secret key.

### endpoint [string]

The BOS endpoint, for example `http://bj.bcebos.com`.

### file_format_type [string]

Supported file types: `text`, `csv`, `parquet`, `orc`, `json`, `excel`, `xml`, `binary`, `markdown`, `pdf`.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

```hocon
source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/warehouse/table/"
    file_format_type = "orc"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
  }
}
```

### Transfer Binary File

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/read/binary/"
    file_format_type = "binary"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    binary_chunk_size = 2048
  }
}

sink {
  BosFile {
    bucket = "bos://sink-bucket"
    path = "/write/binary/"
    file_format_type = "binary"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
  }
}
```

### Filter File

```hocon
source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/read/data/"
    file_format_type = "text"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    file_filter_pattern = "abc[DX]*.*"
    schema {
      fields {
        id = int
        name = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />
