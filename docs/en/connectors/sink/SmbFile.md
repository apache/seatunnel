import ChangeLog from '../changelog/connector-file-smb.md';

# SmbFile

> SMB file sink connector

## Description

Output data to SMB (Server Message Block) file shares.

:::tip

If you use spark/flink, In order to use this connector, You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.

If you use SeaTunnel Engine, It automatically integrated the hadoop jar when you download and install SeaTunnel Engine. You can check the jar package under ${SEATUNNEL_HOME}/lib to confirm this.

:::

## Key features

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

## Options

| name                                  | type    | required | default value                              | remarks                                                                                                                                                                         |
|---------------------------------------|---------|----------|--------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | string  | yes      | -                                          | The SMB server host                                                                                                                                                             |
| port                                  | int     | no       | 445                                        | The SMB server port                                                                                                                                                             |
| user                                  | string  | yes      | -                                          | The SMB authentication username                                                                                                                                                 |
| password                              | string  | no       | -                                          | The SMB authentication password                                                                                                                                                 |
| domain                                | string  | no       | (empty)                                    | The SMB authentication domain (e.g. WORKGROUP)                                                                                                                                  |
| share                                 | string  | yes      | -                                          | The SMB share name to connect to                                                                                                                                                |
| path                                  | string  | yes      | -                                          | The target file path within the share                                                                                                                                           |
| tmp_path                              | string  | no       | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir.                                                                               |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename                                                                                                                                            |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when custom_filename is true                                                                                                                                          |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when custom_filename is true                                                                                                                                          |
| file_format_type                      | string  | no       | "csv"                                      | Supported file types: text, csv, parquet, orc, json, excel, xml, binary                                                                                                        |
| filename_extension                    | string  | no       | -                                          | Override the default file name extensions with custom file name extensions                                                                                                      |
| field_delimiter                       | string  | no       | '\001' for text and ',' for csv            | Only used when file_format_type is text and csv                                                                                                                                 |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format_type is text, csv and json                                                                                                                           |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                                                         |
| partition_by                          | array   | no       | -                                          | Only used when have_partition is true                                                                                                                                           |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used when have_partition is true                                                                                                                                           |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used when have_partition is true                                                                                                                                           |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns                                                                                                                       |
| is_enable_transaction                 | boolean | no       | true                                       |                                                                                                                                                                                 |
| batch_size                            | int     | no       | 1000000                                    |                                                                                                                                                                                 |
| compress_codec                        | string  | no       | none                                       |                                                                                                                                                                                 |
| common-options                        | object  | no       | -                                          |                                                                                                                                                                                 |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when file_format_type is excel.                                                                                                                                       |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when file_format_type is excel.                                                                                                                                       |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml.                                                                                                                                              |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml.                                                                                                                                              |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml.                                                                                                                                              |
| single_file_mode                      | boolean | no       | false                                      | Each parallelism will only output one file.                                                                                                                                     |
| encoding                              | string  | no       | UTF-8                                      | Only used when file_format_type is text, json, csv, xml                                                                                                                         |
| date_format                           | string  | no       | yyyy-MM-dd                                 | Date type format                                                                                                                                                                |
| datetime_format                       | string  | no       | yyyy-MM-dd HH:mm:ss                        | Datetime type format                                                                                                                                                            |
| time_format                           | string  | no       | HH:mm:ss                                   | Time type format                                                                                                                                                                |
| create_empty_file_when_no_data        | boolean | no       | false                                      | Whether to create an empty file when no data                                                                                                                                    |
| schema_save_mode                      | string  | no       | CREATE_SCHEMA_WHEN_NOT_EXIST               | Existing dir processing method                                                                                                                                                  |
| data_save_mode                        | string  | no       | APPEND_DATA                                | Existing data processing method                                                                                                                                                 |
| enable_header_write                   | boolean | no       | false                                      | Only used when file_format_type is text, csv. false: don't write header, true: write header.                                                                                    |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                                                          |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                                                          |

## Example

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

sink {
  SmbFile {
    host = "192.168.1.100"
    port = 445
    user = seatunnel
    password = pass
    domain = "WORKGROUP"
    share = "data"
    path = "/output/csv"
    tmp_path = "/tmp/seatunnel"
    file_format_type = "csv"
  }
}
```

## Changelog

<ChangeLog />
