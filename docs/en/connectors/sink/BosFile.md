import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS file sink connector

## Support those engines

> Spark
>
> Flink
>
> Seatunnel Zeta

## Key features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Description

Write data to Baidu Cloud BOS via the BOS HDFS SDK.

## Required Jar List

| jar | supported versions | download |
|-----|-------------------|----------|
| bos-hdfs-sdk | >= 1.0.4-community | [Download](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip) |

## Options

| name | type | required | default | description |
|------|------|----------|---------|-------------|
| path | string | yes | - | Target directory under the bucket |
| bucket | string | yes | - | BOS bucket, for example: `bos://my-bucket` |
| access_key | string | yes | - | BOS access key |
| secret_key | string | yes | - | BOS secret key |
| endpoint | string | yes | - | BOS endpoint, for example: `http://bj.bcebos.com` |
| file_format_type | string | no | csv | File format: text, csv, json, parquet, orc |
| row_delimiter | string | no | `\n` | Row delimiter for text/csv/json |
| field_delimiter | string | no | `\001` | Field delimiter for text |

## Example

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
  }
}
```
