import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS file source connector

## Support those engines

> Spark
>
> Flink
>
> Seatunnel Zeta

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Description

Read data from Baidu Cloud BOS (Baidu Object Storage) via the BOS HDFS SDK.

If you use spark/flink, you must ensure your cluster already integrated hadoop 2.x.

If you use SeaTunnel Engine, hadoop jars are bundled under `${SEATUNNEL_HOME}/lib`.

## Required Jar List

| jar | supported versions | download |
|-----|-------------------|----------|
| bos-hdfs-sdk | >= 1.0.4-community | [Download](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip) |

> Install `bos-hdfs-sdk` into your local Maven repository before building from source. See `connector-file-bos/lib/README.md`.
>
> Copy required jars to `$SEATUNNEL_HOME/lib/` when running with Spark/Flink.

## Options

| name | type | required | default | description |
|------|------|----------|---------|-------------|
| path | string | yes | - | The target dir path under the bucket |
| file_format_type | string | yes | - | File type: text, csv, json, parquet, orc |
| bucket | string | yes | - | BOS bucket, for example: `bos://my-bucket` |
| access_key | string | yes | - | BOS access key |
| secret_key | string | yes | - | BOS secret key |
| endpoint | string | yes | - | BOS endpoint, for example: `http://bj.bcebos.com` |
| schema | config | conditional | - | Required for text/csv/json source |
| row_delimiter | string | no | `\n` | Row delimiter for text files |
| field_delimiter | string | no | `\001` | Field delimiter for text files |
| parse_partition_from_path | boolean | no | true | Parse partition keys from file path |
| recursive_file_scan | boolean | no | true | Scan subdirectories recursively |

## Example

```hocon
source {
  BosFile {
    bucket = "bos://source-bucket"
    path = "/warehouse/table/"
    file_format_type = "text"
    access_key = "your-access-key"
    secret_key = "your-secret-key"
    endpoint = "http://bj.bcebos.com"
    schema {
      fields {
        id = int
        name = string
      }
    }
  }
}
```
