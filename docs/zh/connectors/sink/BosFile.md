import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS 文件 Sink 连接器

## 支持的引擎

> Spark
>
> Flink
>
> SeaTunnel Zeta

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## 描述

通过 BOS HDFS SDK 将数据写入百度智能云 BOS。

## 依赖 Jar

| jar | 版本 | 下载 |
|-----|------|------|
| bos-hdfs-sdk | >= 1.0.4-community | [下载](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip) |

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| path | string | 是 | - | bucket 下的目标目录 |
| bucket | string | 是 | - | BOS bucket，例如 `bos://my-bucket` |
| access_key | string | 是 | - | BOS Access Key |
| secret_key | string | 是 | - | BOS Secret Key |
| endpoint | string | 是 | - | BOS Endpoint，例如 `http://bj.bcebos.com` |
| file_format_type | string | 否 | csv | 文件格式 |
| row_delimiter | string | 否 | `\n` | 行分隔符 |
| field_delimiter | string | 否 | `\001` | 列分隔符 |

## 示例

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
