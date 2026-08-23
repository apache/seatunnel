import ChangeLog from '../changelog/connector-file-bos.md';

# BosFile

> BOS 文件 Source 连接器

## 支持的引擎

> Spark
>
> Flink
>
> SeaTunnel Zeta

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] 文件格式
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## 描述

通过 BOS HDFS SDK 从百度智能云 BOS 读取文件数据。

## 依赖 Jar

| jar | 版本 | 下载 |
|-----|------|------|
| bos-hdfs-sdk | >= 1.0.4-community | [下载](https://sdk.bce.baidu.com/console-sdk/bos-hdfs-sdk-1.0.4-community.jar.zip) |

> 源码构建前需将 `bos-hdfs-sdk` 安装到本地 Maven 仓库，参见 `connector-file-bos/lib/README.md`。

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|------|------|------|--------|------|
| path | string | 是 | - | bucket 下的目录路径 |
| file_format_type | string | 是 | - | 文件格式：text、csv、json、parquet、orc |
| bucket | string | 是 | - | BOS bucket，例如 `bos://my-bucket` |
| access_key | string | 是 | - | BOS Access Key |
| secret_key | string | 是 | - | BOS Secret Key |
| endpoint | string | 是 | - | BOS Endpoint，例如 `http://bj.bcebos.com` |
| schema | config | 条件必填 | - | text/csv/json 读取时需要 |
| row_delimiter | string | 否 | `\n` | 文本行分隔符 |
| field_delimiter | string | 否 | `\001` | 文本列分隔符 |
| parse_partition_from_path | boolean | 否 | true | 从路径解析分区 |
| recursive_file_scan | boolean | 否 | true | 递归扫描子目录 |

## 示例

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
