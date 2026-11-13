import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance source 连接器

## 支持的引擎

> Spark 3.4 及以上版本<br/>
> Flink 暂不支持<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 描述

Lance source 从已有的 Lance 数据集中读取数据。它以批处理方式读取数据集的最新版本，每个 Lance fragment 对应一个 split，并根据数据集的 Arrow schema 推导 SeaTunnel 表结构。

当前连接器支持基于目录的 Lance namespace，包括本地文件系统路径和 `s3://` 等对象存储路径（通过 `lance.read.storage.options` 提供访问凭据）。

## 依赖

```xml
<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-core</artifactId>
    <version>0.33.0</version>
</dependency>

<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-namespace-core</artifactId>
    <version>0.0.14</version>
</dependency>
```

## Source 配置项

| Name                       | Type   | Required | Default     | Description                                                                                       |
|----------------------------|--------|----------|-------------|---------------------------------------------------------------------------------------------------|
| dataset_path               | string | no       | /test.lance | Lance dataset 路径。对于目录 namespace，即 Lance 数据集使用的路径。                                  |
| namespace_type             | string | no       | dir         | Lance namespace 类型。当前仅支持 `dir`。                                                            |
| namespace_id               | string | no       | ""          | Lance namespace ID。                                                                              |
| namespace_ids              | list   | no       | []          | Lance namespace 路径段。                                                                           |
| root_namespace_path        | string | no       | /tmp        | Lance namespace 使用的根路径。                                                                     |
| table                      | string | no       | test        | Lance 表名。设置后会覆盖从 dataset 路径推导出的表名。                                                 |
| lance.read.storage.options | map    | no       | {}          | 打开数据集时使用的存储选项，例如 `aws_endpoint_url`、`aws_access_key_id`、`aws_secret_access_key`、`allow_http`。 |

### dataset_path

要读取的 Lance 数据集路径，可以是本地目录路径，也可以是 `s3://bucket/path/to/dataset` 这样的对象存储路径。

如果指定，该值不能为空或全为空白字符。

### namespace_type

Lance namespace 类型。当前连接器支持 `dir`。

如果指定，该值不能为空或全为空白字符。

### lance.read.storage.options

打开数据集时传给 Lance 读取器的存储选项，key 与 sink 的 `lance.write.storage.options` 相同。针对 MinIO 或其他 S3 兼容对象存储：

```hocon
"lance.read.storage.options" = {
  aws_endpoint_url = "http://minio:9000"
  aws_access_key_id = "admin"
  aws_secret_access_key = "password"
  allow_http = "true"
  aws_default_region = "us-east-1"
}
```

## 数据类型映射

Lance 使用 Apache Arrow 类型系统。source 根据数据集的 Arrow schema 推导 SeaTunnel 表结构：

| Lance（Arrow）数据类型 | SeaTunnel 数据类型 |
|-----------------------|-------------------|
| int（32 位）          | int               |
| int（64 位）          | bigint            |
| float                 | float             |
| double                | double            |
| utf8 / largeutf8      | string            |
| bool                  | boolean           |
| date                  | date              |
| time                 | time              |
| timestamp             | timestamp          |
| decimal               | decimal           |
| binary / largebinary  | bytes             |
| null                  | null              |
| list                  | array             |

struct、map 等复杂 Arrow 类型暂不支持，遇到时会直接报错，错误信息会指明出错的列。定长二进制列（Lance 用作向量的 Arrow 类型）同样会被拒绝：SeaTunnel 没有对应的定宽向量类型，因此连接器会快速报错，而不是将其强转为 bytes。Lance blob 列以普通 binary 形式呈现，按 `bytes` 读取（见上表）；其余 blob 形态同样走快速拒绝路径。

首版存在以下限制：数据集始终以其最新版本打开（不支持版本或快照选择）；每个 split 扫描其 fragment 的全部列（不支持列投影）；不支持过滤下推。列投影、版本选择与过滤下推将作为后续改进。

## 任务示例

### 读取 Lance 数据到控制台

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lance {
    dataset_path = "/tmp/seatunnel_practice/users"
    namespace_type = "dir"
    table = "users"
  }
}

sink {
  Console {}
}
```

### 从 MinIO（S3）读取 Lance 数据

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Lance {
    dataset_path = "s3://lance/seatunnel_demo/users"
    namespace_type = "dir"
    table = "users"
    "lance.read.storage.options" = {
      aws_endpoint_url = "http://127.0.0.1:50073"
      aws_access_key_id = "lanceadmin"
      aws_secret_access_key = "lanceX9k2mQ7w4Tz8b"
      allow_http = "true"
      aws_default_region = "us-east-1"
    }
  }
}

sink {
  Console {}
}
```

## 更新日志

<ChangeLog />
