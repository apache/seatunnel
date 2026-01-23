import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance sink 连接器

## 支持的引擎

> Spark（不支持 Spark 3.4 以下版本，参考 https://lance.org/integrations/spark/install/#scala）<br/>
> Flink（暂不支持，参考 https://github.com/lance-format/lance-flink）<br/>
> SeaTunnel Zeta<br/>

## 描述

Lance 格式的 Sink 连接器。支持创建和写入数据集、Lance 命名空间管理 schema 和版本。

## 主要特性

- [] [精确一次语义](../../introduction/concepts/connector-v2-features.md)

## 依赖

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

## Sink 配置项

| Name            | Type   | Required | Default | Description                                             |
|-----------------|--------|----------|---------|---------------------------------------------------------|
| dataset_path    | string | yes      | /tmp    | Lance sink 连接的数据集路径 .                                   |
| namespace_type  | string | yes      | dir     | Lance 数据集的命名空间类型，目前仅支持 DirectoryNamespace，类型默认为 "dir"   |
| table           | string | yes      | test    | Lance 数据集的名称，如果未设置，数据集名称默认为 test                        |
| namespace_id    | string | no       | -       | Lance 命名空间的 ID。请参考 https://lance.org/format/namespace/  |


## 数据类型映射

Lance 的数据类型依赖于 Arrow 数据类型系统

| Seatunnel数据类型 | Lance 数据类型   |
|---------------|--------------|
| BOOLEAN       | bool/boolean |
| TINYINT       | int8         |
| SMALLINT      | int16        |
| INT           | int32        |
| BIGINT        | int64        |
| FLOAT         | float16      |
| DOUBLE        | float32      |
| BYTES         | binary       |
| DATE          | DATE         |
| TIME          | TIME         |
| TIMESTAMP     | TIMESTAMP    |
| STRING        | string/utf8  |


## 任务示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"

  # 可以在这里设置 Spark 配置
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}

transform {
}

sink {
  Lance {
    dataset_path = "/tmp/seatunnel_mnt/lanceTest/lance_sink_table"
    namespace_type = "dir"
    namespace_id = "root"
    table = "lance_sink_table"
  }
}

```

## 更新日志

<ChangeLog />

