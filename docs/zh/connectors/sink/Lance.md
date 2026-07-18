import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance sink 连接器

## 支持的引擎

> Spark 3.4 及以上版本<br/>
> Flink 暂不支持<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

Lance sink 用于把 SeaTunnel 数据写入 Lance 数据集。它可以根据上游 SeaTunnel 表结构创建 Lance 表，并按配置的 Lance 写入模式创建或追加数据。

当前连接器支持基于目录的 Lance namespace。
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

## Sink 配置项

| 名称                               | 类型      | 是否必填 | 默认值         | 说明                                                 |
|------------------------------------|-----------|----------|----------------|------------------------------------------------------|
| dataset_path                       | string    | 否       | /test.lance    | Lance 数据集路径。目录 namespace 下通常是本地数据路径。 |
| namespace_type                     | string    | 否       | dir            | Lance namespace 类型。当前仅支持 `dir`。             |
| namespace_id                       | string    | 否       | ""             | Lance namespace ID。                                 |
| namespace_ids                      | list      | 否       | []             | 解析目标表 namespace 时使用的 namespace 路径片段。    |
| root_namespace_path                | string    | 否       | /tmp           | Lance namespace 的根路径。                           |
| table                              | string    | 否       | test           | 目标 Lance 表名。设置后会覆盖上游表名。               |
| lance.write.max-rows-per-file      | int       | 否       | 10             | 单个 Lance 文件最多写入的行数。                      |
| lance.write.max-rows-per-group     | int       | 否       | 20             | 单个 Lance row group 最多写入的行数。                |
| lance.write.max-bytes-per-file     | long      | 否       | 20480          | 单个 Lance 文件最多写入的字节数。                    |
| lance.write.mode                   | string    | 否       | CREATE         | Lance 写入模式，会传给 Lance `WriteParams.WriteMode`。 |
| lance.write.enable.stable.row.ids  | boolean   | 否       | true           | 写入 Lance 时是否启用稳定 row ID。                   |
| lance.write.storage.options        | map       | 否       | {}             | 传给 Lance 的额外存储参数。                          |
| multi_table_sink_replica           | int       | 否       | 1              | 多表写入时的 sink 并行副本数。                       |

### dataset_path

Lance 数据的目录或数据集路径。使用本地目录模式时，请确保 SeaTunnel 运行环境有权限创建并写入该路径。

### namespace_type

Lance namespace 类型。当前连接器支持 `dir`。

### namespace_id

目录 namespace 实现使用的 namespace 名称。本地目录模式下可以填写类似 `root` 的简单名称。

### namespace_ids

解析目标表 namespace 时使用的额外路径片段。如果直接写入根 namespace，可以保持为空。

### root_namespace_path

Lance namespace 的根目录。SeaTunnel 运行用户需要有权限在该目录下创建和写入文件。

### table

目标 Lance 表名。不设置时，如果上游存在表名，连接器会使用上游表名；该配置本身的默认值是 `test`。

### lance.write.mode

控制 Lance 的写入方式。默认值是 `CREATE`。该值需要是 Lance `WriteParams.WriteMode` 支持的值：`CREATE`、`APPEND` 或 `OVERWRITE`。

### lance.write.storage.options

以键值对形式传递额外的 Lance 存储参数。

示例：

```hocon
lance.write.storage.options = {
  key1 = "value1"
  key2 = "value2"
}
```

## 数据类型映射

Lance 使用 Apache Arrow 类型系统。sink 会根据上游 SeaTunnel 表结构创建 Lance schema。

| SeaTunnel 数据类型 | Lance / Arrow 数据类型 |
|--------------------|------------------------|
| BOOLEAN            | bool                   |
| TINYINT            | int32                  |
| SMALLINT           | int32                  |
| INT                | int32                  |
| BIGINT             | int32                  |
| FLOAT              | float32                |
| DOUBLE             | float64                |
| DECIMAL            | decimal128             |
| NULL               | null                   |
| BYTES              | binary                 |
| DATE               | date32                 |
| TIME               | time32                 |
| TIMESTAMP          | timestamp              |
| STRING             | utf8                   |
| ARRAY              | list                   |
| MAP                | map                    |

## 任务示例

### 写入 FakeSource 数据到 Lance

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
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
