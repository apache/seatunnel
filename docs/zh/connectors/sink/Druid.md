import ChangeLog from '../changelog/connector-druid.md';

# Druid

> Druid 接收器连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 Druid 索引任务 API 将数据写入 Apache Druid。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

| SeaTunnel 数据类型 | Druid 数据类型 |
|----------------|-----------------|
| TINYINT        | LONG            |
| SMALLINT       | LONG            |
| INT            | LONG            |
| BIGINT         | LONG            |
| FLOAT          | FLOAT           |
| DOUBLE         | DOUBLE          |
| DECIMAL        | DOUBLE          |
| STRING         | STRING          |
| BOOLEAN        | STRING          |
| TIMESTAMP      | STRING          |

## 选项

| 名称           | 类型   | 必需 | 默认值 | 说明 |
|----------------|--------|------|--------|------|
| coordinatorUrl | string | 是   | -      | Druid 协调器或路由节点的主机和端口。 |
| datasource     | string | 是   | -      | Druid datasource 名称，支持 `${table_name}` 这类占位符。 |
| batchSize      | int    | 否   | 10000  | 缓存多少行后提交一次索引任务。 |
| common-options |        | 否   | -      | Sink 通用参数。 |

### coordinatorUrl [string]

Druid 协调器或路由节点的主机和端口，例如 `router:8888`。

SeaTunnel 会向 `http://{coordinatorUrl}/druid/indexer/v1/task` 提交索引任务，所以这里只需要填写主机和端口，不要带协议和 API 路径。

### datasource [string]

要写入的 Druid datasource 名称。

当上游有多张表时，可以使用 `${table_name}` 这类占位符，把每张上游表写入不同的 Druid datasource。

### batchSize [int]

SeaTunnel 缓存多少行之后向 Druid 提交一次索引任务。默认值为 `10000`。

写入器关闭时，SeaTunnel 也会把剩余缓存数据提交到 Druid。

批量写入量较大时，可以适当调大该值，减少提交给 Druid 的索引任务数量。如果希望数据更早提交到 Druid，可以适当调小该值，但这会产生更多索引任务。

### common options

Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。

多表写入时，可以配合通用参数里的 `multi_table_sink_replica` 使用。

## 写入行为

连接器会把 SeaTunnel 每一行数据转换成内联 CSV 数据，然后作为 Druid 原生批量索引任务提交。

Druid 写入需要主时间列。连接器会自动追加一个名为 `timestamp` 的处理时间列给 Druid 使用；上游的 `TIMESTAMP` 字段会按上面的类型映射写成字符串维度。

当缓存行数达到 `batchSize` 时会触发一次写入；写入器关闭时，也会把剩余缓存数据提交到 Druid。当前没有按时间间隔定期刷新的配置。

该 Sink 适合追加式批量写入，不会把 CDC 的更新/删除行自动转换成 Druid 的 upsert 或 delete 操作。

仅支持 [数据类型映射](#数据类型映射) 中列出的 SeaTunnel 类型，其他类型会在写入规划阶段报错。

由于连接器会把数据以内联 CSV 的形式提交给 Druid，字符串字段中不建议直接包含英文逗号或换行符；如有这类内容，建议在进入 Druid Sink 前先做清洗或替换。

## 示例

### 写入单表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        c_boolean = boolean
        c_timestamp = timestamp
        c_string = string
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [true, "2020-02-02T02:02:02", "NEW", 1, 2, 3, 4, 4.3, 5.3, 6.3]
      },
      {
        kind = INSERT
        fields = [false, "2012-12-21T12:34:56", "AAA", 1, 1, 333, 323232, 3.1, 9.33333, 99999.99999999]
      }
    ]
  }
}

sink {
  Druid {
    coordinatorUrl = "router:8888"
    datasource = "testDataSource"
    batchSize = 10000
  }
}
```

### 写入多表

使用 `${table_name}` 可以把每张上游表写入同名的 Druid datasource。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "druid_sink_1"
          fields {
            id = int
            val_bool = boolean
            val_tinyint = tinyint
            val_smallint = smallint
            val_int = int
            val_bigint = bigint
            val_float = float
            val_double = double
            val_decimal = "decimal(16, 1)"
            val_string = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "druid_sink_2"
          fields {
            id = int
            val_bool = boolean
            val_tinyint = tinyint
            val_smallint = smallint
            val_int = int
            val_bigint = bigint
            val_float = float
            val_double = double
            val_decimal = "decimal(16, 1)"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, 1, 2, 3, 4, 4.3, 5.3, 6.3]
          }
        ]
      }
    ]
  }
}

sink {
  Druid {
    coordinatorUrl = "router:8888"
    datasource = "${table_name}"
  }
}
```

## 常见问题

### Druid Sink 是否支持 CDC？

不支持。该连接器面向追加式批量写入场景，CDC 的 update/delete 行不会被解释为 Druid 的 upsert 或 delete。

### Druid Sink 支持哪些 SeaTunnel 数据类型？

仅支持[数据类型映射](#数据类型映射)中列出的类型，其他类型在写入规划阶段就会失败。复杂嵌套类型请在上游 transform 中扁平化后再交给 Druid Sink。

### 连接器如何刷新数据？

行先在内存中缓冲，达到 `batchSize` 时作为一个 native batch indexing 任务提交给 Druid；writer 关闭时也会刷新剩余行。该连接器没有基于时间的周期刷新选项，需要结合 `batchSize` 与 writer 关闭时机来控制端到端延迟。

### 如何把多张上游表写到多个 Druid datasource？

设置 `datasource = "${table_name}"`，SeaTunnel 会把每张上游表写到同名 Druid datasource。如需针对每张表进行并行度微调，可叠加通用 sink 选项（如 `multi_table_sink_replica`）。

## 变更日志

<ChangeLog />

