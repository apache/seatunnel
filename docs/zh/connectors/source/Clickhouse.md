import ChangeLog from '../changelog/connector-clickhouse.md';

# Clickhouse

> Clickhouse source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 核心特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列映射](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读](../../introduction/concepts/connector-v2-features.md)

> 支持查询SQL，可以实现投影效果。

## 描述

用于从Clickhouse读取数据。

## 支持的数据源信息

为了使用 Clickhouse 连接器，需要以下依赖项。它们可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源        | 支持的版本     | 依赖                                                                               |
|------------|--------------------|------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## 数据类型映射

| Clickhouse 数据类型                                                              | SeaTunnel 数据类型 |
|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            | STRING              |
| Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         | INT                 |
| UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond | BIGINT              |
| Float64                                                                                                                                       | DOUBLE              |
| Decimal                                                                                                                                       | DECIMAL             |
| Float32                                                                                                                                       | FLOAT               |
| Date                                                                                                                                          | DATE                |
| DateTime                                                                                                                                      | TIME                |
| Array                                                                                                                                         | ARRAY               |
| Map                                                                                                                                           | MAP                 |

## Source 选项

|       名称                   |   类型    | 是否必须 |  默认值         |                                                                                                                                                 描述                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-----------------------------------------------------------------------------------|
| host              | String | 是      | -                      | `ClickHouse` 集群地址, 格式是`host:port` , 允许多个`hosts`配置. 例如 `"host1:8123,host2:8123"` . |
| username          | String | 是      | -                      | `ClickHouse` user 用户账号.                                                           |
| password          | String | 是      | -                      | `ClickHouse` user 用户密码.                                                           |
| table_list        | Array  | 否       | -                      | 要读取的数据表列表，支持配置多表.                                                                 |
| clickhouse.config | Map    | 否       | -                      | 除了上述必须由 `clickhouse-jdbc` 指定的必填参数外，用户还可以指定多个可选参数，这些参数涵盖了 `clickhouse-jdbc` 提供的所有[参数](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration). |
| server_time_zone  | String | 否       | ZoneId.systemDefault() | 数据库服务中的会话时区。如果未设置，则使用ZoneId.systemDefault（）设置服务时区.                                                                                                                                                                                |
| common-options    |        | 否       | -                      | 源插件常用参数，详见 [源通用选项](../common-options/source-common-options.md).                                                                                                                                                                                          |

多表配置：

|       名称                   |   类型    | 是否必须 |  默认值         |                                                                                                                                                 描述                                                                                                                                                 |
|----------------|--------|------|------|--------------------------------------------------------------------------------------|
| table_path     | String | 否    | -    | 数据表的完整路径, 例如: `default.table`.                                                       |
| sql            | String | 否    | -    | 用于通过Clickhouse服务搜索数据的查询sql.                                                          |
| filter_query   | String | 否    | -    | 数据过滤条件. 格式为: "field = value", 例如 : filter_query = "id > 2 and type = 1"              |
| partition_list | Array  | 否    | -    | 指定分区列表过滤数据. 如果是分区表，该字段可以配置为过滤指定分区的数据。. 例如: partition_list = ["20250615", "20250616"] |
| split_size     | int    | 否    | Integer.MAX_VALUE | 在 `table_list` 内配置时，每个 SeaTunnel split 包含的 ClickHouse part 数量。只有把配置展开到 source 外层时才使用 `split.size`。最小值为 `1`，值越小，拆分越多，并行读取粒度越细。 |
| batch_size     | int    | 否    | 1024 | 从Clickhouse读取一次可以获得的最大数据行数。                                                          |

注意: 当此配置对应于单个表时，您可以将table_list中的配置项展平到外层。展开到外层时，分片大小参数使用 `split.size`，而不是 `split_size`。

## 并行读取

Clickhouse源连接器支持并行读取数据。

当仅指定`table_path`参数时，连接器根据从`system.parts`系统表中获取的数据表的part文件实现并行读取。

当仅指定`sql`参数时，连接器在集群的每个分片上基于本地表执行查询来实现并发读取。如果`sql`参数指定了一个分布式表，则会根据分布式表引擎的集群名获取分片列表执行并发读取。如果`sql`指定了一个本地表，那么`host`参数配置的节点列表将被视作集群分片列表执行并发读取。

如果同时设置了`table_path`和`sql`参数，则将在sql模式下执行。推荐在指定`sql`参数时同时配置`table_path`参数以更好地识别表的元数据。

## Tips
当指定`table_path`参数时，如果不想读取整个表，可以指定`partition_list`或`filter_query`参数过滤指定条件或分区的数据。
* `partition_list`: 过滤指定分区的数据
* `filter_query`: 根据指定条件对数据进行过滤。它也可以和 `sql` 一起使用，SeaTunnel 会把它作为额外的 ClickHouse 侧过滤条件。
* `split.size`: 使用 `table_path` 读取时，控制每个 SeaTunnel split 包含多少个 ClickHouse part

`batch_size`参数可用于控制每次查询读取的数据量，以避免在读取大量数据时出现OOM异常。适当增加这个值将有助于提高读取过程的性能。

当读取单个表的数据时，建议使用`table_path`参数替代`sql`参数。

## 如何创建Clickhouse数据同步作业

### 单表配置
下面的示例演示了如何创建一个数据同步作业，该作业从Clickhouse读取数据并在本地客户端上打印数据

**案例1：基于part文件读取策略的并行读取**
```hocon
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    table_path = "default.table"
    server_time_zone = "UTC"
    partition_list = ["20250615", "20250616"]
    filter_query = "id > 2 and type = 1"
    split.size = 1
    batch_size = 1024
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

**案例2：基于SQL读取策略的并行读取**
> 注意：SQL模式下的并行读取方式目前仅支持单表和where条件查询
```hocon
env {
  job.mode = "BATCH"
  parallelism = 5
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    table_path = "default.table"
    server_time_zone = "UTC"
    sql = "select * from default.table where id > 2 and type = 1"
    batch_size = 1024
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

**案例3：针对复杂SQL场景的单并发读取**

当执行复杂SQL查询场景（例如带有join、group by、子查询等的查询）时，连接器将自动切换到单并发执行方式，即使配置了更高的并行度值。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "xxx"
    password = "xxx"
    server_time_zone = "UTC"
    sql = "select t1.id, t2.category from default.table1 t1 global join default.table2 t2 on t1.id = t2.id where t1.age > 18"
    batch_size = 1024
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# Console printing of the read Clickhouse data
sink {
  Console {
    parallelism = 1
  }
}
```

### 多表配置

当 `table_list` 包含多个条目时，SeaTunnel 会为每个上游表生成一个独立的数据流，下游连接器可以使用 `${table_name}`
占位符将每条数据流路由到对应的目标表。结合 Clickhouse 接收端的 `table = "${table_name}_multi_table_sink"` 等
配置，可以把每个上游表分别写入各自的目标表。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 3
}

source {
  Clickhouse {
    host = "localhost:8123"
    username = "default"
    password = ""
    table_list = [
      {
        table_path = "default.source_table"
        sql = "select * from source_table"
      },
      {
        table_path = "default.source_merge_tree_table"
        filter_query = "id < 47"
      }
    ]
    server_time_zone = "UTC"
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# 每个上游表写入一个独立的目标表
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "${table_name}_multi_table_sink"
    username = "default"
    password = ""
  }
}
```

## 常见问题

### 如何只读取 ClickHouse 表的部分分区或部分行？

优先使用 `partition_list` 按分区过滤，或使用 `filter_query` 在服务端下推谓词；这两种方式都能在大表上避免全表扫描。
当同时需要投影特定列时，可以把它们和 `sql` 一起配置；此时连接器会进入 SQL 模式，并结合分区/过滤提示一起执行用户查询。

### 什么时候使用 SQL 模式，什么时候使用查表模式？

需要 join、聚合、子查询或多表投影时使用 SQL 模式（设置了 `sql`）。只需按主键读取单张逻辑表并希望基于
`system.parts` 做并行切分时使用查表模式（仅设置 `table_path`）。对于必须在单分片上运行的 SQL，连接器会自动
降级为单并发，即使配置了更高的 `parallelism` 也不会真正并行。

### 如何调节吞吐和内存压力？

两个主要参数是 `split.size`（每个 SeaTunnel split 合并多少 ClickHouse part）和 `batch_size`（每次往返拉取
的行数）。在小 part 较多的场景调小 `split.size` 可以提高并行度；在 part 较大的场景调大 `batch_size` 可以摊薄
往返开销。`clickhouse.config` 这个 map 暴露了所有 `clickhouse-jdbc` 参数（例如 `socket_timeout`）供进一步调优。

### `table_list` 和单个 `table_path` 是什么关系？

当配置只对应一张逻辑表时，可以把表级配置直接展平到外层，并把参数写成 `split.size`（注意带点）。当 `table_list`
中有多张表时，每张表的选项都要写在各自的条目里，并在条目内继续使用 `split_size`。

## 变更日志

<ChangeLog />
