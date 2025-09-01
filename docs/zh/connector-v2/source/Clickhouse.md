import ChangeLog from '../changelog/connector-clickhouse.md';

# Clickhouse

> Clickhouse source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 核心特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列映射](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../concept/connector-v2-features.md)
- [x] [支持多表读](../../concept/connector-v2-features.md)

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
| table_list        | Array  | NO       | -                      | 要读取的数据表列表，支持配置多表.                                                                 |
| clickhouse.config | Map    | 否       | -                      | 除了上述必须由 `clickhouse-jdbc` 指定的必填参数外，用户还可以指定多个可选参数，这些参数涵盖了 `clickhouse-jdbc` 提供的所有[参数](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration). |
| server_time_zone  | String | 否       | ZoneId.systemDefault() | 数据库服务中的会话时区。如果未设置，则使用ZoneId.systemDefault（）设置服务时区.                                                                                                                                                                                |
| common-options    |        | 否       | -                      | 源插件常用参数，详见 [源通用选项](../source-common-options.md).                                                                                                                                                                                          |

多表配置：

|       名称                   |   类型    | 是否必须 |  默认值         |                                                                                                                                                 描述                                                                                                                                                 |
|----------------|--------|------|------|--------------------------------------------------------------------------------------|
| table_path     | String | 否    | -    | 数据表的完整路径, 例如: `default.table`.                                                       |
| sql            | String | 否    | -    | 用于通过Clickhouse服务搜索数据的查询sql.                                                          |
| filter_query   | String | 否    | -    | 数据过滤条件. 格式为: "field = value", 例如 : filter_query = "id > 2 and type = 1"              |
| partition_list | Array  | 否    | -    | 指定分区列表过滤数据. 如果是分区表，该字段可以配置为过滤指定分区的数据。. 例如: partition_list = ["20250615", "20250616"] |
| batch_size     | int    | 否    | 1024 | 从Clickhouse读取一次可以获得的最大数据行数。                                                          |

注意: 当此配置对应于单个表时，您可以将table_list中的配置项展平到外层。

## 并行读取

Clickhouse源连接器支持并行读取数据。

当仅指定`table_path`参数时，连接器根据从`system.parts`系统表中获取的数据表的part文件实现并行读取。

当仅指定`sql`参数时，连接器在集群的每个分片上基于本地表执行查询来实现并发读取。如果`sql`参数指定了一个分布式表，则会根据分布式表引擎的集群名获取分片列表执行并发读取。如果`sql`指定了一个本地表，那么`host`参数配置的节点列表将被视作集群分片列表执行并发读取。

如果同时设置了`table_path`和`sql`参数，则将在sql模式下执行。推荐在指定`sql`参数时同时配置`table_path`参数以更好地识别表的元数据。

## Tips
当指定`table_path`参数时，如果不想读取整个表，可以指定`partition_list`或`filter_query`参数过滤指定条件或分区的数据。
* `partition_list`: 过滤指定分区的数据
* `filter_query`: 根据指定条件对数据进行过滤

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
    table_list = [
      {
        table_path = "default.table1"
        sql = "select * from default.table1 where id > 2 and type = 1"
      },
      {
        table_path = "default.table2"
        sql = "select * from default.table2 where age > 18"
      }
    ]
    server_time_zone = "UTC"
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

## 变更日志

<ChangeLog />