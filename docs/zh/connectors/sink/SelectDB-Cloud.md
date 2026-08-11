import ChangeLog from '../changelog/connector-selectdb-cloud.md';

# SelectDB Cloud

> SelectDB Cloud Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于将数据发送到 SelectDB Cloud。支持流式和批处理模式。

SelectDB Cloud 接收器连接器的内部实现是在批量缓存后上传数据，并提交 CopyInto SQL 以将数据加载到表中。

## 支持的数据源信息

:::提示

支持的版本

* 支持的 `SelectDB Cloud 版本 >= 2.2.x`

:::

## 接收器选项

|        名称        |  类型  | 是否必填 |        默认值         |                                                                                                                                                                    描述                                                                                                                                                                    |
|--------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| load-url           | String | 是       | -                      | `SelectDB Cloud` 仓库的 HTTP 地址，格式为 `warehouse_ip:http_port`，用于提交 stream-load 请求。                                                                                                                                                                                                                                                  |
| jdbc-url           | String | 是       | -                      | `SelectDB Cloud` 仓库的 JDBC 地址，格式为 `warehouse_ip:mysql_port`，用于元数据查询（如表结构发现）。                                                                                                                                                                                                                                                |
| cluster-name       | String | 是       | -                      | `SelectDB Cloud` 集群名称。                                                                                                                                                                                                                                                                                                                  |
| username           | String | 是       | -                      | `SelectDB Cloud` 用户名。                                                                                                                                                                                                                                                                                                                    |
| password           | String | 是       | -                      | `SelectDB Cloud` 用户密码。                                                                                                                                                                                                                                                                                                                  |
| table.identifier   | String | 是       | -                      | `SelectDB Cloud` 表的名称，格式为 `database.table`。表必须已经存在并且列定义兼容，连接器不会自动建表。                                                                                                                                                                                                                                          |
| selectdb.config    | Map    | 是       | -                      | 透传给 `Copy Into` 语句的 stream-load 参数，至少需要设置 `file.type`（`json` 或 `csv`）。常用的还有 `file.column_separator`、`file.line_delimiter`、`file.strip_outer_array`、`max_filter_ratio` 等。在 HOCON 中每个键都需要带 `selectdb.config.` 前缀。                                                |
| sink.enable-2pc    | bool   | 否       | true                   | 是否启用两阶段提交（2pc），默认为 `true` 以保证 Exactly-Once 语义。SelectDB 使用缓存文件加载数据，当数据量较大时缓存可能失效（默认过期时间 1 小时）。如果遇到大量数据写入丢失，请把 `sink.enable-2pc` 配置为 `false` 并接受 At-Least-Once 语义。                                                                  |
| sink.enable-delete | bool   | 否       | false                  | 是否启用删除功能。该选项要求 `SelectDB Cloud` 表启用批量删除功能，并且仅支持 Unique 模型。                                                                                                                                                                                                                                                  |
| sink.max-retries   | int    | 否       | 3                      | 写入数据库失败时的最大重试次数。                                                                                                                                                                                                                                                                                                            |
| sink.buffer-size   | int    | 否       | 10 * 1024 * 1024 (1MB) | 用于流式加载的数据缓存缓冲区大小（字节）。调大可以减少请求次数，但每个 writer 占用的内存也会增加。                                                                                                                                                                                                                                          |
| sink.buffer-count  | int    | 否       | 10000                  | 用于流式加载的数据缓存缓冲区行数。调大可以减少请求次数，但每个 writer 占用的内存也会增加。                                                                                                                                                                                                                                              |
| sink.label-prefix  | String | 否       | 随机 UUID              | 每个 stream-load 事务的唯一标签前缀。在需要确定性标签做回放或审计时使用，默认的 UUID 已经是每个 writer 唯一的，除非有明确需求否则不需要修改。                                                                                                                                                                                  |
| sink.flush.queue-size | int  | 否       | 1                      | 异步上传线程把缓存数据上传到对象存储时的队列长度。当 SeaTunnel worker 与 SelectDB 之间的网络较慢时调大该值。                                                                                                                                                                                                                          |
| common-options     | config | 否       | -                      | Sink 插件通用参数，请参考 [Sink 常用选项](../common-options/sink-common-options.md)。                                                                                                                                                                                                                                            |

## 数据类型映射

| SelectDB Cloud 数据类型 |           SeaTunnel 数据类型           |
|--------------------------|-----------------------------------------|
| BOOLEAN                  | BOOLEAN                                 |
| TINYINT                  | TINYINT                                 |
| SMALLINT                 | SMALLINT<br/>TINYINT                    |
| INT                      | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT                   | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT                 | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT                    | FLOAT                                   |
| DOUBLE                   | DOUBLE<br/>FLOAT                        |
| DECIMAL                  | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE                     | DATE                                    |
| DATETIME                 | TIMESTAMP                               |
| CHAR                     | STRING                                  |
| VARCHAR                  | STRING                                  |
| STRING                   | STRING                                  |
| ARRAY                    | ARRAY                                   |
| MAP                      | MAP                                     |
| JSON                     | STRING                                  |
| HLL                      | 尚未支持                                |
| BITMAP                   | 尚未支持                                |
| QUANTILE_STATE           | 尚未支持                                |
| STRUCT                   | 尚未支持                                |

#### 支持的导入数据格式

支持的格式包括 CSV 和 JSON。

`HLL`、`BITMAP`、`QUANTILE_STATE` 和 `STRUCT` 是 SelectDB 独有的类型，在 SeaTunnel 运行时 schema 中没有对应
类型。如果下游需要用到这些字段，请用 JDBC 通道而不是 stream-load 通道单独写入。

## CDC 行为说明

上游为 CDC 源（MySQL-CDC、PostgreSQL-CDC 等）时，SelectDB Cloud 接收器会根据行类型自动生成 `INSERT` /
`DELETE` 语句。要让 CDC 写入正常工作，下游表必须满足：

- 使用 **Unique** 模型，保证主键重复时能被去重。
- 启用 **批量删除**（`ALTER TABLE ... ENABLE BATCH DELETE`），让连接器能下发 `DELETE`。
- 在作业配置中把 `sink.enable-delete` 设置为 `true`。

上述条件缺一不可，否则 CDC 写入会失败或者静默丢弃删除。

## 任务示例

### 简单示例

> 以下示例描述了将多种数据类型写入 SelectDBCloud，用户需要在下游创建相应的表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}
```

### 使用 JSON 格式导入数据

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}

```

### 使用 CSV 格式导入数据

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "csv"
        file.column_separator = ","
        file.line_delimiter = "\n"
    }
  }
}
```

### 启用 Checkpoint 的 STREAMING 作业

`STREAMING` 模式下需要设置 `checkpoint.interval`，让 writer 能定期刷新缓冲区并提交 `Copy Into` 事务。
如果不开启 checkpoint，连接器会一直把行缓存在内存里，直到 `sink.buffer-size` 或 `sink.buffer-count` 触发
刷盘——对慢源来说这可能是非常大的一批数据。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Kafka {
    # ...
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.events"
    username = "admin"
    password = "******"
    sink.buffer-count = 50000
    sink.label-prefix = "seatunnel-events"
    selectdb.config {
      file.type = "json"
    }
  }
}
```

### 从 MySQL-CDC 接入

CDC 接入要求下游表使用 Unique 模型并启用批量删除。连接器会根据行类型生成 `INSERT` / `DELETE` 语句，
通过 `sink.enable-delete = true` 启用。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    # ...
    table-names = ["demo.orders"]
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.orders"
    username = "admin"
    password = "******"
    sink.enable-delete = true
    selectdb.config {
      file.type = "json"
      file.strip_outer_array = "false"
    }
  }
}
```

### 2PC 超时场景

`sink.enable-2pc` 默认 `true`，每批写入都会被包在一个事务里。SelectDB 事务默认过期时间是 1 小时，
批次非常大或网络很慢时可能超过这个时间，导致缓存数据失效，下游出现丢行。如果出现这种大批次场景，
请把 `sink.enable-2pc = false` 并接受 At-Least-Once 语义（配合 Unique 主键的下游表，重复行仍然会被正确
合并）。

```hocon
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.large_events"
    username = "admin"
    password = "******"
    sink.enable-2pc = false
    sink.buffer-count = 200000
    selectdb.config {
      file.type = "json"
    }
  }
}
```

## 变更日志

<ChangeLog />
