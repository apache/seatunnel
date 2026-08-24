import ChangeLog from '../changelog/connector-fluss.md';

# Fluss

> Fluss Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持 CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

Fluss Sink 用于在批处理或流处理作业中，将 SeaTunnel 数据写入已有的 Fluss 表。

如果目标 Fluss 表有主键，连接器会使用 upsert 写入方式：`INSERT` 和 `UPDATE_AFTER` 会写入或更新数据，`DELETE` 会删除数据，`UPDATE_BEFORE` 会被忽略。如果目标表没有主键，连接器会使用 append 写入方式：`INSERT` 和 `UPDATE_AFTER` 会追加数据，`DELETE` 和 `UPDATE_BEFORE` 会被忽略。

运行作业前，目标 Fluss database 和 table 必须已经存在。当前 Sink 不会自动创建 Fluss database 或 table。

目标表结构需要和上游 SeaTunnel 数据按字段顺序匹配，并且字段类型需要兼容。Sink 会按字段位置写入数据。

## 依赖

```xml
<dependency>
    <groupId>com.alibaba.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

## Sink 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
|---|---|---|---|---|
| bootstrap.servers | string | 是 | - | Fluss coordinator 地址，例如 `fluss-coordinator:9123`。 |
| database | string | 否 | 上游数据库名 | 目标 Fluss database。支持 `${database_name}`、`${schema_name}` 等占位符。 |
| table | string | 否 | 上游表名 | 目标 Fluss table。支持 `${table_name}`、`${schema_name}` 等占位符。 |
| client.config | map | 否 | - | 传递给 Fluss 连接的额外客户端参数。 |
| multi_table_sink_replica | int | 否 | 1 | 多表写入模式下的 Sink writer 副本数。 |
| common-options | - | 否 | - | Sink 通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。 |

### database

未配置 `database` 时，Sink 会使用输入表标识中的上游数据库名。

可以配置固定 database 名称：

```hocon
database = "target_db"
```

也可以使用占位符：

```hocon
database = "fluss_${database_name}"
database = "fluss_${schema_name}"
```

### table

未配置 `table` 时，Sink 会使用输入表标识中的上游表名。

可以配置固定 table 名称：

```hocon
table = "target_table"
```

也可以使用占位符：

```hocon
table = "fluss_${table_name}"
table = "fluss_${schema_name}_${table_name}"
```

### client.config

使用 `client.config` 传递额外 Fluss 客户端配置。

```hocon
client.config = {
  request.timeout = "30s"
}
```

支持的配置键以 Fluss 客户端文档为准。

## 数据类型映射

| SeaTunnel 数据类型 | Fluss 数据类型 |
|---|---|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| BYTES | BYTES |
| STRING | STRING |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_TZ | TIMESTAMP_LTZ |

## 任务示例

### 单表写入

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 3
    schema {
      table = "test.table1"
      fields {
        id = int
        name = string
        score = double
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice", 98.5] }
      { kind = INSERT, fields = [2, "Bob", 88.0] }
      { kind = UPDATE_AFTER, fields = [2, "Bob", 90.0] }
    ]
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_${database_name}"
    table = "fluss_${table_name}"
  }
}
```

在这个示例中，上游表 `test.table1` 会写入 Fluss 表 `fluss_test.fluss_table1`。

### 多表写入

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        row.num = 2
        schema {
          table = "test.table1"
          fields {
            id = int
            name = string
          }
        }
      },
      {
        row.num = 2
        schema {
          table = "test.table2"
          fields {
            id = int
            name = string
          }
        }
      }
    ]
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_${database_name}"
    table = "fluss_${table_name}"
    multi_table_sink_replica = 1
  }
}
```

多表写入时，Sink 会为每个上游表分别解析目标表名。运行作业前，需要先创建对应的 Fluss database 和 table。

多表写入时，`database` 和 `table` 可以同时包含固定文本和占位符。例如 `database = "fluss_db_${database_name}"`、`table = "fluss_tb_${table_name}"` 会把上游表 `test2.table1` 写入 `fluss_db_test2.fluss_tb_table1`。

### 向主键 Fluss 表流式写入 upsert

当目标 Fluss 表带有主键时，Sink 会按每行记录的 `RowKind` 路由到对应的写入操作。
下面的示例从 CDC 源读取，并写入包含主键的 Fluss 表，会产生插入、更新和删除三类事件。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  MySQL-CDC {
    plugin_output = "orders_cdc"
    base-url = "jdbc:mysql://mysql:3306/shop"
    username = "cdc"
    password = "${secret}"
    database-names = ["shop"]
    table-names = ["shop.orders"]
  }
}

sink {
  Fluss {
    plugin_input = "orders_cdc"
    bootstrap.servers = "fluss-coordinator:9123"
    database = "shop"
    table = "orders"
  }
}
```

### 跨库复制 CDC 变更

可以使用占位符把一个 Fluss 命名空间下的 CDC 变更复制到另一个命名空间。
`schema_name` 占位符会解析为上游数据库名，`table_name` 解析为上游表名，
这样单个 Sink 配置就能扇出多张表。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Fluss {
    plugin_output = "fluss_cdc"
    bootstrap.servers = "fluss-coordinator:9123"
    database = "source_db"
    table = "source_table"
    start_mode = "earliest"
  }
}

sink {
  Fluss {
    plugin_input = "fluss_cdc"
    bootstrap.servers = "fluss-coordinator:9123"
    database = "sink_db_${schema_name}"
    table = "sink_${table_name}"
    multi_table_sink_replica = 2
  }
}
```

## Changelog

<ChangeLog />
