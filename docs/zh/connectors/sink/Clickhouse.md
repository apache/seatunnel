import ChangeLog from '../changelog/connector-clickhouse.md';

# Clickhouse

> Clickhouse 数据连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 核心特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

> 当目标表引擎支持去重时，例如 `AggregatingMergeTree` 或 `ReplacingMergeTree`，Clickhouse Sink 可以通过幂等写入减少重复数据影响。这里未标记为精准一次，因为实际保证取决于目标表设计。
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)


## 描述

用于将数据写入 Clickhouse。

## 支持的数据源信息

为了使用 Clickhouse 连接器，需要以下依赖项。它们可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源        | 支持的版本     | 依赖                                                                                 |
|------------|-----------|------------------------------------------------------------------------------------|
| Clickhouse | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## 数据类型映射

| SeaTunnel 数据类型 |                                                                Clickhouse 数据类型                                                                |
|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| STRING         | String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            |
| INT            | Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         |
| BIGINT         | UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond |
| DOUBLE         | Float64                                                                                                                                       |
| DECIMAL        | Decimal                                                                                                                                       |
| FLOAT          | Float32                                                                                                                                       |
| DATE           | Date                                                                                                                                          |
| TIME           | DateTime                                                                                                                                      |
| ARRAY          | Array                                                                                                                                         |
| MAP            | Map                                                                                                                                           |

## Sink 选项

|                  名称                   |   类型    | 是否必须 |  默认值  |                                                                                        描述                                                                                        |
|---------------------------------------|---------|------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | String  | 是   | -     | `ClickHouse` 集群地址，格式为 `host:port`，支持配置多个 host，例如 `"host1:8123,host2:8123"`。 |
| database                              | String  | 是   | -     | `ClickHouse` 数据库名称。 |
| table                                 | String  | 是   | -     | 表名称。 |
| username                              | String  | 是   | -     | `ClickHouse` 用户账号。 |
| password                              | String  | 是   | -     | `ClickHouse` 用户密码。 |
| clickhouse.config                     | Map     | 否   | -     | 除了上述必填参数外，还可以指定 `clickhouse-jdbc` 支持的其他[参数](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration)。 |
| bulk_size                             | int     | 否   | 20000 | 每次通过 [Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) 写入的行数。 |
| split_mode                            | Boolean | 否   | false | 仅当目标 ClickHouse 表使用 `Distributed` 引擎且 `internal_replication` 为 `true` 时生效。SeaTunnel 会拆分分布式表写入，并直接写入各个分片。 |
| sharding_key                          | String  | 否   | -     | `split_mode=true` 时用于分片算法的字段。不配置时会随机选择目标分片。 |
| primary_key                           | String  | 否   | -     | 用于处理 INSERT/UPDATE/DELETE 变更数据的主键列。多列用英文逗号分隔，例如 `id,name`。 |
| support_upsert                        | Boolean | 否   | false | 是否按 `primary_key` 查询后再写入，从而实现类似 upsert 的写入效果。 |
| allow_experimental_lightweight_delete | Boolean | 否   | false | 允许 DELETE 变更数据在 `*MergeTree` 表引擎上使用 ClickHouse lightweight delete。 |
| schema_save_mode                      | Enum    | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST | 表结构保存模式，请参考下面的 `schema_save_mode`。 |
| data_save_mode                        | Enum    | 否   | APPEND_DATA | 数据保存模式，请参考下面的 `data_save_mode`。 |
| custom_sql                            | String  | 否   | -     | 当 `data_save_mode = CUSTOM_PROCESSING` 时必填。该 SQL 会在同步任务开始前执行。 |
| save_mode_create_template             | String  | 否   | 见下文 | 当表结构保存模式需要创建表时使用的建表模板。 |
| common-options                        |         | 否   | -     | Sink 插件通用参数，详见 [Sink 常用选项](../common-options/sink-common-options.md)。 |

### schema_save_mode [Enum]

在开启同步任务之前，针对现有的表结构选择不同的处理方案。
选项介绍：  
`RECREATE_SCHEMA` ：表不存在时创建，表保存时删除并重建。  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时会创建，表存在时跳过。  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：表不存在时会报错。  
`IGNORE` ：忽略对表的处理。

### data_save_mode [Enum]

在开启同步任务之前，针对目标端已有的数据选择不同的处理方案。
选项介绍：  
`DROP_DATA`： 保留数据库结构并删除数据。  
`APPEND_DATA`：保留数据库结构，保留数据。  
`CUSTOM_PROCESSING`：用户自定义处理。  
`ERROR_WHEN_DATA_EXISTS`：有数据时报错。

### save_mode_create_template

使用模板自动创建 Clickhouse 表，
会根据上游数据类型和schema类型创建相应的建表语句，
默认模板可以根据情况进行修改。

默认模板：
```sql
CREATE TABLE IF NOT EXISTS  `${database}`.`${table}` (
    ${rowtype_primary_key},
    ${rowtype_fields}
) ENGINE = MergeTree()
ORDER BY (${rowtype_primary_key})
PRIMARY KEY (${rowtype_primary_key})
SETTINGS
    index_granularity = 8192
COMMENT '${comment}';
```

如果模板中填写了自定义字段，例如添加 id 字段

```sql
CREATE TABLE IF NOT EXISTS  `${database}`.`${table}` (
    id,
    ${rowtype_fields}
) ENGINE = MergeTree()
    ORDER BY (${rowtype_primary_key})
    PRIMARY KEY (${rowtype_primary_key})
    SETTINGS
    index_granularity = 8192
    COMMENT '${comment}';
```

连接器会自动从上游获取对应类型完成填充，
并从“rowtype_fields”中删除 id 字段。 该方法可用于自定义字段类型和属性的修改。

可以使用以下占位符：

- database：用于获取上游schema中的数据库。
- table_name：用于获取上游schema中的表名。
- rowtype_fields：用于获取上游schema中的所有字段，自动映射到 Clickhouse 的字段描述。
- rowtype_primary_key：用于获取上游模式中的主键（可能是列表）。
- rowtype_unique_key：用于获取上游模式中的唯一键（可能是列表）。
- comment：用于获取上游模式中的表注释。

## 示例配置与案例

### 如何创建一个clickhouse 同步任务

以下示例演示如何创建将随机生成的数据写入Clickhouse数据库的数据同步作业。

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval  = 1000
}

source {
  FakeSource {
      row.num = 2
      bigint.min = 0
      bigint.max = 10000000
      split.num = 1
      split.read-interval = 300
      schema {
        fields {
          c_bigint = bigint
        }
      }
    }
}

sink {
  Clickhouse {
    host = "127.0.0.1:9092"
    database = "default"
    table = "test"
    username = "xxxxx"
    password = "xxxxx"
  }
}
```

> 小提示：
>
> 1.[SeaTunnel 部署文档](../../getting-started/locally/deployment.md). <br/>
> 2.需要在同步前提前创建要写入的表.<br/>
> 3.当写入 ClickHouse 表,无需设置其结构，因为连接器会在写入前向 ClickHouse 查询当前表的结构信息.<br/>

### Clickhouse 接收器配置

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    clickhouse.config = {
      max_rows_to_read = "100"
      read_overflow_mode = "throw"
    }
  }
}
```

### 切分模式

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # split mode options
    split_mode = true
    sharding_key = "age"
  }
}
```

### CDC(Change data capture) Sink

处理变更数据时，需要配置 `primary_key`，这样连接器才能把 `UPDATE` 和 `DELETE` 数据对应到目标行。需要写入前按主键查询时，再配置 `support_upsert=true`。

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # cdc options
    primary_key = "id"
    support_upsert = true
  }
}
```

### CDC(Change data capture) for *MergeTree engine

处理 CDC 更新/删除数据时，需要配置 `primary_key`。只有目标 `*MergeTree` 表可以使用 ClickHouse lightweight delete 时，才设置 `allow_experimental_lightweight_delete=true`。

```hocon
sink {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    table = "fake_all"
    username = "xxxxx"
    password = "xxxxx"
    
    # cdc options
    primary_key = "id"
    support_upsert = true
    allow_experimental_lightweight_delete = true
  }
}
```

### 多表写入案例

在ClickHouse中提前创建下面两张数据表：

```
create table if not exists `default`.multi_sink_table1(
     `c_string`          String,
     `c_boolean`         Boolean,
     `c_tinyint`         Int8,
     `c_smallint`        Int16,
     `c_int`             Int32,
     `c_bigint`          Int64,
     `c_float`           Float32,
     `c_double`          Float64,
     `c_decimal`         Decimal(30, 8),
     `c_date`            Date,
     `c_time`            DateTime64,
     `c_map`             Map(String, Int32),
     `c_array`           Array(Int32)
)engine=Memory
comment '''N''-N';

create table if not exists `default`.multi_sink_table2 as `default`.multi_sink_table1;
```

然后使用的配置参考如下：

```
env {
  parallelism = 1
  job.mode = "BATCH"
  job.name = "fake_to_clickhouse_with_multi_table"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "multi_sink_table1"
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
            c_date = date
            c_time = timestamp
            c_map = "map<string, int>"
            c_array = "array<int>"
          }
        }
        row.num = 100
      },
      {
        schema = {
          table = "multi_sink_table2"
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
            c_date = date
            c_time = timestamp
            c_map = "map<string, int>"
            c_array = "array<int>"
          }
        }
        row.num = 100
      }
    ]
    plugin_output = "multi_sink_table"
  }
}

sink {
  Clickhouse {
    plugin_input = "multi_sink_table"
    host = "clickhouse:8123"
    database = "default"
    table = "${table_name}"
    username = "default"
    password = ""
  }
}
```

提交作业并执行成功后，我们可以看到 ClickHouse 数据表 `multi_sink_table1` 和 `multi_sink_table2` 的数据量都为100.

## 常见问题

### ClickHouse Sink 支持自动建表吗？

支持。精确的模式和当前默认行为请以上面的 `schema_save_mode` 小节为准。如果需要自定义自动建表
DDL，请使用 `save_mode_create_template`，不要在 FAQ 中再引入一套独立的 DDL 参数说法。

### 如何调优批量写入性能？

优先围绕 `bulk_size` 调优。精确的参数名和当前默认值已在上方 option 表中定义，因此吞吐调优时应以
该表为准，而不是让 FAQ 再复制一份参数说明。

### 支持哪些 ClickHouse 数据类型？

SeaTunnel 可映射到 ClickHouse 的类型包括 `Int8/16/32/64`、`UInt8/16/32/64`、`Float32/64`、`Decimal`、`String`、`FixedString`、`Date`、`DateTime`、`Array`、`Map` 及其 `Nullable` 变体。复杂嵌套类型写入前可能需要通过 Transform 做转换。

### 为什么提示"Table doesn't exist"错误？

先看上面的 `schema_save_mode` 小节。如果你的环境允许缺表时自动创建，就沿用文档中定义好的
自动建表流程；如果你的环境要求目标表必须预先存在，则显式切到更严格的模式，并在任务启动前先建表。

## 变更日志

<ChangeLog />
