# Clickhouse

> Clickhouse 数据连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 核心特性

- [ ] [精准一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

> Clickhouse sink 插件通过实现幂等写入可以达到精准一次，需要配合 aggregating merge tree 支持重复数据删除的引擎。

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

## 输出选项

|                  名称                   |   类型    | 是否必须 |  默认值  |                                                                                        描述                                                                                        |
|---------------------------------------|---------|------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                                  | String  | Yes  | -     | `ClickHouse` 集群地址, 格式是`host:port` , 允许多个`hosts`配置. 例如 `"host1:8123,host2:8123"`.                                                                                                 |
| database                              | String  | Yes  | -     | `ClickHouse` 数据库名称.                                                                                                                                                              |
| table                                 | String  | Yes  | -     | 表名称.                                                                                                                                                                             |
| username                              | String  | Yes  | -     | `ClickHouse` 用户账号.                                                                                                                                                               |
| password                              | String  | Yes  | -     | `ClickHouse` 用户密码.                                                                                                                                                               |
| clickhouse.config                     | Map     | No   |       | 除了上述必须由 `clickhouse-jdbc` 指定的必填参数外，用户还可以指定多个可选参数，这些参数涵盖了 `clickhouse-jdbc` 提供的所有[参数](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration). |
| bulk_size                             | String  | No   | 20000 | 每次通过[Clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc) 写入的行数，即默认是20000.                                                                                            |
| split_mode                            | String  | No   | false | 此模式仅支持引擎为`Distributed`的 `clickhouse` 表。选项 `internal_replication` 应该是 `true` 。他们将在 seatunnel 中拆分分布式表数据，并直接对每个分片进行写入。分片权重定义为 `clickhouse` 将计算在内。                                   |
| sharding_key                          | String  | No   | -     | 使用 `split_mode` 时，将数据发送到哪个节点是个问题，默认为随机选择，但可以使用`sharding_key`参数来指定分片算法的字段。此选项仅在`split_mode`为 `true` 时有效.                                                                          |
| primary_key                           | String  | No   | -     | 标记`clickhouse`表中的主键列，并根据主键执行INSERT/UPDATE/DELETE到`clickhouse`表.                                                                                                                  |
| support_upsert                        | Boolean | No   | false | 支持按查询主键更新插入行.                                                                                                                                                                    |
| allow_experimental_lightweight_delete | Boolean | No   | false | 允许基于`MergeTree`表引擎实验性轻量级删除.                                                                                                                                                      |
| common-options                        |         | No   | -     | Sink插件查用参数,详见[Sink常用选项](../sink-common-options.md).                                                                                                                              |

## 如何创建一个clickhouse 同步任务

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

### 小提示

> 1.[SeaTunnel 部署文档](../../start-v2/locally/deployment.md). <br/>
> 2.需要在同步前提前创建要写入的表.<br/>
> 3.当写入 ClickHouse 表,无需设置其结构，因为连接器会在写入前向 ClickHouse 查询当前表的结构信息.<br/>

## Clickhouse 接收器配置

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

## 切分模式

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

## CDC(Change data capture) Sink

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

## CDC(Change data capture) for *MergeTree engine

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

