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

## Sink 选项

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
| schema_save_mode               | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | schema保存模式，请参考下面的`schema_save_mode`                                                                                                                    |
| data_save_mode                 | Enum    | no       | APPEND_DATA                  | 数据保存模式，请参考下面的`data_save_mode`。                                                                                                                         |
| save_mode_create_template      | string  | no       | see below                    | 见下文。                                                                                                                                                   |
| common-options                        |         | No   | -     | Sink插件查用参数,详见[Sink常用选项](../sink-common-options.md).                                                                                                                              |

### schema_save_mode[Enum]

在开启同步任务之前，针对现有的表结构选择不同的处理方案。
选项介绍：  
`RECREATE_SCHEMA` ：表不存在时创建，表保存时删除并重建。  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时会创建，表存在时跳过。  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：表不存在时会报错。  
`IGNORE` ：忽略对表的处理。

### data_save_mode[Enum]

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

