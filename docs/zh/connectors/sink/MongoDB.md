import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB 数据接收（Sink）连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [exactly-once 精准一次写入](../../concept/connector-v2-features.md)
- [x] [CDC（变更数据捕获）](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

**提示**

> 1. 如果希望使用 CDC 写入功能，建议启用 `upsert-enable` 配置项。

## 介绍

MongoDB 连接器提供从 MongoDB 读取数据以及向 MongoDB 写入数据的能力。  
本文档将介绍如何配置 MongoDB 连接器，以便执行向 MongoDB 写入数据的任务。

## 支持的数据源信息

要使用 MongoDB 连接器，需要以下依赖。  
可通过 `install-plugin.sh` 下载，或从 Maven 中央仓库获取。

| 数据源 | 支持版本 | 依赖 |
|---------|------------|---------|
| MongoDB | 通用版本 | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## 数据类型映射

以下表格展示了 MongoDB BSON 类型与 SeaTunnel 数据类型之间的映射关系。

| SeaTunnel 数据类型 | MongoDB BSON 类型 |
|--------------------|-------------------|
| STRING             | ObjectId          |
| STRING             | String            |
| BOOLEAN            | Boolean           |
| BINARY             | Binary            |
| INTEGER            | Int32             |
| TINYINT            | Int32             |
| SMALLINT           | Int32             |
| BIGINT             | Int64             |
| DOUBLE             | Double            |
| FLOAT              | Double            |
| DECIMAL            | Decimal128        |
| Date               | Date              |
| Timestamp          | Timestamp / Date  |
| ROW                | Object            |
| ARRAY              | Array             |

**提示**

> 1. 使用 SeaTunnel 将 `Date` 和 `Timestamp` 类型写入 MongoDB 时，MongoDB 中都会生成 `Date` 类型字段，但精度不同：SeaTunnel 的 `Date` 类型精度为秒，`Timestamp` 类型精度为毫秒。<br/>
> 2. 当使用 `DECIMAL` 类型时，最大精度不能超过 34 位，也就是说应使用 `decimal(34, 18)`。

## Sink 参数说明

| 参数名称              | 类型     | 是否必填 | 默认值 | 说明 |
|-----------------------|----------|----------|--------|------|
| uri                   | String   | 是       | -      | MongoDB 标准连接 URI，例如：`mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true`。 |
| database              | String   | 是       | -      | 要读取或写入的 MongoDB 数据库名称。配置多表同步时，可使用占位符 `${database_name}`，例如：`database = "${database_name}_test_database"`。 |
| collection            | String   | 是       | -      | 要读取或写入的 MongoDB 集合名称。配置多表同步时，可使用 `${table_name}`、`${schema_name}` 等占位符，例如：`collection = "${database_name}_${schema_name}_${table_name}_check"`。 |
| buffer-flush.max-rows | String   | 否       | 1000   | 每次批量写入请求的最大缓存行数。 |
| buffer-flush.interval | String   | 否       | 30000  | 批量写入的最大时间间隔（毫秒）。 |
| retry.max             | String   | 否       | 3      | 写入失败时的最大重试次数。 |
| retry.interval        | Duration | 否       | 1000   | 写入失败后的重试间隔时间（毫秒）。 |
| upsert-enable         | Boolean  | 否       | false  | 是否启用 upsert 模式进行写入。 |
| primary-key           | List     | 否       | -      | 用于 upsert 或更新操作的主键，格式为 `["id","name",...]`。 |
| transaction           | Boolean  | 否       | false  | 是否在 MongoSink 中使用事务（需要 MongoDB 4.2+）。 |
| common-options        | -        | 否       | -      | 通用 Sink 插件参数，详见 [Sink Common Options](../sink-common-options.md)。 |
| data_save_mode        | String   | 否       | APPEND_DATA | 数据写入模式：<br/>- `DROP_DATA`: 插入数据前清空集合；<br/>- `APPEND_DATA`: 追加数据；<br/>- `ERROR_WHEN_DATA_EXISTS`: 如果集合已有数据则报错。 |

### 提示

> 1. MongoDB Sink 连接器的数据刷新逻辑由以下三个参数共同控制：`buffer-flush.max-rows`、`buffer-flush.interval` 和 `checkpoint.interval`。  
     > 任一条件满足时，都会触发数据刷写。<br/>
> 2. 兼容历史参数 `upsert-key`。若已设置 `upsert-key`，请勿同时设置 `primary-key`。

## 如何创建 MongoDB 数据同步任务

下面示例展示了一个将随机生成的数据写入 MongoDB 的数据同步任务：

```bash
# 设置作业的基本配置
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
  MongoDB {
    uri = mongodb://user:password@127.0.0.1:27017
    database = "test"
    collection = "test"
  }
}
```

## 参数详解

### MongoDB 数据库连接 URI 示例

无认证的单节点连接：

```bash
mongodb://127.0.0.0:27017/mydb
```

副本集连接：

```bash
mongodb://127.0.0.0:27017/mydb?replicaSet=xxx
```

带认证的副本集连接：

```bash
mongodb://admin:password@127.0.0.0:27017/mydb?replicaSet=xxx&authSource=admin
```

多节点副本集连接：

```bash
mongodb://127.0.0.1:27017,127.0.0.2:27017,127.0.0.3:27017/mydb?replicaSet=xxx
```

分片集群连接：

```bash
mongodb://127.0.0.0:27017/mydb
```

多个 mongos 节点连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

注意：URI 中的用户名与密码在拼接前必须进行 URL 编码。

### Buffer Flush 示例

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 2000
    buffer-flush.interval = 1000
  }
}
```

### 为什么不推荐频繁使用事务？

虽然 MongoDB 自 4.2 版本起已完全支持多文档事务，但这并不意味着所有场景都应使用。  
事务意味着加锁、节点协调、额外开销和性能损耗。  
设计系统时应遵循的原则是：**能不用事务就不要用事务**。  
合理的系统设计可以在大多数情况下避免对事务的依赖。

### 幂等写入（Idempotent Writes）

通过定义明确的主键并启用 `upsert` 模式，可以实现精准一次写入（exactly-once）语义。

当配置中定义了 `primary-key` 且启用了 `upsert-enable`，MongoDB Sink 将使用 Upsert 语义而非普通 INSERT 语句。  
SeaTunnel 会将定义的主键作为 MongoDB 的复合主键，在 Upsert 模式下进行写入，以确保幂等性。

若作业在运行过程中失败，SeaTunnel 会从上一个成功的 checkpoint 恢复并重新处理数据，这可能导致重复数据。  
强烈建议启用 Upsert 模式，以避免主键冲突或重复插入。

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    upsert-enable = true
    primary-key = ["name","status"]
  }
}
```

## 更新日志

<ChangeLog />
