import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB 数据接收（Sink）连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [exactly-once 精准一次写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)（仅 Zeta 引擎）
- [x] [CDC（变更数据捕获）](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

**提示**

> 1. 如果希望使用 CDC 写入功能，建议启用 `upsert-enable` 配置项。
> 2. 启用 `transaction` 与 Zeta 定时刷新互斥，请在同一个作业中选择其中一种模式；混用会导致
>    定时刷新被静默关闭。

## 介绍

MongoDB Sink 连接器将 SeaTunnel 行记录写入 MongoDB 集合。每行记录都会被转换为 BSON 文档
发送到所配置的 `database` 和 `collection`。

连接器支持两种写入语义：

- **追加写入**：每行生成一条新文档。性能最好，但在失败重试场景下不幂等。
- **Upsert 写入**：当 `upsert-enable = true` 且配置了 `primary-key` 时，连接器会把主键作为
  MongoDB 的 `_id`（或复合 `_id`）并以 upsert 方式写入。结合 checkpoint 恢复机制，可以实现
  at-least-once + 幂等重试，这是把 exactly-once 落到 MongoDB 的标准做法。

缓存、重试以及可选的事务都可以通过下文的配置项进行调节。

## 支持的数据源信息

要使用 MongoDB 连接器，需要以下依赖。可以通过 `install-plugin.sh` 下载，也可以从 Maven
中央仓库获取。

| 数据源 | 支持版本 | 依赖 |
|---------|------------|---------|
| MongoDB | 通用版本 | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## 数据类型映射

下表展示了 SeaTunnel 数据类型到 MongoDB BSON 类型的映射关系。

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

> 1. 使用 SeaTunnel 将 `Date` 和 `Timestamp` 类型写入 MongoDB 时，结果都是 `Date` 类型，但
>    精度不同：SeaTunnel 的 `Date` 为秒级精度，`Timestamp` 为毫秒级精度。
> 2. 在 SeaTunnel 中使用 `DECIMAL` 类型时，最大精度不能超过 34 位。建议使用
>    `decimal(34, 18)` 以满足支持的精度与标度。

## Sink 参数说明

| 参数名称              | 类型     | 是否必填 | 默认值          | 描述 |
|-----------------------|----------|----------|----------------|------|
| uri                   | String   | 是       | -              | MongoDB 标准连接 URI，例如 `mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true`。更多示例请参考 [参数详解](#参数详解)。 |
| database              | String   | 是       | -              | 要写入的 MongoDB 数据库名称。配置多表同步时，可使用占位符 `${database_name}`，例如：`database = "${database_name}_test_database"`。 |
| collection            | String   | 是       | -              | 要写入的 MongoDB 集合名称。配置多表同步时，可使用 `${database_name}`、`${schema_name}`、`${table_name}` 等占位符，例如：`collection = "${database_name}_${schema_name}_${table_name}_check"`。 |
| buffer-flush.max-rows | Int      | 否       | 1000           | 每次批量写入请求的最大缓存行数。 |
| buffer-flush.interval | Long     | 否       | 30000          | 批量写入请求的最大时间间隔（毫秒）。 |
| retry.max             | Int      | 否       | 3              | 写入失败时的最大重试次数。 |
| retry.interval        | Long     | 否       | 1000           | 写入失败后的重试间隔（毫秒）。 |
| upsert-enable         | Boolean  | 否       | false          | 是否启用 upsert 模式写入。开启时需要同时配置 `primary-key`。 |
| primary-key           | List     | 否       | -              | 用于 upsert 或更新的主键，格式为 `["id","name",...]`。 |
| transaction           | Boolean  | 否       | false          | 是否在 MongoSink 中启用事务（需要 MongoDB 4.2+）。 |
| data_save_mode        | Enum     | 否       | APPEND_DATA    | MongoDB 集合的数据写入模式：`DROP_DATA` 表示写入前清空集合；`APPEND_DATA` 表示追加写入；`ERROR_WHEN_DATA_EXISTS` 表示集合已有数据时直接报错。 |
| common-options        | -        | 否       | -              | 通用 Sink 插件参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。 |

### 提示

> 1. MongoDB Sink 的连接器级数据刷新由三个参数共同控制：`buffer-flush.max-rows`、
>    `buffer-flush.interval` 和 `checkpoint.interval`。任一条件触发都会立刻刷写。
> 2. 兼容历史参数 `upsert-key` 作为 `primary-key` 的回退名。若已设置 `upsert-key`，请勿同时
>    设置 `primary-key`。
> 3. `transaction` 选项与下文 Zeta 定时刷新互斥，请二选一。

### Zeta 定时刷新

该引擎级能力仅由 Zeta 支持，Spark 和 Flink 不会注入 `FlushSignal` 记录。在 Zeta 中可以在
`env` 块配置 `sink.flush.interval`，使未达到 `buffer-flush.max-rows` 的待处理 bulk 请求也能
定时刷写出去。和 `buffer-flush.interval` 不同，引擎定时器不依赖新记录到达即可触发检查。

定时刷新仅在 `transaction = false` 时启用。MongoDB 事务模式通过 checkpoint 提交，因此会禁用
定时刷新以保持事务边界。初始定时刷新实现提供至少一次语义，不提供基于 2PC 的精确一次语义。
启用 upsert 并使用确定性主键可使重试具备幂等性。

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 5000
}

sink {
  MongoDB {
    uri = "mongodb://127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 10000
    transaction = false
  }
}
```

## 如何创建 MongoDB 数据同步任务

下面示例展示了一个将随机生成的数据写入 MongoDB 的任务：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 1000
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
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test"
    collection = "test"
  }
}
```

### 多表写入

当上游记录携带表元数据时，`database` 和 `collection` 可以使用占位符。常用占位符包括
`${database_name}`、`${schema_name}` 和 `${table_name}`。

```hocon
source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "testDatabase1.testSchema1.testTable1"
          fields {
            id = int
            value = string
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, "NEW"]
          }
        ]
      },
      {
        schema = {
          table = "testDatabase2.testSchema2.testTable2"
          fields {
            id = int
            amount = "decimal(16, 1)"
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, 6.3]
          }
        ]
      }
    ]
  }
}

sink {
  MongoDB {
    uri = "mongodb://127.0.0.1:27017/test_db?retryWrites=true"
    database = "test_db"
    collection = "${database_name}_${schema_name}_${table_name}_check"
  }
}
```

## 参数详解

### MongoDB 数据库连接 URI 示例

无认证的单节点连接：

```bash
mongodb://127.0.0.1:27017/mydb
```

副本集连接：

```bash
mongodb://127.0.0.1:27017/mydb?replicaSet=xxx
```

带认证的副本集连接：

```bash
mongodb://admin:password@127.0.0.1:27017/mydb?replicaSet=xxx&authSource=admin
```

多节点副本集连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb?replicaSet=xxx
```

分片集群连接（通过一个 `mongos` 路由）：

```bash
mongodb://mongos1.example.com:27017,mongos2.example.com:27017,mongos3.example.com:27017/mydb
```

多个 mongos 节点连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

> 注意：URI 中的用户名与密码在拼接前必须进行 URL 编码。

### Buffer Flush 示例

```hocon
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
事务意味着加锁、节点协调、额外往返和性能损耗。设计系统时应遵循的原则是：**能不用事务就
不要用事务**。合理的系统设计可以在大多数情况下避免对事务的依赖。

### 幂等写入（Idempotent Writes）

通过定义明确的主键并启用 `upsert` 模式，可以实现精准一次写入（exactly-once）语义。

当配置中定义了 `primary-key` 且启用了 `upsert-enable`，MongoDB Sink 将使用 Upsert 语义而非
普通 INSERT 语句。SeaTunnel 会将定义的主键作为 MongoDB 的复合主键，在 Upsert 模式下写入，
以确保幂等性。

若作业在运行过程中失败，SeaTunnel 会从上一个成功的 checkpoint 恢复并重新处理数据，这可能
导致重复数据。强烈建议启用 Upsert 模式，以避免主键冲突或重复插入。

```hocon
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    upsert-enable = true
    primary-key = ["name", "status"]
  }
}
```

## 更新日志

<ChangeLog />