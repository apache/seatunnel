import ChangeLog from '../changelog/connector-cdc-mongodb.md';

# MongoDB CDC

> MongoDB CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink<br/>

## 关键特性

- [ ] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

MongoDB CDC连接器允许从MongoDB数据库读取快照数据和增量数据。

## 支持的数据源信息

为了使用Mongodb CDC连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| 数据源 | 支持的版本 | 依赖                                                                                  |
|--------|------------|---------------------------------------------------------------------------------------|
| MongoDB | universal | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cdc-mongodb) |

## 可用性设置

1. MongoDB 版本：MongoDB 版本 >= 4.0。

2. 集群部署：副本集或分片集群。

3. 存储引擎：WiredTiger 存储引擎。

4. 权限：`changeStream` 和 `read`。

```javascript
// 1) 切换到目标数据库
use <DB_NAME>

// 2) 创建角色（CDC 场景常用权限）
db.createRole({
  role: "<ROLE_NAME>",
  privileges: [
    {
      resource: { db: "<DB_NAME>", collection: "" },
      actions: [
        "collStats",
        "splitVector",
        "listDatabases",
        "find",
        "listCollections",
        "changeStream"
      ]
    }
  ],
  roles: []
})

// 3) 创建用户，并绑定 read + 自定义角色
db.createUser({
  user: "<USER_NAME>",
  pwd: "<PASSWORD>",
  roles: [
    { role: "read", db: "<DB_NAME>" },
    { role: "<ROLE_NAME>", db: "<DB_NAME>" }
  ]
})

// 4) 为用户追加授予角色（用户已存在或需要补授权时使用）
db.grantRolesToUser("<USER_NAME>", ["<ROLE_NAME>"])
```

## 数据类型映射

下表列出了从MongoDB BSON类型到Seatunnel数据类型的字段数据类型映射。

| MongoDB BSON Type | SeaTunnel 数据类型 |
|-------------------|---------------------|
| ObjectId          | STRING              |
| String            | STRING              |
| Boolean           | BOOLEAN             |
| Binary            | BINARY              |
| Int32             | INTEGER             |
| Int64             | BIGINT              |
| Double            | DOUBLE              |
| Decimal128        | DECIMAL             |
| Date              | DATE                |
| Timestamp         | TIMESTAMP           |
| Object            | ROW                 |
| Array             | ARRAY               |

对于MongoDB中的特定类型，我们使用扩展JSON格式将其映射到Seatunnel STRING类型。

| MongoDB BSON type |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**提示**

> 1.在SeaTunnel中使用DECIMAL类型时，请注意最大范围不能超过34位数字，这意味着您应该使用DECIMAL(34,18)。<br/>

## 源配置项

| 名称                               | 类型    | 必填 | 默认值  | 描述                                                                                                                       |
|------------------------------------|---------|------|---------|----------------------------------------------------------------------------------------------------------------------------|
| hosts                              | String  | 是   | -       | MongoDB 服务器的主机名和端口列表，也可以是使用 `mongodb://` 或 `mongodb+srv://` 的标准 MongoDB 连接 URI。例如：`localhost:27017,localhost:27018` 或 `mongodb+srv://cluster.example.net`。 |
| username                           | String  | 否   | -       | 连接 MongoDB 时使用的数据库用户名。仅在 MongoDB 开启认证时需要。                                                            |
| password                           | String  | 否   | -       | 连接 MongoDB 时使用的密码。仅在 MongoDB 开启认证时需要。                                                                    |
| database                           | List    | 是   | -       | 要监听的数据库名称，支持正则表达式，例如 `["inventory"]` 或 `["db.*"]`。                                                    |
| collection                         | List    | 是   | -       | 要监听的集合名称，建议使用完整的 `database.collection` 格式，例如 `["inventory.products", "inventory.orders"]`，支持正则表达式。 |
| schema                             | Config  | 否   | -       | 单集合的 Schema 配置，包含字段名、字段类型和可选主键。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| tables_configs                     | List    | 否   | -       | 多集合的 Schema 配置列表，每个元素包含一个 `schema` 块。`tables_configs` 的数量和顺序必须与 `collection` 列表一致。           |
| connection.options                 | String  | 否   | -       | 使用 `&` 分隔的 MongoDB 连接参数，例如 `replicaSet=test&connectTimeoutMS=300000`。                                           |
| batch.size                         | Integer | 否   | 1024    | 读取快照数据时的游标批大小。                                                                                                |
| poll.max.batch.size                | Integer | 否   | 1024    | 轮询新数据时，单个批次中包含的变更流文档最大数量。                                                                          |
| poll.await.time.ms                 | Integer | 否   | 1000    | 检查变更流新结果前等待的时间，单位为毫秒。                                                                                  |
| heartbeat.interval.ms              | Integer | 否   | 0       | 发送心跳消息的间隔，单位为毫秒。设置为 `0` 表示禁用心跳。                                                                   |
| incremental.snapshot.chunk.size.mb | Integer | 否   | 64      | 增量快照读取时的分片大小，单位为 MB。                                                                                       |
| startup.mode                       | Enum    | 否   | INITIAL | MongoDB CDC 的启动模式，可选值为 `initial`、`latest` 和 `timestamp`。详见下方[启动模式](#启动模式)。                         |
| startup.timestamp                  | Long    | 否   | -       | 从指定的毫秒级时间戳开始消费。仅在 `startup.mode` 为 `timestamp` 时使用。                                                   |
| stop.mode                          | Enum    | 否   | NEVER   | MongoDB CDC 的停止模式，可选值为 `never` 和 `timestamp`。详见下方[停止模式](#停止模式)。                                    |
| stop.timestamp                     | Long    | 否   | -       | 在该毫秒级时间戳对应的变更流位置停止。仅在 `stop.mode` 为 `timestamp` 时使用。                                               |
| exactly_once                       | Boolean | 否   | false   | 启用精确一次语义。开启后，大表快照阶段恢复时可能增加内存使用。                                                              |
| debezium                           | Config  | 否   | -       | 透传给内嵌 Debezium 引擎的配置。                                                                                            |
| common-options                     |         | 否   | -       | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md)。                                             |

### 启动模式

`startup.mode` 选项控制作业提交时连接器从哪里开始读取：

- `initial`（默认）：先读取所监视集合的快照，然后切换到变更流。
- `latest`：完全跳过快照，从最新的变更流位置开始，只捕获作业启动之后产生的变更。在该模式下，与快照相关的选项（如 `incremental.snapshot.chunk.size.mb`）将被忽略。
- `timestamp`：跳过快照，从 `startup.timestamp` 指定的位置开始读取变更流。

当作业从检查点或保存点恢复时，无论 `startup.mode` 为何值，都会从检查点记录的变更流位置继续消费，重启不会回退到重新执行快照。

例如，只消费作业启动之后产生的变更：

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "latest"
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

### 停止模式

`stop.mode` 选项控制连接器是持续运行，还是在指定的变更流位置完成作业：

- `never`（默认）：持续读取变更流。
- `timestamp`：读取到 `stop.timestamp` 对应的 MongoDB 变更流时间戳，排空源已产生的记录后完成作业。

MongoDB 变更流时间戳的精度为秒。`stop.timestamp` 使用毫秒级 epoch 时间戳配置，并转换为 MongoDB 的时间戳表示。停止位置之后的事件不会输出。

- 当启动和停止模式都使用时间戳时，停止位置必须晚于启动位置。位于同一秒内的不同毫秒值会转换为相同位置，因此会被拒绝。
- 时间戳停止模式会将 source 设为有界模式，所有 split 到达停止位置后，流式作业会结束。
- 使用 `startup.mode = initial` 时，初始快照会完整读取，停止时间戳只限制增量变更流阶段。
- 使用 `startup.mode = latest` 时，如果停止位置已经过去，增量阶段不会输出记录，有界 source 会直接结束。
- 从 checkpoint 或 savepoint 恢复后，恢复的 split 中保存的停止位置优先于重新提交配置中修改后的 `stop.timestamp`。
- 对于空闲的有界变更流，连接器每次轮询会检查一次 MongoDB 集群时间，使其在没有变更事件到达边界时也能结束。

例如，读取一个有界时间区间：

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "timestamp"
    startup.timestamp = 1785542400000
    stop.mode = "timestamp"
    stop.timestamp = 1785546000000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

### 提示

> 1.如果集合更改速度较慢，强烈建议为heartbeat.interval.ms参数设置一个大于0的适当值。当我们从检查点或保存点恢复Seatunnel作业时，心跳事件可以向前推resumeToken以避免其过期。<br/>
> 2.MongoDB对单个文档的限制为16MB。变更文档包含其他信息，因此即使原始文档不超过15MB，变更文档也可能超过16MB的限制，从而导致变更流操作终止。<br/>
> 3.建议使用不可变分片键。在MongoDB中，分片键允许在启用事务后进行修改，但更改分片键可能会导致频繁的分片迁移，从而导致额外的性能开销。此外，修改分片键也可能导致更新查找功能失效，从而导致CDC（变更数据捕获）场景中的结果不一致。<br/>
> 4. `schema` 和 `tables_configs` 互斥。单集合使用 `schema`，多集合使用 `tables_configs`。

## 更新数据的流

[**更新流**](https://www.mongodb.com/docs/v5.0/changeStreams/) 是MongoDB 3.6为副本集和分片集群提供的一项新功能，允许应用程序访问实时数据更改，而不会出现尾随oplog的复杂性和风险。
应用程序可以使用更改流订阅单个集合、数据库或整个部署上的所有数据更改，并立即对其做出反应。

**查找更新操作的完整文档**是**更改流**提供的一项功能，它可以配置更改流以返回更新文档的最新多数提交版本。由于此功能，我们可以轻松收集最新的完整文档，并将更改日志转换为Changelog流。

更新流中删除事件捕获的数据格式：[delete 事件](https://www.mongodb.com/docs/manual/reference/change-events/delete/)
```
{
   "_id": { <Resume Token> },
   "operationType": "delete",
   "clusterTime": <Timestamp>,
   "ns": {
      "db": "engineering",
      "coll": "users"
   },
   "documentKey": {
      "_id": ObjectId("599af247bb69cd89961c986d")
   }
}
```
由于在更新流游标向客户端发送删除事件时文档已不存在，因此省略了完整文档。

## 如何创建MongoDB CDC数据同步作业

### CDC数据打印到客户端

以下示例演示了如何创建数据同步作业，该作业从MongoDB读取cdc数据并将其打印到本地客户端：

```hocon
env {
  # 您可以在此处设置engine配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema = {
      table = "inventory.products"
      primaryKey {
        name = "id"
        columnNames = ["_id"]
      }
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

## CDC数据写入MysqlDB

以下示例演示了如何创建数据同步作业，该作业从MongoDB读取cdc数据并写入mysql数据库：

```hocon
env {
  # 您可以在此处设置engine配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    username = stuser
    password = stpw
    schema = {
      table = "inventory.products"
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"

    generate_sink_sql = true
    # 您需要同时配置数据库和表
    database = mongodb_cdc
    table = products
    primary_keys = ["_id"]
  }
}
```

### CDC数据写入另一个MongoDB

您也可以将CDC事件路由到另一个MongoDB集合作为sink。下面的示例将源端 `mongo0` 的 `inventory.products` 镜像写入目标集群 `mongo1`：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

sink {
  MongoDB {
    uri = "mongodb://mongo1:27017"
    database = "inventory"
    collection = "products_mirror"
  }
}
```

## 从指定时间戳启动

如果希望跳过快照，从某个已知的时间点继续读取change stream，可以将 `startup.mode` 设置为 `timestamp`，并通过 `startup.timestamp` 提供epoch毫秒时间戳。这在维护窗口之后重放积压变更、或为新的sink初始化时跳过历史写入时非常有用：

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    startup.mode = "timestamp"
    # 2026-08-01 00:00:00 UTC
    startup.timestamp = 1785542400000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## 使用SRV连接URI

对于MongoDB Atlas或任何暴露 `mongodb+srv://` 连接字符串的部署，可以将URI直接传给 `hosts` 选项。URI中携带的认证信息、副本集名称等参数会原样传递给驱动，无需再额外配置 `connection.options`：

```hocon
source {
  MongoDB-CDC {
    hosts = "mongodb+srv://cluster0.example.net"
    username = "stuser"
    password = "stpw"
    database = ["inventory"]
    collection = ["inventory.products"]
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## 心跳与Resume Token维护

如果某个集合长时间没有数据写入，change stream的resume token可能会过期（例如低流量集合）。将 `heartbeat.interval.ms` 设置为非零值，可以让连接器在没有数据时定期推进resume token，从而在checkpoint恢复后保持change stream有效：

```hocon
source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products"]
    # 当没有数据写入时，每30秒发送一次心跳
    heartbeat.interval.ms = 30000
    schema = {
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}
```

## 从多个MongoDB源读取

单个 SeaTunnel 任务在 `source { ... }` 内只支持一个 source 块，因此同时从多个 MongoDB 集群拉取 CDC 流的可行方式是每个源提交一个独立任务，并写入同一个 sink 表。每个任务保留各自的并行度、schema 和重启 token，最终由 sink 表按主键去重。

```hocon
# 任务 A：从集群 mongo0 读取 CDC，写入 inventory_a.products_a
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory_a"]
    collection = ["inventory_a.products_a"]
    username = superuser
    password = superpw
    schema = {
      fields {
        "_id": string,
        "name": string,
        "price": int
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

```hocon
# 任务 B：从集群 mongo1 读取 CDC，写入 inventory_b.products_b
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo1:27017"
    database = ["inventory_b"]
    collection = ["inventory_b.products_b"]
    username = superuser
    password = superpw
    schema = {
      fields {
        "_id": string,
        "name": string,
        "price": int
      }
    }
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

## 多表同步

以下示例演示了如何读取多个 MongoDB 集合的 CDC 数据，并将每个集合写入对应的 MySQL 表。Sink 表名使用 `${table_name}`，因此 `inventory.products` 和 `inventory.orders` 会分别写入自己的目标表。

```hocon
env {
  # 您可以在此处设置engine配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MongoDB-CDC {
    hosts = "mongo0:27017"
    database = ["inventory"]
    collection = ["inventory.products", "inventory.orders"]
    username = superuser
    password = superpw
    tables_configs = [
      {
        schema {
          table = "inventory.products"
          fields {
            "_id" : string,
            "name" : string,
            "description" : string,
            "weight" : string
          }
        }
      },
      {
        schema {
          table = "inventory.orders"
          fields {
            "_id" : string,
            "order_number" : int,
            "order_date" : string,
            "quantity" : int,
            "product_id" : string
          }
        }
      }
    ]
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mongodb_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user"
    password = "seatunnel"
    generate_sink_sql = true
    database = mongodb_cdc
    table = "${table_name}"
    primary_keys = ["_id"]
  }
}
```

## CDC 元数据字段

MongoDB CDC 会提供以下元数据字段，可配合 `Metadata` 转换使用：

| 字段 | 类型 | 说明 |
|------|------|------|
| database | STRING | 源数据库名称。 |
| table | STRING | 源集合名称。 |
| rowKind | STRING | 变更类型，例如 insert、update 或 delete。 |
| ts_ms | LONG | 源事件时间，单位为毫秒。 |
| delay | LONG | 事件时间和处理时间之间的延迟，单位为毫秒。 |

示例：

```hocon
transform {
  Metadata {
    metadata_fields {
      Database = database
      Table = table
      RowKind = rowKind
      EventTime = ts_ms
      Delay = delay
    }
  }
}
```

## 实时流数据格式

```shell
{
   _id : { <BSON Object> },        // Identifier of the open change stream, can be assigned to the 'resumeAfter' parameter for subsequent resumption of this change stream
   "operationType" : "<operation>",        // The type of change operation that occurred, such as: insert, delete, update, etc.
   "fullDocument" : { <document> },      // The full document data involved in the change operation. This field does not exist in delete operations
   "ns" : {   
      "db" : "<database>",         // The database where the change operation occurred
      "coll" : "<collection>"     // The collection where the change operation occurred
   },
   "to" : {   // These fields are displayed only when the operation type is 'rename'
      "db" : "<database>",         // The new database name after the change
      "coll" : "<collection>"     // The new collection name after the change
   },
   "source":{
        "ts_ms":"<timestamp>",     // The timestamp when the change operation occurred
        "table":"<collection>"     // The collection where the change operation occurred
        "db":"<database>",         // The database where the change operation occurred
        "snapshot":"false"         // Identify the current stage of data synchronization
    },
   "documentKey" : { "_id" : <value> },  // The _id field value of the document involved in the change operation
   "updateDescription" : {    // Description of the update operation
      "updatedFields" : { <document> },  // The fields and values that the update operation modified
      "removedFields" : [ "<field>", ... ]     // The fields and values that the update operation removed
   }
   "clusterTime" : <Timestamp>,     // The timestamp of the Oplog log entry corresponding to the change operation
   "txnNumber" : <NumberLong>,    // If the change operation is executed in a multi-document transaction, this field and value are displayed, representing the transaction number
   "lsid" : {          // Represents information related to the Session in which the transaction is located
      "id" : <UUID>,  
      "uid" : <BinData>
   }
}
```

## 修改日志

<ChangeLog />
