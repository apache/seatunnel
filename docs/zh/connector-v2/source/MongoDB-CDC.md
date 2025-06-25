import ChangeLog from '../changelog/connector-cdc-mongodb.md';

# MongoDB CDC

> MongoDB CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink<br/>

## 关键特性

- [ ] [批](../../concept/connector-v2-features.md)
- [x] [流](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户自定义split](../../concept/connector-v2-features.md)

## 描述

MongoDB CDC连接器允许从MongoDB数据库读取快照数据和增量数据。

## 支持的数据源信息

为了使用Mongodb CDC连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| 数据源 | 支持的版本 | Dependency                                                                                |
|------------|--------------------|-------------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-cdc-mongodb) |

## 可用性设置

1.MongoDB版本：MongoDB版本>=4.0。

2.集群部署：副本集或分片集群。

3.存储引擎：WiredTiger存储引擎。

4.权限：更改流和读取

```shell
use admin;
db.createRole(
    {
        role: "strole",
        privileges: [{
            resource: { db: "", collection: "" },
            actions: [
                "splitVector",
                "listDatabases",
                "listCollections",
                "collStats",
                "find",
                "changeStream" ]
        }],
        roles: [
            { role: 'read', db: 'config' }
        ]
    }
);

db.createUser(
  {
      user: 'stuser',
      pwd: 'stpw',
      roles: [
         { role: 'strole', db: 'admin' }
      ]
  }
);
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

| Name                               | 类型   | 必须 | 默认值 | 描述                                                                                    |
|------------------------------------|--------|----------|-------|---------------------------------------------------------------------------------------|
| hosts                              | String | 是      | -     | MongoDB服务器的主机名和端口对的逗号分隔列表。如 `localhost:27017,localhost:27018`                         |
| username                           | String | 否       | -     | 连接到MongoDB时要使用的数据库用户的名称。                                                              |
| password                           | String | 否       | -     | 连接到MongoDB时使用的密码。                                                                     |
| database                           | List   | 是      | -     | 要监视更改的数据库的名称。如果未设置，则将捕获所有数据库。该数据库还支持正则表达式，以监视与正则表达式匹配的多个数据库。例如db1、db2。                |
| collection                         | List   | 是      | -     | 要监视更改的数据库中集合的名称。如果未设置，则将捕获所有集合。该集合还支持正则表达式来监视与完全限定的集合标识符匹配的多个集合。例如db1.coll1、db2.coll2。 |
| schema                             |        | 否       | -     | 数据的结构，包括字段名和字段类型，使用单表cdc。                                                             |
| tables_configs                     |        | 否       | -     | 数据的结构，包括字段名和字段类型，使用多表cdc。                                                             |
| connection.options                 | String | 否       | -     | 与号分隔了MongoDB的连接选项。如。 `replicaSet=test&connectTimeoutMS=300000`.                       |
| batch.size                         | Long   | 否       | 1024  | 批量大小。                                                                                 |
| poll.max.batch.size                | Enum   | 否       | 1024  | 轮询新数据时，单个批中包含的更改流文档的最大数量。                                                             |
| poll.await.time.ms                 | Long   | 否       | 1000  | 在检查更改流上的新结果之前等待的时间量。                                                                  |
| heartbeat.interval.ms              | String | 否       | 0     | 发送心跳消息之间的时间长度（毫秒）。使用0禁用。                                                              |
| incremental.snapshot.chunk.size.mb | Long   | 否       | 64    | 增量快照的块大小（mb）。                                                                         |
| exactly_once                       | Boolean| 否       | false | 启用精确一次语义，若开启在大表快照阶段恢复时会有内存溢出风险。                                                       |
| common-options                     |        | 否       | -     | 源插件常用参数，请参考 [Source Common Options](../source-common-options.md)                      |

### 提示

> 1.如果集合更改速度较慢，强烈建议为heartbeat.interval.ms参数设置一个大于0的适当值。当我们从检查点或保存点恢复Seatunnel作业时，心跳事件可以向前推resumeToken以避免其过期。<br/>
> 2.MongoDB对单个文档的限制为16MB。变更文档包含其他信息，因此即使原始文档不超过15MB，变更文档也可能超过16MB的限制，从而导致变更流操作终止。<br/>
> 3.建议使用不可变分片键。在MongoDB中，分片键允许在启用事务后进行修改，但更改分片键可能会导致频繁的分片迁移，从而导致额外的性能开销。此外，修改分片键也可能导致更新查找功能失效，从而导致CDC（变更数据捕获）场景中的结果不一致。<br/>
> 4.“schema”和“tables_config”是互斥的，必须一次配置一个。

## 更新数据的流

[**更新流**](https://www.mongodb.com/docs/v5.0/changeStreams/) 是MongoDB 3.6为副本集和分片集群提供的一项新功能，允许应用程序访问实时数据更改，而不会出现尾随oplog的复杂性和风险。
应用程序可以使用更改流订阅单个集合、数据库或整个部署上的所有数据更改，并立即对其做出反应。

**查找更新操作的完整文档**是**更改流**提供的一项功能，它可以配置更改流以返回更新文档的最新多数提交版本。由于此功能，我们可以轻松收集最新的完整文档，并将更改日志转换为Changelog流。

更新流中删除事件捕获的数据格式：[delete envet](https://www.mongodb.com/docs/v5.0/reference/change-events/delete/)
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
      fields {
        "_id" : string,
        "name" : string,
        "description" : string,
        "weight" : string
      }
    }
  }
}

# 控制台打印读取的Mongodb数据
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
    user = "st_user"
    password = "seatunnel"

    generate_sink_sql = true
    # 您需要同时配置数据库和表
    database = mongodb_cdc
    table = products
    primary_keys = ["_id"]
  }
}
```

## 多表同步

以下示例演示了如何创建数据同步作业，该作业读取多个库表mongodb的cdc数据并将其打印到本地客户端：

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

# 控制台打印读取的Mongodb数据
sink {
  Console {
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