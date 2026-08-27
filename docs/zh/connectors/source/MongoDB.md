import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)

## 描述

MongoDB 源连接器从 MongoDB 集合中读取文档，并把每个 BSON 文档转换为 SeaTunnel 行记录。
它同时支持**批**和**流**两种作业模式，并通过 `partition.split-key` 把集合按取值范围切分为多个
split，实现并行读取。

在不扫描整张集合的前提下，可以缩小读取范围并控制返回字段：

- 使用 `match.query` 过滤符合条件的文档。
- 使用 `match.projection` 控制结果中返回的字段。
- 使用 `flat.sync-string` 把整篇文档作为一条 JSON `STRING` 列读入，跳过固定 schema 的定义。

在流模式下，连接器读取已分配的 split，并通过 checkpoint 跟踪读取位置；任务重启后会从上次提交
的游标继续读取。

## 支持的数据源信息

要使用 MongoDB 连接器，需要以下依赖。可以通过 `install-plugin.sh` 或 Maven 中央仓库下载。

| 数据源 | 支持的版本 | 依赖                                                                                    |
|--------|------------|-----------------------------------------------------------------------------------------|
| MongoDB | 通用版本    | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb)   |

## 数据类型映射

下表列出了从 MongoDB BSON 类型到 SeaTunnel 数据类型的字段映射。

| MongoDB BSON 类型 | SeaTunnel 数据类型 |
|-------------------|-------------------|
| ObjectId          | STRING            |
| String            | STRING            |
| Boolean           | BOOLEAN           |
| Binary            | BINARY            |
| Int32             | INTEGER           |
| Int64             | BIGINT            |
| Double            | DOUBLE            |
| Decimal128        | DECIMAL           |
| Date              | Date              |
| Timestamp         | Timestamp         |
| Object            | ROW               |
| Array             | ARRAY             |

针对 MongoDB 中的特殊类型，连接器使用扩展 JSON（Extended JSON）格式映射到 SeaTunnel 的
`STRING` 类型。

| MongoDB BSON 类型 |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**提示**

> 1. 在 SeaTunnel 中使用 `DECIMAL` 类型时，最大精度不能超过 34 位。建议使用
>    `decimal(34, 18)` 以满足支持的精度与标度。

## 源配置项

| 参数名称              | 类型    | 是否必填 | 默认值            | 描述                                                                                                                                                                                                                                                                                          |
|-----------------------|---------|----------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                   | String  | 是       | -                  | MongoDB 标准连接 URI，例如 `mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true`。更多示例请参考 [参数说明](#参数说明)。                                                                                                                                            |
| database              | String  | 是       | -                  | 要读取的 MongoDB 数据库名称。                                                                                                                                                                                                                                                                  |
| collection            | String  | 是       | -                  | 要读取的 MongoDB 集合名称。                                                                                                                                                                                                                                                                    |
| schema                | Config  | 是       | -                  | MongoDB 的 BSON 与 SeaTunnel 数据结构的映射。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。                                                                                                                                                                       |
| match.query           | String  | 否       | -                  | 用于过滤读取文档的 MongoDB 查询表达式。兼容旧版参数名 `matchQuery`。                                                                                                                                                                                                                            |
| match.projection      | String  | 否       | -                  | 用于控制查询结果中包含字段的 MongoDB 投影表达式。                                                                                                                                                                                                                                              |
| partition.split-key   | String  | 否       | _id                | 用作 MongoDB 分片字段的列名，连接器会按该字段的取值范围切分集合。                                                                                                                                                                                                                              |
| partition.split-size  | Long    | 否       | 64 * 1024 * 1024   | 每个 MongoDB split 的大小。split 越小并行度越高，split 越大并行度越低。                                                                                                                                                                                                                          |
| cursor.no-timeout     | Boolean | 否       | true               | MongoDB 服务端默认会在游标空闲 10 分钟后关闭游标以回收内存。将此选项设置为 `true` 可让游标在长时间运行的批次中保持打开。如果应用持有批次超过 30 分钟，MongoDB 会将当前会话标记为过期并关闭。                                                                                                       |
| fetch.size            | Int     | 否       | 2048               | 每批从服务器获取的文档数。合理设置可以提升查询性能并降低一次性获取大量数据带来的内存压力。                                                                                                                                                                                                       |
| max.time-min          | Long    | 否       | 10                 | 每次 MongoDB 查询的最大执行时间（分钟）。超过该限制 MongoDB 将终止操作并返回错误。                                                                                                                                                                                                              |
| flat.sync-string      | Boolean | 否       | false              | 开启后，连接器会把整篇 MongoDB 文档映射到一个 SeaTunnel `STRING` 字段。此时 schema 只能声明一个字段，且该字段必须是 `STRING` 类型。                                                                                                                                                              |
| common-options        |         | 否       | -                  | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。                                                                                                                                                                                                                |

### 提示

> 1. `match.query` 与旧版参数名 `matchQuery` 等价，二者不能同时设置。
> 2. 使用 `partition.split-key` 时建议选择有索引的字段，能显著加快 split 边界扫描。
> 3. 当 `flat.sync-string = true` 时，schema 仅用于声明单个接收文档的 `STRING` 字段。

## 如何创建 MongoDB 数据同步作业

下面的示例从 MongoDB 读取数据并打印到本地客户端：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "source_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(34, 18)"
        c_timestamp = timestamp
        c_row = {
          c_map = "map<string, string>"
          c_array = "array<int>"
          c_string = string
          c_boolean = boolean
          c_int = int
          c_bigint = bigint
          c_double = double
          c_bytes = bytes
          c_date = date
          c_decimal = "decimal(34, 18)"
          c_timestamp = timestamp
        }
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

## 参数说明

### MongoDB 数据库连接 URI 示例

无认证的单节点连接：

```bash
mongodb://192.168.0.100:27017/mydb
```

副本集连接：

```bash
mongodb://192.168.0.100:27017/mydb?replicaSet=xxx
```

带认证的副本集连接：

```bash
mongodb://admin:password@192.168.0.100:27017/mydb?replicaSet=xxx&authSource=admin
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

> 注意：URI 中的用户名与密码在拼接到连接字符串前必须进行 URL 编码。

### 匹配查询扫描

在数据同步场景中，建议尽早使用 `match.query` 减少下游算子需要处理的文档数量，从而提升整体
性能。下面是一个简单的示例：

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "orders"
    match.query = "{status: \"A\"}"
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

下面是常见数据类型对应的 `match.query` 表达式：

```bash
# 布尔类型
"{c_boolean: true}"
# 字符串类型
"{c_string: \"OCzCj\"}"
# 整数类型
"{c_int: 2}"
# 日期类型
"{c_date: {\$date: \"2023-06-26T16:00:00.000Z\"}}"
# 浮点类型
"{c_double: {\$gte: 1.71763202185342e+308}}"
```

完整查询语法请参考 MongoDB 官方文档：
<https://www.mongodb.com/docs/manual/tutorial/query-documents>

### 投影扫描

MongoDB 中的 Projection 用来控制查询结果中返回哪些字段：在 `find()` 方法中通过第二个参数
传入一个投影对象，键表示字段，值 `1` 表示包含，`0` 表示排除。例如对于 `users` 集合：

```javascript
// 仅返回 `name` 字段，过滤掉 `email` 字段
db.users.find({}, { name: 1, email: 0 });
```

在数据同步场景中，尽早使用 Projection 可以减少下游算子需要处理的字段数量。下面是 SeaTunnel
中使用投影的简单示例：

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    match.projection = "{ name: 1, email: 0 }"
    schema = {
      fields {
        name = string
      }
    }
  }
}
```

### 分区扫描

为了加速并行源任务中的数据读取，SeaTunnel 为 MongoDB 集合提供了分区扫描能力。通过
`partition.split-key` 指定分片字段、`partition.split-size` 指定每个 split 的大小，可以控制
数据分片方式：

```hocon
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    partition.split-key = "id"
    partition.split-size = 1024
    schema = {
      fields {
        id = bigint
        status = string
      }
    }
  }
}
```

> 建议选择有索引的字段作为 split key，能显著加快 split 边界扫描。

### Flat Sync String

启用 `flat.sync-string` 后，只需声明一个 `STRING` 类型字段，连接器会把每条 MongoDB 文档序列化
为扩展 JSON 字符串写入该字段。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    flat.sync-string = true
    schema = {
      fields {
        data = string
      }
    }
  }
}
sink {
  Console {}
}
```

通过该配置写入的样本数据示例：

```json
{
  "_id": {
    "$oid": "643d41f5fdc6a52e90e59cbf"
  },
  "c_map": {
    "OQBqH": "jllt",
    "rkvlO": "pbfdf",
    "pCMEX": "hczrdtve",
    "DAgdj": "t",
    "dsJag": "voo"
  },
  "c_array": [
    { "$numberInt": "-865590937" },
    { "$numberInt": "833905600" },
    { "$numberInt": "-1104586446" },
    { "$numberInt": "2076336780" },
    { "$numberInt": "-1028686444" }
  ],
  "c_string": "bddkzxr",
  "c_boolean": false,
  "c_tinyint": { "$numberInt": "39" },
  "c_smallint": { "$numberInt": "23672" },
  "c_int": { "$numberInt": "-495763561" },
  "c_bigint": { "$numberLong": "3768307617923954543" },
  "c_double": { "$numberDouble": "1.1706091642478246E308" },
  "c_bytes": { "$binary": { "base64": "ZWJ4", "subType": "00" } },
  "c_date": { "$date": { "$numberLong": "1686614400000" } },
  "c_decimal": { "$numberDecimal": "683265300" },
  "c_timestamp": { "$date": { "$numberLong": "1684283772000" } }
}
```

## 修改日志

<ChangeLog />