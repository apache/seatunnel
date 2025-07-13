import ChangeLog from '../changelog/connector-mongodb.md';

# MongoDB

> MongoDB 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../concept/connector-v2-features.md)
- [ ] [流](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户自定义split](../../concept/connector-v2-features.md)

## 描述

MongoDB连接器提供了从MongoDB读取数据和向MongoDB写入数据的能力。
本文档描述了如何设置MongoDB连接器以对MongoDB运行数据读取。

## 支持的数据源信息

为了使用Mongodb连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| 数据源 | 支持的版本 | 依赖                                                                                    |
|------------|--------------------|---------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## 数据类型映射

下表列出了从MongoDB BSON类型到SeaTunnel数据类型的字段数据类型映射。

| MongoDB BSON type | SeaTunnel 数据类型 |
|-------------------|----------------|
| ObjectId          | STRING         |
| String            | STRING         |
| Boolean           | BOOLEAN        |
| Binary            | BINARY         |
| Int32             | INTEGER        |
| Int64             | BIGINT         |
| Double            | DOUBLE         |
| Decimal128        | DECIMAL        |
| Date              | Date           |
| Timestamp         | Timestamp      |
| Object            | ROW            |
| Array             | ARRAY          |

对于MongoDB中的特定类型，我们使用扩展JSON格式将其映射到SeaTunnel STRING类型。

| MongoDB BSON type |                                       SeaTunnel STRING                                       |
|-------------------|----------------------------------------------------------------------------------------------|
| Symbol            | {"_value": {"$symbol": "12"}}                                                                |
| RegularExpression | {"_value": {"$regularExpression": {"pattern": "^9$", "options": "i"}}}                       |
| JavaScript        | {"_value": {"$code": "function() { return 10; }"}}                                           |
| DbPointer         | {"_value": {"$dbPointer": {"$ref": "db.coll", "$id": {"$oid": "63932a00da01604af329e33c"}}}} |

**提示**

> 1.在SeaTunnel中使用DECIMAL类型时，请注意最大范围不能超过34位数字，这意味着您应该使用DECIMAL(34,18)。<br/>

## 源配置项

|         参数名         |  类型   | 必须 |     默认值      | 描述                                                                                                                                                                                                                                                                                                 |
|----------------------|---------|----|------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| uri                  | String  | 是  | -                | MongoDB标准连接uri。例如 mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true.                                                                                                                                                                                              |
| database             | String  | 是  | -                | 要读取或写入的MongoDB数据库的名称。                                                                                                                                                                                                                                                                              |
| collection           | String  | 是  | -                | 要读取或写入的MongoDB集合的名称。                                                                                                                                                                                                                                                                               |
| schema               | String  | 是  | -                | MongoDB的BSON和seatunnel数据结构映射。                                                                                                                                                                                                                                                                      |
| match.query          | String  | 否  | -                | 在MongoDB中，过滤器用于过滤查询操作的文档。                                                                                                                                                                                                                                                                          |
| match.projection     | String  | 否 | -                | 在MongoDB中，投影用于控制查询结果中包含的字段。                                                                                                                                                                                                                                                                        |
| partition.split-key  | String  | 否 | _id              | 分片字段。                                                                                                                                                                                                                                                                                              |
| partition.split-size | Long    | 否 | 64 * 1024 * 1024 | 分片大小。                                                                                                                                                                                                                                                                                              |
| cursor.no-timeout    | Boolean | 否 | true             | MongoDB服务器通常在非活动期（10分钟）后超时空闲游标，以防止过度使用内存。将此选项设置为true以防止这种情况发生。但是，如果应用程序处理当前一批文档的时间超过30分钟，则会话将标记为已过期并关闭。 |
| fetch.size           | Int     | 否 | 2048             | 设置每批从服务器获取的文档数量。设置适当的批大小可以提高查询性能，避免一次获取大量数据造成的内存压力。                                                                                    |
| max.time-min         | Long    | 否 | 600              | 此参数是一个MongoDB查询选项，用于限制查询操作的最大执行时间。maxTimeMin的值以分钟为单位。如果查询的执行时间超过指定的时间限制，MongoDB将终止操作并返回错误。                                     |
| flat.sync-string     | Boolean | 否 | true             | 通过使用flatSyncString，只能设置一个字段属性值，字段类型必须是String。此操作将对单个MongoDB数据条目执行字符串映射。                                                                                                                      |
| common-options       |         | 否 | -                | 源插件常用参数，请参考 [源通用选项](../source-common-options.md)                                                                                                                                                                                              |

### 提示

> 1.参数`match.query `与历史旧版本参数`matchQuery `兼容，它们是等效的替换。<br/>

## 如何创建MongoDB数据同步作业

以下示例演示了如何创建数据同步作业，该作业从MongoDB读取数据并将其打印到本地客户端：

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建MongoDB源
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
        c_decimal = "decimal(38, 18)"
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
          c_decimal = "decimal(38, 18)"
          c_timestamp = timestamp
        }
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

## 参数说明

### MongoDB数据库连接URI示例

未经身份验证的单节点连接：

```bash
mongodb://192.168.0.100:27017/mydb
```

副本集连接：

```bash
mongodb://192.168.0.100:27017/mydb?replicaSet=xxx
```

经过身份验证的副本集连接：

```bash
mongodb://admin:password@192.168.0.100:27017/mydb?replicaSet=xxx&authSource=admin
```

多节点副本集连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb?replicaSet=xxx
```

分片集群连接：

```bash
mongodb://192.168.0.100:27017/mydb
```

多个mongos连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

注意：URI中的用户名和密码在连接到连接字符串之前必须进行URL编码。

### 匹配查询扫描

在数据同步场景中，需要尽早使用matchQuery方法来减少后续操作员需要处理的文档数量，从而提高性能。
下面是一个使用`match.query的seatunnel的简单示例`

```bash
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

以下是各种数据类型的MatchQuery查询语句的示例：

```bash
# Query Boolean type
"{c_boolean:true}"
# Query string type
"{c_string:\"OCzCj\"}"
# Query the integer
"{c_int:2}"
# Type of query time
"{c_date:ISODate(\"2023-06-26T16:00:00.000Z\")}"
# Query floating point type
{c_double:{$gte:1.71763202185342e+308}}
```

请参阅如何编写`match.query的语法`：https://www.mongodb.com/docs/manual/tutorial/query-documents

### 投影扫描

在MongoDB中，Projection用于控制查询结果中包含哪些字段。这可以通过指定哪些字段需要返回，哪些字段不需要返回来实现。
在find（）方法中，投影对象可以作为第二个参数传递。投影对象的键表示要包含或排除的字段，值1表示包含，0表示排除。
这里有一个简单的例子，假设我们有一个名为users的集合：

```bash
# Returns only the name and email fields
db.users.find({}, { name: 1, email: 0 });
```

在数据同步场景中，需要尽早使用投影来减少后续操作员需要处理的文档数量，从而提高性能。
以下是一个使用投影的seatunnel的简单示例：

```bash
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

为了加快并行源任务实例中的数据读取速度，seatunnel为MongoDB集合提供了分区扫描功能。提供了以下分区策略。
用户可以通过设置用于分片字段的partition.split-key和用于分片大小的partition.split-size来控制数据分片。

```bash
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

### Flat Sync String

通过使用“flat.sync string”，只能设置一个字段属性值，并且字段类型必须是string。
此操作将对单个MongoDB数据条目执行字符串映射。

```bash
env {
  parallelism = 10
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

使用与修改后的参数同步的数据样本，例如：

```json
{
  "_id":{
    "$oid":"643d41f5fdc6a52e90e59cbf"
  },
  "c_map":{
    "OQBqH":"jllt",
    "rkvlO":"pbfdf",
    "pCMEX":"hczrdtve",
    "DAgdj":"t",
    "dsJag":"voo"
  },
  "c_array":[
    {
      "$numberInt":"-865590937"
    },
    {
      "$numberInt":"833905600"
    },
    {
      "$numberInt":"-1104586446"
    },
    {
      "$numberInt":"2076336780"
    },
    {
      "$numberInt":"-1028688944"
    }
  ],
  "c_string":"bddkzxr",
  "c_boolean":false,
  "c_tinyint":{
    "$numberInt":"39"
  },
  "c_smallint":{
    "$numberInt":"23672"
  },
  "c_int":{
    "$numberInt":"-495763561"
  },
  "c_bigint":{
    "$numberLong":"3768307617923954543"
  },
  "c_float":{
    "$numberDouble":"5.284220288280258E37"
  },
  "c_double":{
    "$numberDouble":"1.1706091642478246E308"
  },
  "c_bytes":{
    "$binary":{
      "base64":"ZWJ4",
      "subType":"00"
    }
  },
  "c_date":{
    "$date":{
      "$numberLong":"1686614400000"
    }
  },
  "c_decimal":{
    "$numberDecimal":"683265300"
  },
  "c_timestamp":{
    "$date":{
      "$numberLong":"1684283772000"
    }
  },
  "c_row":{
    "c_map":{
      "OQBqH":"cbrzhsktmm",
      "rkvlO":"qtaov",
      "pCMEX":"tuq",
      "DAgdj":"jzop",
      "dsJag":"vwqyxtt"
    },
    "c_array":[
      {
        "$numberInt":"1733526799"
      },
      {
        "$numberInt":"-971483501"
      },
      {
        "$numberInt":"-1716160960"
      },
      {
        "$numberInt":"-919976360"
      },
      {
        "$numberInt":"727499700"
      }
    ],
    "c_string":"oboislr",
    "c_boolean":true,
    "c_tinyint":{
      "$numberInt":"-66"
    },
    "c_smallint":{
      "$numberInt":"1308"
    },
    "c_int":{
      "$numberInt":"-1573886733"
    },
    "c_bigint":{
      "$numberLong":"4877994302999518682"
    },
    "c_float":{
      "$numberDouble":"1.5353209063652051E38"
    },
    "c_double":{
      "$numberDouble":"1.1952441956458565E308"
    },
    "c_bytes":{
      "$binary":{
        "base64":"cWx5Ymp0Yw==",
        "subType":"00"
      }
    },
    "c_date":{
      "$date":{
        "$numberLong":"1686614400000"
      }
    },
    "c_decimal":{
      "$numberDecimal":"656406177"
    },
    "c_timestamp":{
      "$date":{
        "$numberLong":"1684283772000"
      }
    }
  },
  "id":{
    "$numberInt":"2"
  }
}
```

## 修改日志

<ChangeLog />

