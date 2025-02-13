# MongoDB

> MongoDB接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

**提示**

> 1.如果要使用CDC编写的功能，建议启用追加销售启用配置。

## 描述

MongoDB连接器提供了从MongoDB读取数据和向MongoDB写入数据的能力。
本文档描述了如何设置MongoDB连接器以针对MongoDB运行数据写入器。

## 支持的数据源信息

为了使用Mongodb连接器，需要以下依赖关系。
它们可以通过install-plugin.sh或Maven中央存储库下载。

| 数据来源 | 支持的版本 | 依赖                                                                            |
|------------|--------------------|---------------------------------------------------------------------------------------|
| MongoDB    | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-mongodb) |

## 数据类型映射

下表列出了从MongoDB BSON类型到Seatunnel数据类型的字段数据类型映射。

| Seatunnel数据类型 | MongoDB BSON类型 |
|---------------------|-------------------|
| STRING              | ObjectId          |
| STRING              | String            |
| BOOLEAN             | Boolean           |
| BINARY              | Binary            |
| INTEGER             | Int32             |
| TINYINT             | Int32             |
| SMALLINT            | Int32             |
| BIGINT              | Int64             |
| DOUBLE              | Double            |
| FLOAT               | Double            |
| DECIMAL             | Decimal128        |
| Date                | Date              |
| Timestamp           | Timestamp[Date]   |
| ROW                 | Object            |
| ARRAY               | Array             |

**提示**

> 1.当使用SeaTunnel将Date和Timestamp类型写入MongoDB时，两者都会在MongoDB中生成Date数据类型，但精度会有所不同。SeaTunnel Date类型生成的数据具有二级精度，而SeaTunnel Timestamp类型生成的数字具有毫秒级精度。<br/>
> 2.在SeaTunnel中使用DECIMAL类型时，请注意最大范围不能超过34位数字，这意味着您应该使用DECIMAL（34,18）。<br/>

## Sink 选项

|         名称          |   类型   | 需要 | 默认 |                                                         描述                                                          |
|-----------------------|----------|----------|---------|------------------------------------------------------------------------------------------------------------------------------|
| uri                   | String   | 是      | -       | MongoDB标准连接uri。例如。mongodb://user:password@hosts:27017/database?readPreference=secondary&slaveOk=true. |
| database              | String   | 是      | -       | 要读取或写入的MongoDB数据库的名称.                                                                               |
| collection            | String   | 是      | -       | 要读取或写入的MongoDB集合的名称.                                                                             |
| schema                | String   | 是      | -       | MongoDB的BSON和seatunnel数据结构映射.                                                                         |
| buffer-flush.max-rows | String   | 否       | 1000    | 指定每个批处理请求的最大缓冲行数.                                                             |
| buffer-flush.interval | String   | 否       | 30000   | 指定每个批处理请求的最大缓冲行间隔，单位为毫秒.                                  |
| retry.max             | String   | 否       | 3       | 指定向数据库写入记录失败时的最大重试次数.                                                     |
| retry.interval        | Duration | 否       | 1000    | 指定向数据库写入记录失败时的重试时间间隔，单位为毫秒.                            |
| upsert-enable         | Boolean  | 否       | 否   | 是否通过追加销售模式编写文档.                                                                                  |
| primary-key           | List     | 否       | -       | 追加销售/更新的主键。属性的键采用`[“id”、“name”、…]`格式.                                   |
| transaction           | Boolean  | 否       | 否   | 是否在MongoSink中使用交易 (需要MongoDB 4.2+).                                                            |
| common-options        |          | 否       | -       | Sink插件常用参数，详见[Sink common Options]（../Sink common Options.md）

### 提示

> 1.MongoDB Sink Connector的数据刷新逻辑由三个参数共同控制：“buffer flush.max lines”、“buffer flush.interval”和“checkpoint.interval“。<br/>
> 如果满足这些条件中的任何一个，将触发数据刷新。<br/>
> 2.与历史参数“upstart key”兼容。如果设置了“追加密钥”，请不要设置“主键”。<br/>

## 如何创建MongoDB数据同步作业

以下示例演示了如何创建数据同步作业，将随机生成的数据写入MongoDB数据库：

```bash
# 设置要执行的任务的基本配置
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
  MongoDB{
    uri = mongodb://user:password@127.0.0.1:27017
    database = "test"
    collection = "test"
    schema = {
      fields {
        _id = string
        c_bigint = bigint
      }
    }
  }
}
```

## Parameter Interpretation

### MongoDB数据库连接URI示例

未经身份验证的单节点连接：

```bash
mongodb://127.0.0.0:27017/mydb
```

副本集连接：

```bash
mongodb://127.0.0.0:27017/mydb?replicaSet=xxx
```

经过身份验证的副本集连接：

```bash
mongodb://admin:password@127.0.0.0:27017/mydb?replicaSet=xxx&authSource=admin
```

多节点副本集连接：

```bash
mongodb://127.0.0..1:27017,127.0.0..2:27017,127.0.0.3:27017/mydb?replicaSet=xxx
```

分片集群连接：

```bash
mongodb://127.0.0.0:27017/mydb
```

多个mongos连接：

```bash
mongodb://192.168.0.1:27017,192.168.0.2:27017,192.168.0.3:27017/mydb
```

注意：URI中的用户名和密码在连接到连接字符串之前必须进行URL编码。

### 缓冲区冲洗

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    buffer-flush.max-rows = 2000
    buffer-flush.interval = 1000
    schema = {
      fields {
        _id = string
        id = bigint
        status = string
      }
    }
  }
}
```

### 为什么不建议使用交易进行操作？

尽管MongoDB从4.2版本开始就完全支持多文档事务，但这并不意味着每个人都应该鲁莽地使用它们。
事务相当于锁、节点协调、额外开销和性能影响。
相反，使用交易的原则应该是：尽可能避免使用它们。
通过合理设计系统，可以大大避免使用事务的必要性。

### Idempotent写作

通过指定一个明确的主键并使用upstart方法，可以实现精确一次写入语义。

如果在配置中定义了“主键”和“upstart enable”，MongoDB接收器将使用upstart语义，而不是常规的INSERT语句。我们将upstart key中声明的主键组合为MongoDB保留的主键，并使用upstart模式进行写入，以确保幂等写入。
如果发生故障，Seatunnel作业将从上一个成功的检查点恢复并重新处理，这可能会导致恢复过程中出现重复的消息处理。强烈建议使用升级模式，因为它有助于避免违反数据库主键约束，并在需要重新处理记录时生成重复数据。

```bash
sink {
  MongoDB {
    uri = "mongodb://user:password@127.0.0.1:27017"
    database = "test_db"
    collection = "users"
    upsert-enable = true
    primary-key = ["name","status"]
    schema = {
      fields {
        _id = string
        name = string
        status = string
      }
    }
  }
}
```

## Changelog

### 2.2.0-beta

- 添加MongoDB源连接器

### 2.3.1-release

- [功能]重构mongodb源连接器([4620](https://github.com/apache/incubator-seatunnel/pull/4620))

### 下一个版本

- [功能]Mongodb支持cdc接收器([4833](https://github.com/apache/seatunnel/pull/4833))

