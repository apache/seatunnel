# DB2

> JDBC DB2接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过jdbc写入数据。支持批处理模式和流模式，支持并发写入，只支持一次
语义（使用XA事务保证）.

## 使用依赖关系

### 适用于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/plugins/`.

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) 已放置在目录 `${SEATUNNEL_HOME}/lib/`.

## 关键特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> 使用 `Xa transactions` 来确保 `精确一次`. 因此，数据库只支持 `exactly-once` 即
> 支持 `Xa transactions`. 您可以设置 `is_exactly_once=true` 来启用它.

## 支持的数据源信息

| 数据库 |                    支持版本                    |             驱动             |                Url                |                                 Maven                                 |
|------------|---------------------------------------------------------|--------------------------------|-----------------------------------|-----------------------------------------------------------------------|
| DB2        | Different dependency version has different driver class. | com.ibm.db2.jdbc.app.DB2Driver | jdbc:db2://127.0.0.1:50000/dbname | [Download](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## 数据类型映射

|                                            DB2数据类型                                             | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                              | BOOLEAN             |
| SMALLINT                                                                                             | SHORT               |
| INT<br/>INTEGER<br/>                                                                                 | INTEGER             |
| BIGINT                                                                                               | LONG                |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)      |
| REAL                                                                                                 | FLOAT               |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE              |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING              |
| BLOB                                                                                                 | BYTES               |
| DATE                                                                                                 | DATE                |
| TIME                                                                                                 | TIME                |
| TIMESTAMP                                                                                            | TIMESTAMP           |
| ROWID<br/>XML                                                                                        | Not supported yet   |

## 选项

|                   名称                    |  类型   | 必需 | 默认值 | 描述                                                                                                                                                                                                                                             |
|-------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | Yes      | -       | JDBC连接的URL。请参考案例 : jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                                                           |
| driver                                    | String  | Yes      | -       | 用于连接到远程数据源的jdbc类名,<br/> 如果使用DB2，则值为 `com.ibm.db2.jdbc.app.DB2Driver`.                                                                                                                                                                          |
| user                                      | String  | No       | -       | 连接实例用户名                                                                                                                                                                                                                                        |
| password                                  | String  | No       | -       | 连接实例密码                                                                                                                                                                                                                                         |
| query                                     | String  | No       | -       | 使用此sql将上游输入数据写入数据库。例如 `INSERT ...`,`query` 具有更高的优先级                                                                                                                                                                                            |
| database                                  | String  | No       | -       | 使用这个 `database` 和 `table-name` 自动生成sql并接收上游输入数据写入数据库.<br/>此选项与 `query` 互斥，具有更高的优先级.                                                                                                                                                            |
| table                                     | String  | No       | -       | 使用数据库和此表名自动生成sql并接收上游输入数据写入数据库.<br/>此选项与 `query` 互斥，具有更高的优先级.                                                                                                                                                                                  |
| primary_keys                              | Array   | No       | -       | 此选项用于在自动生成sql时支持 `insert`, `delete`, 和 `update` 等操作.                                                                                                                                                                                           |
| support_upsert_by_query_primary_key_exist | Boolean | No       | false   | 选择使用INSERT sql、UPDATE sql根据查询主键是否存在来处理更新事件(INSERT, UPDATE_AFTER). 此配置仅在数据库不支持升级语法时使用. **注**: 此方法性能低                                                                                                                                            |
| connection_check_timeout_sec              | Int     | No       | 30      | 等待用于验证连接的数据库操作完成的时间（秒）.                                                                                                                                                                                                                        |
| max_retries                               | Int     | No       | 0       | 提交失败的重试次数 (执行批处理)                                                                                                                                                                                                                              |
| batch_size                                | Int     | No       | 1000    | 对于批量写入，当缓冲记录的数量达到 `batch_size` 的数量或时间达到 `checkpoint.interval` 时<br/>, 数据将被刷新到数据库中                                                                                                                                                              |
| is_exactly_once                           | Boolean | No       | false   | 是否启用精确一次语义，这将使用 Xa 事务. 如果启用，则需要<br/>设置 `xa_data_source_class_name`.                                                                                                                                                                            |
| generate_sink_sql                         | Boolean | No       | false   | 根据要写入的数据库表生成sql语句                                                                                                                                                                                                                              |
| xa_data_source_class_name                 | String  | No       | -       | 数据库Driver的 xa 数据源类名, for example, DB2 是 `com.db2.cj.jdbc.Db2XADataSource`, <br/>其他数据来源请参考附录                                                                                                           |
| max_commit_attempts                       | Int     | No       | 3       | 事务提交失败的重试次数                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | No       | -1      | 事务打开后的超时，默认值为-1（永不超时）. 请注意，设置超时可能会影响＜br/＞精确一次语义                                                                                            |
| auto_commit                               | Boolean | No       | true    | 默认情况下启用自动事务提交                                                                                                                                                                                             |
| properties                                | Map     | No       | -       | 附加连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定. 例如，在MySQL中，属性优先于URL. |
| common-options                            |         | no       | -       | Sink插件常用参数，详见 [Sink Common Options](../sink-common-options.md)                                                                                                                                     |

### 小贴士

> 如果未设置partition_column，它将以单并发运行，如果设置了partition_coolumn，它将根据任务的并发性并行执行.

## 任务示例

### 简单:

> 此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并将其发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终的目标表是test_table，表中也将有16行数据。在运行此作业之前，您需要在DB2中创建数据库测试和表test_table。如果您尚未安装和部署SeaTunnel，则需要按照[Install SeaTunnel](../../start-v2/locally/deployment.md)中的说明安装和部署SeaTunnel。然后按照[Quick Start With SeaTunnel Engine](../../start-v2/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业.

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示功能源插件**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的源插件列表,
  # 请前往 https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表
    # 请前往 https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        }
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的接收插件列表,
  # 请前往 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的sql语句，您可以配置数据库名称表名以自动为您生成add语句

```
sink {
    jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        user = "root"
        password = "123456"
        # Automatically generate sql statements based on database table names
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### 精确一次 :

> 为了准确的书写场景，我们保证一次准确

```
sink {
    jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
    
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "com.db2.cj.jdbc.Db2XADataSource"
    }
}
```

