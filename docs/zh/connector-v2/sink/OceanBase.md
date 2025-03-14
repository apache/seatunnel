# OceanBase

> JDBC OceanBase Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)

## 描述

通过jdbc写入数据。支持批处理模式和流模式，支持并发写入，支持精确一次语义。

## 支持的数据源信息

| 数据源      |       支持版本       |          Driver           |                 Url                  |                                     Maven                                     |
|------------|---------------------|---------------------------|--------------------------------------|-------------------------------------------------------------------------------|
| OceanBase  | 所有OceanBase服务版本 | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [Download](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## 数据库相关依赖

> 请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”工作目录<br/>
> 例如: cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

### Mysql模式

|                                                          Mysql Data type                                                          |                                                                 SeaTunnel Data type                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                                                           | BOOLEAN                                                                                                                                             |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                                                                                                                                                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                                                      | BIGINT                                                                                                                                              |
| BIGINT UNSIGNED                                                                                                                   | DECIMAL(20,0)                                                                                                                                       |
| DECIMAL(x,y)(获取指定列的指定列大小<38)                                                                                               | DECIMAL(x,y)                                                                                                                                        |
| DECIMAL(x,y)(获取指定列的指定列大小>38)                                                                                               | DECIMAL(38,18)                                                                                                                                      |
| DECIMAL UNSIGNED                                                                                                                  | DECIMAL((获取指定列的指定列大小)+1,<br/>(获取指定列小数点右侧的位数。)))                                                                                     |
| FLOAT<br/>FLOAT UNSIGNED                                                                                                          | FLOAT                                                                                                                                               |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                                                        | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                                                       | STRING                                                                                                                                              |
| DATE                                                                                                                              | DATE                                                                                                                                                |
| TIME                                                                                                                              | TIME                                                                                                                                                |
| DATETIME<br/>TIMESTAMP                                                                                                            | TIMESTAMP                                                                                                                                           |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)                                                  | BYTES                                                                                                                                               |
| GEOMETRY<br/>UNK否WN                                                                                                              | 否t supported yet                                                                                                                                   |

### Oracle 模式

|                     Oracle Data type                      | SeaTunnel Data type |
|-----------------------------------------------------------|---------------------|
| Number(p), p <= 9                                         | INT                 |
| Number(p), p <= 18                                        | BIGINT              |
| Number(p), p > 18                                         | DECIMAL(38,18)      |
| REAL<br/> BINARY_FLOAT                                    | FLOAT               |
| BINARY_DOUBLE                                             | DOUBLE              |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>ROWID | STRING              |
| DATE                                                      | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE              | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                       | BYTES               |
| UNK否WN                                                   | 否t supported yet   |

## Sink 选项

|                   Name                    |  Type   | Required | Default |                                                                                                                  Description                                                                                                                   |
|-------------------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是       | -       | JDBC连接的URL。参考案例: jdbc:oceanbase://localhost:2883/test                                                                                                                                                          |
| driver                                    | String  | 是       | -       | 用于连接到远程数据源的jdbc类名应为 `com.oceanbase.jdbc.Driver`.                                                                                                                                          |
| user                                      | String  | 否       | -       | 连接实例用户名                                                                                                                                                                                                                  |
| password                                  | String  | 否       | -       | 连接实例密码                                                                                                                                                                                                                   |
| query                                     | String  | 否       | -       | 使用此sql将上游输入数据写入数据库。例如“insert…”查询具有更高的优先级                                                                                                                                         |
| compatible_mode                           | String  | 是       | -       | OceanBase的兼容模式可以是“mysql”或“oracle”。                                                                                                                                                                                 |
| database                                  | String  | 否       | -       | 使用这个“database”和“table-name”自动生成sql并接收上游输入数据写入数据库<br/>此选项与“query”互斥，具有更高的优先级。                                                       |
| table                                     | String  | 否       | -       | 使用数据库和此表名自动生成sql并接收上游输入数据写入数据库<br/>此选项与“query”互斥，并且具有更高的 priority.                                                           |
| primary_keys                              | Array   | 否       | -       | 此选项用于在自动生成sql时支持“insert”、“delete”和“update”等操作。                                                                                                                           |
| support_upsert_by_query_primary_key_exist | Boolean | 否       | false   | 选择使用INSERT sql、UPDATE sql根据查询主键是否存在来处理更新事件（INSERT、UPDATE_AFTER）。此配置仅在数据库不支持升级语法时使用**注**：此方法性能低   |
| connection_check_timeout_sec              | Int     | 否       | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                            |
| max_retries                               | Int     | 否       | 0       | 提交失败的重试次数(executeBatch)                                                                                                                                                                                          |
| batch_size                                | Int     | 否       | 1000    | 对于批量写入，当缓冲记录的数量达到“batch_size”的数量或时间达到“checkpoint.interval”<br/>时，数据将被刷新到数据库中                                                           |
| generate_sink_sql                         | Boolean | 否       | false   | 根据要写入的数据库表生成sql语句                                                                                                                            |
| max_commit_attempts                       | Int     | 否       | 3       | 事务提交失败的重试次数                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | 否       | -1      | 事务打开后的超时，默认值为-1（永不超时）。请注意，设置超时可能会影响＜br/＞精确一次语义                                                                                           |
| auto_commit                               | Boolean | 否       | true    | 默认情况下启用自动事务提交                                                                                                                                                                                             |
| properties                                | Map     | 否       | -       | 其他连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定。例如，在MySQL中，属性优先于URL。 |
| common-options                            |         | 否       | -       | Sink插件常用参数，详见[Sink common Options]（../sink-common-options.md）                                                                                                                                    |
| enable_upsert                             | Boolean | 否       | true    | 通过primary_keys存在启用upsert，如果任务没有键重复数据，将此参数设置为“false”可以加快数据导入                                                                                                         |

### 提示

> 如果未设置partition_column，它将以单并发运行，如果设置了partition_column，它将根据任务的并发数并行执行。

## 任务示例

### 简单示例:

> 此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并将其发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终的目标表是test_table，表中也将有16行数据。在运行此作业之前，您需要在mysql中创建数据库测试和表test_table。如果您尚未安装和部署SeaTunnel，则需要按照[安装SeaTunnel]（../../start-v2/local/deployment.md）中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎]（../../Start-v2/locale/Quick-Start-SeaTunnel-Engine.md）中的说明运行此作业。

```
# 定义运行环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件，**仅用于测试和演示功能源插件**
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
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的source插件列表，
  # 请前往https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看transform插件的完整列表，
    # 请前往https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        user = "root"
        password = "123456"
        compatible_mode = "mysql"
        query = "insert into test_table(name,age) values(?,?)"
    }
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的sink插件列表，
  # 请前往https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的sql语句，您可以配置数据库名称表名以自动为您生成add语句

```
sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        user = "root"
        password = "123456"
        compatible_mode = "mysql"
        # 根据数据库表名自动生成sql语句
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### CDC(Change Data Capture) 数据变更事件

> 我们也支持CDC变更数据。在这种情况下，您需要配置数据库、表和主键。

```
sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:3306/test"
        driver = "com.oceanbase.jdbc.Driver"
        user = "root"
        password = "123456"
        compatible_mode = "mysql"
        generate_sink_sql = true
        # 您需要同时配置数据库和表
        database = test
        table = sink_table
        primary_keys = ["id","name"]
    }
}
```

