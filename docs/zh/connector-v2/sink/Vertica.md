# Vertica

> JDBC Vertica Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 写入数据。支持批处理和流处理模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://www.vertica.com/download/vertica/client-drivers/) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 需要确保 [jdbc 驱动 jar 包](https://www.vertica.com/download/vertica/client-drivers/) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

> 使用 `Xa 事务` 来保证 `精确一次`。因此仅支持支持 `Xa 事务` 的数据库。可以通过设置 `is_exactly_once=true` 来启用。

## 支持的数据源信息

| 数据源    | 支持的版本                     | 驱动类名                     | URL 格式                             | Maven 依赖                                                                                   |
|-----------|--------------------------------|------------------------------|--------------------------------------|---------------------------------------------------------------------------------------------|
| Vertica   | 不同依赖版本有不同的驱动类名   | com.vertica.jdbc.Driver      | jdbc:vertica://localhost:5433/vertica | [下载](https://www.vertica.com/download/vertica/client-drivers/)                            |

## 数据库依赖

> 请下载支持列表中对应的 'Maven' 依赖，并将其复制到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 工作目录中。<br/>
> 例如 Vertica 数据源：`cp vertica-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

| Vertica 数据类型                                                                                     | SeaTunnel 数据类型                                                                                   |
|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                              | BOOLEAN                                                                                             |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                                                                                                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                        | BIGINT                                                                                              |
| BIGINT UNSIGNED                                                                                      | DECIMAL(20,0)                                                                                       |
| DECIMAL(x,y)(获取指定列的列大小 <38)                                                                 | DECIMAL(x,y)                                                                                        |
| DECIMAL(x,y)(获取指定列的列大小 >38)                                                                 | DECIMAL(38,18)                                                                                      |
| DECIMAL UNSIGNED                                                                                     | DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数)))                                |
| FLOAT<br/>FLOAT UNSIGNED                                                                             | FLOAT                                                                                               |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                           | DOUBLE                                                                                              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                          | STRING                                                                                              |
| DATE                                                                                                 | DATE                                                                                                |
| TIME                                                                                                 | TIME                                                                                                |
| DATETIME<br/>TIMESTAMP                                                                               | TIMESTAMP                                                                                           |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)                     | BYTES                                                                                               |
| GEOMETRY<br/>UNKNOWN                                                                                 | 尚未支持                                                                                            |

## 接收器选项

| 名称                              | 类型    | 是否必填 | 默认值  | 描述                                                                                                                                                                                                 |
|-----------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                               | String  | 是       | -       | JDBC 连接的 URL。参考示例：`jdbc:vertica://localhost:5433/vertica`                                                                                                                                    |
| driver                            | String  | 是       | -       | 用于连接远程数据源的 JDBC 类名，如果使用 Vertica，值为 `com.vertica.jdbc.Driver`。                                                                                                                   |
| user                              | String  | 否       | -       | 连接实例的用户名                                                                                                                                                                                     |
| password                          | String  | 否       | -       | 连接实例的密码                                                                                                                                                                                       |
| query                             | String  | 否       | -       | 使用此 SQL 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 优先级更高。                                                                                                                         |
| database                          | String  | 否       | -       | 使用此 `database` 和 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                   |
| table                             | String  | 否       | -       | 使用 `database` 和此 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。此选项与 `query` 互斥，且优先级更高。                                                                                   |
| primary_keys                      | Array    | 否       | -       | 此选项用于在自动生成 SQL 时支持 `insert`、`delete` 和 `update` 等操作。                                                                                                                              |
| support_upsert_by_query_primary_key_exist | Boolean  | 否       | false   | 选择使用 INSERT SQL、UPDATE SQL 来处理更新事件（INSERT, UPDATE_AFTER），基于查询主键是否存在。此配置仅在数据库不支持 upsert 语法时使用。**注意**：此方法性能较低。                                   |
| connection_check_timeout_sec      | Int    | 否       | 30      | 用于验证连接完成的数据库操作的等待时间（秒）。                                                                                                                                                       |
| max_retries                       | Int    | 否       | 0       | 提交失败（executeBatch）的重试次数。                                                                                                                                                                 |
| batch_size                        | Int    | 否       | 1000    | 对于批量写入，当缓冲的记录数达到 `batch_size` 或时间达到 `checkpoint.interval` 时，数据将被刷新到数据库中。                                                                                           |
| is_exactly_once                   | Boolean  | 否       | false   | 是否启用精确一次语义，将使用 Xa 事务。如果启用，需要设置 `xa_data_source_class_name`。                                                                                                               |
| generate_sink_sql                 | Boolean  | 否       | false   | 根据要写入的数据库表生成 SQL 语句。                                                                                                                                                                  |
| xa_data_source_class_name         | String  | 否       | -       | 数据库驱动的 XA 数据源类名，例如 Vertica 为 `com.vertical.cj.jdbc.VerticalXADataSource`，其他数据源请参考附录。                                                                                      |
| max_commit_attempts               | Int    | 否       | 3       | 事务提交失败的重试次数。                                                                                                                                                                             |
| transaction_timeout_sec           | Int    | 否       | -1      | 事务打开后的超时时间，默认为 -1（永不超时）。注意：设置超时可能会影响精确一次语义。                                                                                                                  |
| auto_commit                       | Boolean  | 否       | true    | 默认启用自动事务提交。                                                                                                                                                                               |
| properties                        | Map    | 否       | -       | 额外的连接配置参数，当 properties 和 URL 中有相同的参数时，优先级由驱动的具体实现决定。例如，在 MySQL 中，properties 优先于 URL。                                                                     |
| common-options                    |         | 否       | -       | 接收器插件通用参数，详情请参考 [Sink Common Options](../sink-common-options.md)。                                                                                                                    |
| enable_upsert                     | Boolean  | 否       | true    | 通过主键存在启用 upsert。如果任务中没有键重复数据，将此参数设置为 `false` 可以加快数据导入速度。                                                                                                     |

### 提示

> 如果未设置 `partition_column`，将以单并发运行；如果设置了 `partition_column`，将根据任务的并发度并行执行。

## 任务示例

### 简单示例：

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并发送到 JDBC Sink。FakeSource 总共生成 16 行数据（row.num=16），每行有两个字段，name（字符串类型）和 age（int 类型）。最终目标表 test_table 中也将有 16 行数据。在运行此任务之前，您需要在 Vertica 中创建数据库 test 和表 test_table。如果您尚未安装和部署 SeaTunnel，请按照 [安装 SeaTunnel](../../start-v2/locally/deployment.md) 中的说明进行安装和部署。然后按照 [使用 SeaTunnel Engine 快速开始](../../start-v2/locally/quick-start-seatunnel-engine.md) 中的说明运行此任务。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件，**仅用于测试和演示功能**
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
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的源插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的转换插件列表，
  # 请访问 https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    }
  # 如果想了解更多关于如何配置 SeaTunnel 的信息，并查看完整的接收器插件列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成接收器 SQL

> 此示例不需要编写复杂的 SQL 语句，您可以通过配置数据库名称和表名称自动生成插入语句。

```
sink {
    jdbc {
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        user = "root"
        password = "123456"
        # 根据数据库表名自动生成 SQL 语句
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### 精确一次：

> 对于精确写入场景，我们保证精确一次语义。

```
sink {
    jdbc {
        url = "jdbc:vertica://localhost:5433/vertica"
        driver = "com.vertica.jdbc.Driver"
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        is_exactly_once = "true"
        xa_data_source_class_name = "com.vertical.cj.jdbc.VerticalXADataSource"
    }
}
```
