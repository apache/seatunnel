import ChangeLog from '../changelog/connector-jdbc.md';

# Snowflake

> JDBC Snowflake Sink连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [（CDC）](../../concept/connector-v2-features.md)

## 描述

通过JDBC写入数据。支持批处理模式和流处理模式，支持并发写入。

## 支持的数据源列表

| 数据源     | 支持的版本                                                   | 驱动类                                      | URL                                                          | Maven                                                                 |
|------------|--------------------------------------------------------------|---------------------------------------------|--------------------------------------------------------------|---------------------------------------------------------------------------|
| Snowflake  | 不同依赖版本对应不同的驱动类。                                 | net.snowflake.client.jdbc.SnowflakeDriver   | jdbc:snowflake://<account_name>.snowflakecomputing.com   | [下载](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc)   |

## 数据库依赖

> 请下载支持列表中对应的'Maven'依赖，并将其复制到'$SEATUNNEL_HOME/plugins/jdbc/lib/'工作目录下<br/>
> 例如Snowflake数据源：cp snowflake-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

| Snowflake 数据类型                                                       | SeaTunnel 数据类型 |
|--------------------------------------------------------------------------|--------------------|
| BOOLEAN                                                                  | BOOLEAN            |
| TINYINT<br/>SMALLINT<br/>BYTEINT<br/>                                    | SHORT_TYPE         |
| INT<br/>INTEGER<br/>                                                     | INT                |
| BIGINT                                                                   | LONG               |
| DECIMAL<br/>NUMERIC<br/>NUMBER<br/>                                      | DECIMAL(x,y)       |
| DECIMAL(x,y)（获取指定列的大小>38）                                       | DECIMAL(38,18)     |
| REAL<br/>FLOAT4                                                          | FLOAT              |
| DOUBLE<br/>DOUBLE PRECISION<br/>FLOAT8<br/>FLOAT<br/>                    | DOUBLE             |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>STRING<br/>TEXT<br/>VARIANT<br/>OBJECT| STRING             |
| DATE                                                                     | DATE               |
| TIME                                                                     | TIME               |
| DATETIME<br/>TIMESTAMP<br/>TIMESTAMP_LTZ<br/>TIMESTAMP_NTZ<br/>TIMESTAMP_TZ | TIMESTAMP          |
| BINARY<br/>VARBINARY<br/>GEOGRAPHY<br/>GEOMETRY                          | BYTES              |

## 配置选项

| 名称                                      | 类型    | 必填 | 默认值 | 描述                                                                                                                                                                                                 |
|-------------------------------------------|---------|------|--------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是   | -      | JDBC连接的URL。参考示例：jdbc&#58;snowflake://<account_name>.snowflakecomputing.com                                                                                                                 |
| driver                                    | String  | 是   | -      | 用于连接远程数据源的JDBC类名，<br/>如果使用Snowflake，值为`net.snowflake.client.jdbc.SnowflakeDriver`。                                                                                             |
| user                                      | String  | 否   | -      | 连接实例的用户名                                                                                                                                                                                     |
| password                                  | String  | 否   | -      | 连接实例的密码                                                                                                                                                                                       |
| query                                     | String  | 否   | -      | 使用此SQL将上游输入数据写入数据库。例如`INSERT ...`，`query`具有更高的优先级                                                                                                                         |
| database                                  | String  | 否   | -      | 使用此`database`和`table-name`自动生成SQL并接收上游输入数据写入数据库。<br/>此选项与`query`互斥，且具有更高的优先级。                                                                               |
| table                                     | String  | 否   | -      | 使用`database`和此`table-name`自动生成SQL并接收上游输入数据写入数据库。<br/>此选项与`query`互斥，且具有更高的优先级。                                                                               |
| primary_keys                              | Array    | 否   | -      | 此选项用于在自动生成SQL时支持`insert`、`delete`和`update`等操作。                                                                                                                                    |
| support_upsert_by_query_primary_key_exist | Boolean  | 否   | false  | 选择使用INSERT SQL、UPDATE SQL来处理更新事件（INSERT, UPDATE_AFTER），基于查询主键是否存在。此配置仅在数据库不支持upsert语法时使用。**注意**：此方法性能较低。                                       |
| connection_check_timeout_sec              | Int    | 否   | 30     | 用于验证连接的操作的等待时间（秒）。                                                                                                                                                                 |
| max_retries                               | Int    | 否   | 0      | 提交失败（executeBatch）的重试次数                                                                                                                                                                   |
| batch_size                                | Int    | 否   | 1000   | 对于批处理写入，当缓冲的记录数达到`batch_size`或时间达到`checkpoint.interval`时，<br/>数据将被刷新到数据库中                                                                                         |
| max_commit_attempts                       | Int    | 否   | 3      | 事务提交失败的重试次数                                                                                                                                                                               |
| transaction_timeout_sec                   | Int    | 否   | -1     | 事务打开后的超时时间，默认为-1（永不超时）。注意，设置超时可能会影响<br/>精确一次语义                                                                                                                |
| auto_commit                               | Boolean  | 否   | true   | 默认启用自动事务提交                                                                                                                                                                                 |
| properties                                | Map    | 否   | -      | 额外的连接配置参数，当properties和URL中有相同参数时，优先级由驱动程序的<br/>具体实现决定。例如，在MySQL中，properties优先于URL。                                                                     |
| common-options                            |         | 否   | -      | 接收器插件通用参数，详情请参考[接收器通用选项](../sink-common-options.md)                                                                                                                           |
| enable_upsert                             | Boolean  | 否   | true   | 通过主键存在启用upsert，如果任务没有键重复数据，将此参数设置为`false`可以加快数据导入速度                                                                                                             |

## 提示

> 如果未设置`partition_column`，将以单并发运行，如果设置了`partition_column`，将根据任务的并发度并行执行。

## 任务示例

### 简单示例：

> 此示例定义了一个SeaTunnel同步任务，通过FakeSource自动生成数据并发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终目标表`test_table`中也将有16行数据。在运行此作业之前，您需要在Snowflake数据库中创建数据库`test`和表`test_table`。如果您尚未安装和部署SeaTunnel，请按照[安装SeaTunnel](../../start-v2/locally/deployment.md)中的说明进行安装和部署。然后按照[使用SeaTunnel Engine快速入门](../../start-v2/locally/quick-start-seatunnel-engine.md)中的说明运行此作业。

```
# 定义运行时环境
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
    # 如果您想了解更多关于如何配置SeaTunnel的信息，并查看完整的源插件列表，
    # 请访问 https://seatunnel.apache.org/docs/connector-v2/source
}
transform {

    # 如果您想了解更多关于如何配置SeaTunnel的信息，并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2
}
sink {
    jdbc {
        url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
        driver = "net.snowflake.client.jdbc.SnowflakeDriver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    }
    # 如果您想了解更多关于如何配置SeaTunnel的信息，并查看完整的接收器插件列表，
    # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### CDC（变更数据捕获）事件

> 我们也支持CDC变更数据。在这种情况下，您需要配置`database`、`table`和`primary_keys`。

```
sink {
   jdbc {
   url = "jdbc:snowflake://<account_name>.snowflakecomputing.com"
   driver = "net.snowflake.client.jdbc.SnowflakeDriver"
   user = "root"
   password = "123456"
   generate_sink_sql = true
   
   
   # 您需要同时配置database和table
   database = test
   table = sink_table
   primary_keys = ["id","name"]
  }
}
```

## 变更日志

<ChangeLog />