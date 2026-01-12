import ChangeLog from '../changelog/connector-jdbc.md';

# Kingbase

> JDBC Kingbase Sink 连接器

## 支持连接器版本

- 8.6

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## 描述

> 使用 `Xa transactions` 来确保 `精确一次`。因此仅支持支持 `Xa transactions` 的数据库的 `精确一次`。您可以设置 `is_exactly_once=true` 来启用它。Kingbase 目前不支持

## 支持的数据源信息

| 数据源 | 支持的版本 |        驱动        |                   URL                    |                                             Maven                                              |
|--------|-----------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/'
> 工作目录<br/>
> 例如：cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|              Kingbase 数据类型              |                                                                SeaTunnel 数据类型                                                                |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                         | BOOLEAN                                                                                                                                           |
| INT2                                         | SHORT                                                                                                                                             |
| SMALLSERIAL <br/>SERIAL <br/>INT4            | INT                                                                                                                                               |
| INT8 <br/>BIGSERIAL                          | BIGINT                                                                                                                                            |
| FLOAT4                                       | FLOAT                                                                                                                                             |
| FLOAT8                                       | DOUBLE                                                                                                                                            |
| NUMERIC                                      | DECIMAL((获取指定列的指定列大小),<br/>(获取指定列小数点右边的位数。))) |
| BPCHAR <br/>CHARACTER <br/>VARCHAR <br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                    | LOCALDATETIME                                                                                                                                     |
| TIME                                         | LOCALTIME                                                                                                                                         |
| DATE                                         | LOCALDATE                                                                                                                                         |
| 其他数据类型                              | 暂不支持                                                                                                                                 |

## Sink 选项

|                   参数名                    |  类型   | 必须 | 默认值 |                                                                                                                 描述                                                                                                                  |
|-------------------------------------------|---------|------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是   | -       | JDBC 连接的 URL。参考示例：jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                           |
| driver                                    | String  | 是   | -       | 用于连接到远程数据源的 jdbc 类名，<br/> 如果使用 DB2，则值为 `com.ibm.db2.jdbc.app.DB2Driver`。                                                                                                            |
| username                                      | String  | 否   | -       | 连接实例用户名                                                                                                                                                                                                                |
| password                                  | String  | 否   | -       | 连接实例密码                                                                                                                                                                                                                 |
| query                                     | String  | 否   | -       | 使用此 sql 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 具有更高的优先级                                                                                                                                       |
| database                                  | String  | 否   | -       | 使用此 `database` 和 `table-name` 自动生成 sql 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，具有更高的优先级。                                                     |
| table                                     | String  | 否   | -       | 使用数据库和此 table-name 自动生成 sql 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，具有更高的优先级。                                                         |
| primary_keys                              | Array   | 否   | -       | 此选项用于在自动生成 sql 时支持 `insert`、`delete` 和 `update` 等操作。                                                                                                                          |
| connection_check_timeout_sec              | Int     | 否   | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                          |
| max_retries                               | Int     | 否   | 0       | 提交失败的重试次数 (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | 否   | 1000    | 对于批量写入，当缓冲记录数达到 `batch_size` 数量或时间达到 `checkpoint.interval` 时<br/>，数据将被刷新到数据库                                                         |
| is_exactly_once                           | Boolean | 否   | false   | 是否启用精确一次语义，这将使用 Xa 事务。如果启用，您需要<br/>设置 `xa_data_source_class_name`。Kingbase 目前不支持                                                                        |
| generate_sink_sql                         | Boolean | 否   | false   | 根据您要写入的数据库表生成 sql 语句                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | 否   | -       | 数据库驱动程序的 xa 数据源类名，Kingbase 目前不支持                                                                                                                                                     |
| max_commit_attempts                       | Int     | 否   | 3       | 事务提交失败的重试次数                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | 否   | -1      | 事务打开后的超时时间，默认为 -1（永不超时）。请注意，设置超时可能会影响<br/>精确一次语义                                                                                          |
| auto_commit                               | Boolean | 否   | true    | 默认启用自动事务提交                                                                                                                                                                           |
| common-options                            |         | 否   | -       | Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见                                                                                                                                  |
| enable_upsert                             | Boolean | 否   | true    | 如果存在 primary_keys，启用 upsert。如果任务没有重复数据，将此参数设置为 `false` 可以加快数据导入                                                                                                       |

### 提示

> 如果未设置 partition_column，它将以单并发运行，如果设置了 partition_column，它将根据任务的并发性并行执行。

## 任务示例

### 简单

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 JDBC Sink。FakeSource 生成总共 16 行数据 (row.num=16)，每行有 12 个字段。最终目标表 test_table 也将有 16 行数据。
> 在运行此作业之前，您需要在 Kingbase 中创建数据库 test 和表 test_table。如果您还没有安装和部署 SeaTunnel，您需要按照 [安装 SeaTunnel](../../getting-started/locally/deployment.md) 中的说明进行安装和部署。然后按照 [使用 SeaTunnel 引擎快速开始](../../getting-started/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(30, 8)"
            c_date = date
            c_time = time
            c_timestamp = timestamp
      }
    }
  }
  # 如果您想了解更多关于如何配置 seatunnel 和查看源插件的完整列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果您想了解更多关于如何配置 seatunnel 和查看转换插件的完整列表，
    # 请访问 https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        username = "root"
        password = "123456"
        query = "insert into test_table(c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_decimal,c_date,c_time,c_timestamp) values(?,?,?,?,?,?,?,?,?,?,?,?)"
        }
  # 如果您想了解更多关于如何配置 seatunnel 和查看 sink 插件的完整列表，
  # 请访问 https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的 sql 语句，您可以配置数据库名称表名称来自动为您生成添加语句

```
sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        username = "root"
        password = "123456"
        # 根据数据库表名自动生成 sql 语句
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

## 变更日志

<ChangeLog />


