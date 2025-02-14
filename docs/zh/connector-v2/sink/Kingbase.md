# Kingbase

> JDBC Kingbase数据接收器

## 支持连接器版本

- 8.6

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 描述

>使用"Xa transactions"来确保"精确一次"。因此，数据库只支持"精确一次"，即
>支持"Xa transactions"。您可以设置`is_exactly_once=true `来启用它。Kingbase目前不支持

## 支持的数据源信息

| Datasource | Supported versions |        Driver        |                   Url                    |                                             Maven                                              |
|------------|--------------------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库相关性

> 请下载"Maven"对应的支持列表，并将其复制到"$SEATUNNEL_HOME/plugins/jdbc/lib/"
> 工作目录<br/>
> 例如：cp-kingbase8-8.6.0.jar$SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|              Kingbase 数据类型              |                                                                SeaTunnel 数据类型                                                                |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                         | BOOLEAN                                                                                                                                           |
| INT2                                         | SHORT                                                                                                                                             |
| SMALLSERIAL <br/>SERIAL <br/>INT4            | INT                                                                                                                                               |
| INT8 <br/>BIGSERIAL                          | BIGINT                                                                                                                                            |
| FLOAT4                                       | FLOAT                                                                                                                                             |
| FLOAT8                                       | DOUBLE                                                                                                                                            |
| NUMERIC                                      | DECIMAL((Get the designated column's specified column size),<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| BPCHAR <br/>CHARACTER <br/>VARCHAR <br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                    | LOCALDATETIME                                                                                                                                     |
| TIME                                         | LOCALTIME                                                                                                                                         |
| DATE                                         | LOCALDATE                                                                                                                                         |
| Other data type                              | Not supported yet                                                                                                                                 |

## Sink 选项

|                   名称                    |  类型   | 是否必传 | 默认值 |                                                                                                                 描述                                                                                                                  |
|-------------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是      | -       | JDBC连接的URL。请参考案例：jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                           |
| driver                                    | String  | 是      | -       | 用于连接到远程数据源的jdbc类名，<br/>如果使用DB2，则值为 `com.ibm.db2.jdbc.app.DB2Driver`.                                                                                                            |
| user                                      | String  | 否       | -       | 连接实例用户名                                                                                                                                                                                                                |
| password                                  | String  | 否       | -       | 连接实例用密码                                                                                                                                                                                                                 |
| query                                     | String  | 否       | -       | 使用此sql将上游输入数据写入数据库。例如`INSERT ...`,`query`具有更高的优先级                                                                                                                                       |
| database                                  | String  | 否       | -       | 使用这个“database”和“table-name”自动生成sql并接收上游输入数据写入数据库<br/>此选项与“query”互斥，具有更高的优先级。                                                     |
| table                                     | String  | 否       | -       | 使用数据库和此表名自动生成sql并接收上游输入数据写入数据库<br/>此选项与“query”互斥，具有更高的优先级。                                                         |
| primary_keys                              | Array   | 否       | -       | 此选项用于在自动生成sql时支持`insert`, `delete` 和`update`等操作。                                                                                                                          |
| support_upsert_by_query_primary_key_exist | Boolean | 否       | false   | 选择使用INSERT sql、UPDATE sql根据查询主键是否存在来处理更新事件（INSERT, UPDATE_AFTER）。此配置仅在数据库不支持升级语法时使用**注**：此方法性能低 |
| connection_check_timeout_sec              | Int     | 否       | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                          |
| max_retries                               | Int     | 否       | 0       | 提交失败的重试次数（executeBatch）                                                                                                                                                                                        |
| batch_size                                | Int     | 否       | 1000    | 对于批量写入，当缓冲记录的数量达到“batch_size”的数量或时间达到“checkpoint.interval”<br/>时，数据将被刷新到数据库中                                                         |
| is_exactly_once                           | Boolean | 否       | false   | 是否启用精确一次语义，这将使用Xa事务。如果启用，则需要<br/>设置`xa_data_source_class_name`。Kingbase目前不支持                                                                        |
| generate_sink_sql                         | Boolean | 否       | false   | 根据要写入的数据库表生成sql语句                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | 否       | -       | database Driver的xa数据源类名，Kingbase目前不支持                                                                                                                                                     |
| max_commit_attempts                       | Int     | 否       | 3       | 事务提交失败的重试次数                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1      | 事务打开后的超时，默认值为-1（永不超时）。请注意，设置超时可能会影响＜br/＞精确一次语义                                                                                          |
| auto_commit                               | Boolean | 否       | true    | 默认情况下启用自动事务提交                                                                                                                                                                                           |
| common-options                            |         | 否       | -       | Sink插件常用参数，详见[Sink common Options]（../Sink common Options.md）                                                                                                                                  |
| enable_upsert                             | Boolean | 否       | true    | 通过primary_keys存在启用upstart，如果任务没有密钥重复数据，将此参数设置为“false”可以加快数据导入                                                                                                       |

### 提示

> 如果未设置partition_column，它将以单并发方式运行，如果设置了partition_coolumn，它将被执行
> 根据任务的并发性并行执行。

## 任务示例

### 简单示例:

> 此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并发送
> 它连接到JDBC接收器。FakeSource总共生成16行数据（row.num=16），每行有12个字段。最终的目标表是test_table，表中也将有16行数据。
> 之前
> 运行此作业，您需要在Kingbase中创建数据库测试和表test_table。如果你还没有安装和
> 已部署SeaTunnel，您需要按照[安装SeaTunnel]中的说明进行操作（../../start-v2/local/deployment.md）
> to
> 安装和部署SeaTunnel。然后按照指示进行
> 在[快速启动SeaTunnel引擎]（../../Start-v2/locale/Quick-Start SeaTunnel Engine.md）中运行此作业。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件**，仅用于测试和演示功能源插件**
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
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的源插件列表，
  # 请前往https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表，
    # 请前往https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_decimal,c_date,c_time,c_timestamp) values(?,?,?,?,?,?,?,?,?,?,?,?)"
        }
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的sink插件列表，
  # 请前往https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成Sink SQL

> 此示例不需要编写复杂的sql语句，可以将数据库名称表名配置为自动
> 为您生成add语句

```
sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        user = "root"
        password = "123456"
        # Automatically generate sql statements based on database table names
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

