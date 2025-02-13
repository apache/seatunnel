# Kingbase

> JDBC Kingbase接收器连接器

## 支持连接器版本

- 8.6

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 描述

>使用“Xa事务”来确保“恰好一次”。因此，数据库只支持“恰好一次”，即
>支持“Xa交易”。您可以设置`is_exactly_once=true `来启用它。Kingbase目前不支持

## 支持的数据源信息

| Datasource | Supported versions |        Driver        |                   Url                    |                                             Maven                                              |
|------------|--------------------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [Download](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库相关性

> 请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”
> 工作目录<br/>
> 例如：cp-kingbase8-8.6.0.jar$SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|              Kingbase Data Type              |                                                                SeaTunnel Data Type                                                                |
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

|                   名称                    |  类型   | 需要 | 默认 |                                                                                                                 描述                                                                                                                  |
|-------------------------------------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是      | -       | The URL of the JDBC connection. Refer to a case: jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                           |
| driver                                    | String  | 是      | -       | The jdbc class name used to connect to the remote data source,<br/> if you use DB2 the value is `com.ibm.db2.jdbc.app.DB2Driver`.                                                                                                            |
| user                                      | String  | 否       | -       | Connection instance user name                                                                                                                                                                                                                |
| password                                  | String  | 否       | -       | Connection instance password                                                                                                                                                                                                                 |
| query                                     | String  | 否       | -       | Use this sql write upstream input datas to database. e.g `INSERT ...`,`query` have the higher priority                                                                                                                                       |
| database                                  | String  | 否       | -       | Use this `database` and `table-name` auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                     |
| table                                     | String  | 否       | -       | Use database and this table-name auto-generate sql and receive upstream input datas write to database.<br/>This option is mutually exclusive with `query` and has a higher priority.                                                         |
| primary_keys                              | Array   | 否       | -       | This option is used to support operations such as `insert`, `delete`, and `update` when automatically generate sql.                                                                                                                          |
| support_upsert_by_query_primary_key_exist | Boolean | 否       | false   | Choose to use INSERT sql, UPDATE sql to process update events(INSERT, UPDATE_AFTER) based on query primary key exists. This configuration is only used when database unsupport upsert syntax. **Note**: that this method has low performance |
| connection_check_timeout_sec              | Int     | 否       | 30      | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                          |
| max_retries                               | Int     | 否       | 0       | The number of retries to submit failed (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | 否       | 1000    | For batch writing, when the number of buffered records reaches the number of `batch_size` or the time reaches `checkpoint.interval`<br/>, the data will be flushed into the database                                                         |
| is_exactly_once                           | Boolean | 否       | false   | Whether to enable exactly-once semantics, which will use Xa transactions. If on, you need to<br/>set `xa_data_source_class_name`. Kingbase currently does not support                                                                        |
| generate_sink_sql                         | Boolean | 否       | false   | Generate sql statements based on the database table you want to write to                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | 否       | -       | The xa data source class name of the database Driver，Kingbase currently does not support                                                                                                                                                     |
| max_commit_attempts                       | Int     | 否       | 3       | The number of retries for transaction commit failures                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | No       | -1      | The timeout after the transaction is opened, the default is -1 (never timeout). Note that setting the timeout may affect<br/>exactly-once semantics                                                                                          |
| auto_commit                               | Boolean | 否       | true    | Automatic transaction commit is enabled by default                                                                                                                                                                                           |
| common-options                            |         | 否       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details                                                                                                                                  |
| enable_upsert                             | Boolean | 否       | true    | Enable upsert by primary_keys exist, If the task has no key duplicate data, setting this parameter to `false` can speed up data import                                                                                                       |

### 提示

> 如果未设置partition_column，它将以单并发方式运行，如果设置了partition_coolumn，它将被执行
> 根据任务的并发性并行执行。

## Task Example

### 简单的:

> 此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并发送
> 它连接到JDBC接收器。FakeSource总共生成16行数据（row.num=16），每行有12个字段。最终的目标表是test_table，表中也将有16行数据。
> 之前
> 运行此作业，您需要在Kingbase中创建数据库测试和表test_table。如果你还没有安装和
> 已部署SeaTunnel，您需要按照[安装SeaTunnel]中的说明进行操作（../../start-v2/local/deployment.md）
> to
> 安装和部署海底隧道。然后按照指示进行
> 在[快速启动海底隧道引擎]（../../Start-v2/locale/Quick-Start SeaTunnel Engine.md）中运行此作业。

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

