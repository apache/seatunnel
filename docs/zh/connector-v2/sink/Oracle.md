# Oracle

> JDBC Oracle Sink 连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过jdbc写入数据。支持批处理模式和流模式，支持并发写入，支持“精确一次”
语义（使用XA事务保证）。

## 依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8)已经添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已经添加到目录  `${SEATUNNEL_HOME}/lib/`.

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

>使用“Xa事务”来确保“精确一次”。因此，数据库只支持“精确一次”，即
>支持“Xa事务”。您可以设置`is_exactly_once=true `来启用它。

## 支持的数据源信息

| 数据源 |                    支持的版本                    |          驱动器          |                  网址                    |                               Maven下载链接                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | 不同的依赖版本具有不同的驱动程序类。 | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## 数据库依赖关系

>请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”工作目录<br/>
>例如，Oracle数据源：cp ojdbc8-xxxx.jar$SEATUNNEL_HOME/lib/<br/>
>要支持i18n字符集，请将orai18n.jar复制到$SEATUNNEL_HOME/lib/目录。

## 数据类型映射

|                                   Oracle 数据类型                                   | SeaTunnel 数据类型 |
|--------------------------------------------------------------------------------------|---------------------|
| INTEGER                                                                              | INT                 |
| FLOAT                                                                                | DECIMAL(38, 18)     |
| NUMBER(precision <= 9, scale == 0)                                                   | INT                 |
| NUMBER(9 < precision <= 18, scale == 0)                                              | BIGINT              |
| NUMBER(18 < precision, scale == 0)                                                   | DECIMAL(38, 0)      |
| NUMBER(scale != 0)                                                                   | DECIMAL(38, 18)     |
| BINARY_DOUBLE                                                                        | DOUBLE              |
| BINARY_FLOAT<br/>REAL                                                                | FLOAT               |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/> | STRING              |
| DATE                                                                                 | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                         | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                  | BYTES               |

## 参数

|                   名称                    |  类型   | 是否必填 |           默认值            |                                                                                                                  描述                                                                                                                   |
|-------------------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是      | -                            | JDBC 连接的 URL。参见示例: jdbc:oracle:thin:@datasource01:1523:xe                                                                                                                                                        |
| driver                                    | String  | 是      | -                            | 用于连接远程数据源的 JDBC 类名，<br/> 如果使用 Oracle，值为 `oracle.jdbc.OracleDriver`。                                                                                                                 |
| user                                      | String  | 否       | -                            | 连接实例用户名。                                                                                                                                                                                                                  |
| password                                  | String  | 否       | -                            | 连接实例密码。                                                                                                                                                                                                                   |
| query                                     | String  | 否       | -                            | 使用此sql将上游输入数据写入数据库。例如： `INSERT ...`,`query` 具有更高的优先级                                                                                                                                           |
| database                                  | String  | 否       | -                            | 使用此 `database` 和 `table-name` 自动生成sql并接收上游输入数据写入数据库。<br/>此选项与`query` 互斥，具有更高的优先级                                                       |
| table                                     | String  | 否       | -                            | 使用数据库和此表名自动生成sql并接收上游输入数据写入数据库。<br/>此选项与`query` 互斥，具有更高的优先级                                                           |
| primary_keys                              | Array   | 否       | -                            | 此选项用于支持以下操作，例如 `insert`, `delete`, 和 `update` 当自动生成sql.                                                                                                                                    |
| support_upsert_by_query_primary_key_exist | Boolean | 否       | false                        | 选择使用INSERT sql、UPDATE sql根据查询主键是否存在来处理更新事件（INSERT、UPDATE_AFTER）。此配置仅在数据库不支持升级语法时使用**注**：此方法性能低   |
| connection_check_timeout_sec              | Int     | 否       | 30                           | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                            |
| max_retries                               | Int     | 否       | 0                            | 提交失败的重试次数（executeBatch）                                                                                                                                                                                          |
| batch_size                                | Int     | 否       | 1000                         | 对于批量写入，当缓冲记录的数量达到“batch_size”的数量或时间达到“checkpoint.interval”<br/>时，数据将被刷新到数据库中。                                                                  |
| batch_interval_ms                         | Int     | 否       | 1000                         | 对于批写入，当缓冲区的数量达到“batch_size”的数量或时间达到“batch-interval_ms”时，数据将被刷新到数据库中。                                                                           |
| is_exactly_once                           | Boolean | 否       | false                        | 是否启用精确一次语义，这将使用Xa事务。如果启用，则需要<br/>设置`xa_data_source_class_name`。                                                                                                              |
| generate_sink_sql                         | Boolean | 否       | false                        | 根据要写入的数据库表生成sql语句                                                                                                                                                                        |
| xa_data_source_class_name                 | String  | 否       | -                            | 数据库Driver的xa数据源类名，例如Oracle，是`Oracle.jdbc.xa.client。OracleXADataSource和<br/>请参阅附录了解其他数据源                                                               |
| max_commit_attempts                       | Int     | 否       | 3                            | 事务提交失败的重试次数                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | 否       | -1                           | 事务打开后的超时，默认值为-1（永不超时）。请注意，设置超时可能会影响＜br/＞精确一次语义                                                                                            |
| auto_commit                               | Boolean | 否       | true                         | 默认情况下启用自动事务提交                                                                                                                                                                                              |
| properties                                | Map     | 否       | -                            | 其他连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定。例如，在MySQL中，属性优先于URL。 |
| common-options                            |         | 否       | -                            | Sink插件常用参数，请参考 [Sink Common Options](../sink-common-options.md)                                                                                                                                     |
| schema_save_mode                          | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 在启动同步任务之前，对目标侧的现有表面结构选择不同的处理方案。                                                                                                      |
| data_save_mode                            | Enum    | 否       | APPEND_DATA                  | 在启动同步任务之前，对目标端的现有数据选择不同的处理方案。                                                                                                                 |
| custom_sql                                | String  | 否       | -                            | 当data_save_mode选择CUSTOM_PROCESSING时，您应该填写CUSTOM_SQL参数。此参数通常填充可以执行的SQL。SQL将在同步任务之前执行。                                       |
| enable_upsert                             | Boolean | 否       | true                         | 通过primary_keys存在启用upstart，如果任务只有“插入”，将此参数设置为“false”可以加快数据导入                                                                                                          |

### 提示

>如果未设置partition_column，它将以单并发运行，如果设置了partition_column，它将根据任务的并发数并行执行。

## 任务示例

### 简单的例子:

>此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并将其发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终的目标表是test_table，表中也将有16行数据。在运行此作业之前，您需要在Oracle中创建测试数据库和表test_table。如果您尚未安装和部署SeaTunnel，则需要按照[安装SeaTunnel]（../../start-v2/local/deployment.md）中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎]（../../Start-v2/locale/Quick-Start-SeaTunnel-Engine.md）中的说明运行此作业。

```
# 定义运行环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
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
	#如果你想了解更多关于如何配置seatunnel的信息，并查看完整的源插件列表，
	#请前往https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
	#如果你想了解更多关于如何配置seatunnel的信息，并查看转换插件的完整列表，
	#请前往https://seatunnel.apache.org/docs/transform-v2
}

sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        user = root
        password = 123456
        query = "INSERT INTO TEST.TEST_TABLE(NAME,AGE) VALUES(?,?)"
     }
	#如果你想了解更多关于如何配置seatunnel的信息，并查看完整的sink插件列表，
	#请前往https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成Sink SQL

>此示例不需要编写复杂的sql语句，您可以配置数据库名称表名以自动为您生成add语句

```
sink {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        user = root
        password = 123456
        
        generate_sink_sql = true
        database = XE
        table = "TEST.TEST_TABLE"
    }
}
```

### 精确一次：

为了准确的写入场景，我们保证一次准确

```
sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
    
        max_retries = 0
        user = root
        password = 123456
        query = "INSERT INTO TEST.TEST_TABLE(NAME,AGE) VALUES(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "oracle.jdbc.xa.client.OracleXADataSource"
    }
}
```

### CDC（变更数据捕获）事件

>我们也支持CDC更改数据。在这种情况下，您需要配置数据库、表和主键。

```
sink {
    jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        user = root
        password = 123456
        
        generate_sink_sql = true
        # You need to configure both database and table
        database = XE
        table = "TEST.TEST_TABLE"
        primary_keys = ["ID"]
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

