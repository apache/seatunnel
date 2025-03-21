import ChangeLog from '../changelog/connector-jdbc.md';

# MySQL

> JDBC Mysql Sink 连接器
  
## 支持的Mysql版本

- 5.5/5.6/5.7/8.0/8.1/8.2/8.3/8.4

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过jdbc写入数据。支持批处理模式和流模式，支持并发写入，支持exactly-once精确一次
语义（使用XA事务保证）。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

>使用“Xa事务”来确保“精确一次”。因此，数据库只支持“精确一次”，即
>支持“Xa事务”。您可以设置`is_exactly_once=true `来启用它。

## 支持的数据源信息

| 数据源 |                    支持的版本                   |          驱动器          |                  网址                  | Maven下载链接                                                           |
|-----|---------------------------------------------------------|--------------------------|---------------------------------------|---------------------------------------------------------------------|
| Mysql | 不同的依赖版本具有不同的驱动程序类。 | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306:3306/test | [下载](https://mvnrepository.com/artifact/mysql/mysql-connector-java) |


## 数据类型映射

|                                                          Mysql 数据类型                                                          |                                                                 SeaTunnel 数据类型                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>INT UNSIGNED                                                                                                           | BOOLEAN                                                                                                                                             |
| TINYINT<br/>TINYINT UNSIGNED<br/>SMALLINT<br/>SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                                                                                                                                                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                                                      | BIGINT                                                                                                                                              |
| BIGINT UNSIGNED                                                                                                                   | DECIMAL(20,0)                                                                                                                                       |
| DECIMAL(x,y)(获取指定列的列大小<38)                                                               | DECIMAL(x,y)                                                                                                                                        |
| DECIMAL(x,y)(获取指定列的列大小>38)                                                               | DECIMAL(38,18)                                                                                                                                      |
| DECIMAL UNSIGNED                                                                                                                  | DECIMAL((DECIMAL((获取指定列的列大小)+1,<br/>(获取指定列的小数点右侧的位数))) |
| FLOAT<br/>FLOAT UNSIGNED                                                                                                          | FLOAT                                                                                                                                               |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                                                        | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON                                                       | STRING                                                                                                                                              |
| DATE                                                                                                                              | DATE                                                                                                                                                |
| TIME                                                                                                                              | TIME                                                                                                                                                |
| DATETIME<br/>TIMESTAMP                                                                                                            | TIMESTAMP                                                                                                                                           |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)                                                  | BYTES                                                                                                                                               |
| GEOMETRY<br/>UNKNOWN                                                                                                              | Not supported yet                                                                                                                                   |

## Sink 参数

|                   名称                    |  类型   | 是否必填 |           默认值            |                                                                                                                  描述                                                                                                                   |
|-------------------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是      | -                            | JDBC 连接的 URL。参见示例: <br/>`jdbc:mysql://localhost:3306:3306/test`。                                                                                                                                                         |
| driver                                    | String  | 是      | -                            | 用于连接远程数据源的 JDBC 类名，<br/>如果使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                  |
| user                                      | String  | 否       | -                            | 连接实例用户名。                                                                                                                                                                                                                  |
| password                                  | String  | 否       | -                            | 连接实例密码。                                                                                                                                                                                                                   |
| query                                     | String  | 否       | -                            | 使用此sql将上游输入数据写入数据库。例如： `INSERT ...`,`query` 具有更高的优先级                                                                                                                                         |
| database                                  | String  | 否       | -                            | 使用此 `database` 和 `table-name` 自动生成sql并接收上游输入数据写入数据库。<br/>此选项与`query` 互斥，具有更高的优先级                                                       |
| table                                     | String  | 否       | -                            | 使用数据库和此表名自动生成sql并接收上游输入数据写入数据库。<br/>此选项与`query` 互斥，具有更高的优先级                                                           |
| primary_keys                              | Array   | 否       | -                            | 此选项用于支持以下操作，例如 `insert`, `delete`, 和 `update` 当自动生成sql.                                                                                                                            |
| support_upsert_by_query_primary_key_exist | Boolean | 否       | false                        | 选择使用INSERT sql、UPDATE sql根据查询主键是否存在来处理更新事件（INSERT、UPDATE_AFTER）。此配置仅在数据库不支持升级语法时使用**注**：此方法性能低   |
| connection_check_timeout_sec              | Int     | 否       | 30                           | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                            |
| max_retries                               | Int     | 否       | 0                            | 提交失败的重试次数（executeBatch）                                                                                                                                                                                          |
| batch_size                                | Int     | 否       | 1000                         | 对于批量写入，当缓冲记录的数量达到“batch_size”的数量或时间达到“checkpoint.interval”<br/>时，数据将被刷新到数据库中                                                           |
| is_exactly_once                           | Boolean | 否       | false                        | 是否启用精确一次语义，这将使用Xa事务。如果启用，则需要<br/>设置`xa_data_source_class_name`。                                                                                                              |
| generate_sink_sql                         | Boolean | 否       | false                        | 根据要写入的数据库表生成sql语句                                                                                                                                                                       |
| xa_data_source_class_name                 | String  | 否       | -                            | 数据库Driver的xa数据源类名，例如mysql是`com.mysql.cj.jdbc。MysqlXADataSource，和<br/>请参阅附录了解其他数据源                                                                     |
| max_commit_attempts                       | Int     | 否       | 3                            | 事务提交失败的重试次数                                                                                                                                                                                          |
| transaction_timeout_sec                   | Int     | 否       | -1                           | 事务打开后的超时，默认值为-1（永不超时）。请注意，设置超时可能会影响＜br/＞精确一次语义                                                                                            |
| auto_commit                               | Boolean | 否       | true                         | 默认情况下启用自动事务提交                                                                                                                                                                                             |
| field_ide                                 | String  | 否       | -                            | 确定从源同步到汇时是否需要转换字段`ORIGINAL表示不需要转换`大写`表示转换为大写`LOWERCASE表示转换为小写。     |
| properties                                | Map     | 否       | -                            | 其他连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定。例如，在MySQL中，属性优先于URL。 |
| common-options                            |         | 否       | -                            | Sink插件常用参数，请参考 [Sink Common Options](../sink-common-options.md) 详见                                                                                                                                    |
| schema_save_mode                          | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 在启动同步任务之前，对目标侧的现有表面结构选择不同的处理方案。                                                                                                      |
| data_save_mode                            | Enum    | 否       | APPEND_DATA                  | 在启动同步任务之前，对目标端的现有数据选择不同的处理方案。                                                                                                                 |
| custom_sql                                | String  | 否       | -                            | 当data_save_mode选择CUSTOM_PROCESSING时，您应该填写CUSTOM_SQL参数。此参数通常填充可以执行的SQL。SQL将在同步任务之前执行。                                     |
| enable_upsert                             | Boolean | 否       | true                         | 通过primary_keys存在启用upstart，如果任务只有“插入”，将此参数设置为“false”可以加快数据导入                                                                                                                 |

### 提示

>如果未设置partition_column，它将以单并发运行，如果设置了partition_coolumn，它将根据任务的并发性并行执行。

## 任务示例

### 简单的例子:

>此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并将其发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终的目标表是test_table，表中也将有16行数据。在运行此作业之前，您需要在mysql中创建数据库测试表test_table。如果您尚未安装和部署SeaTunnel，则需要按照[安装SeaTunnel]（../../start-v2/local/deployment.md）中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎]（../../Start-v2/locale/Quick-Start SeaTunnel Engine.md）中的说明运行此作业。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
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
        url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
        }
	#如果你想了解更多关于如何配置seatunnel的信息，并查看完整的sink插件列表，
	#请前往https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成Sink SQL

>此示例不需要编写复杂的sql语句，您可以配置数据库名称表名以自动为您生成add语句

```
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "123456"
        # Automatically generate sql statements based on database table names
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### 精确一次：

为了准确的书写场景，我们保证精确一次

```
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
    
        max_retries = 0
        user = "root"
        password = "123456"
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
    }
}
```

### CDC（变更数据捕获）事件

>我们也支持CDC变更数据。在这种情况下，您需要配置数据库、表和主键。

```
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "123456"
        
        generate_sink_sql = true
        # You need to configure both database and table
        database = test
        table = sink_table
        primary_keys = ["id","name"]
        field_ide = UPPERCASE
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

## 变更日志

<ChangeLog />