# PostgreSql

> JDBC PostgreSql 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 写入数据。支持批处理模式和流式模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 使用依赖

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [变更数据捕获（CDC）](../../concept/connector-v2-features.md)

> 使用 `XA 事务` 来确保 `精确一次`。因此，仅对支持 `XA 事务` 的数据库支持 `精确一次`。您可以设置 `is_exactly_once=true` 来启用此功能。

## 支持的数据源信息
| 数据源       |                     支持的版本                     |        驱动         |                  URL                  |                                  Maven                                   |
|--------------|-----------------------------------------------------|----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL   | 不同的依赖版本有不同的驱动类。                     | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql)      |
| PostgreSQL   | 如果您想在 PostgreSQL 中处理 GEOMETRY 类型。      | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)     |

## 数据库依赖

> 请下载与 'Maven' 对应的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录中。<br/>
> 例如 PostgreSQL 数据源：`cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`<br/>
> 如果您想在 PostgreSQL 中处理 GEOMETRY 类型，请将 `postgresql-xxx.jar` 和 `postgis-jdbc-xxx.jar` 添加到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 中。

## 数据类型映射
|                                       PostgreSQL 数据类型                                       |                                                              SeaTunnel 数据类型                                                               |
|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                                        | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                                                       | ARRAY&lt;BOOLEAN&gt;                                                                                                                           |
| BYTEA<br/>                                                                                       | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                                                      | ARRAY&lt;TINYINT&gt;                                                                                                                           |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                                    | INT                                                                                                                                            |
| _INT2<br/>_INT4<br/>                                                                             | ARRAY&lt;INT&gt;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                                                          | BIGINT                                                                                                                                         |
| _INT8<br/>                                                                                       | ARRAY&lt;BIGINT&gt;                                                                                                                            |
| FLOAT4<br/>                                                                                      | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                                                     | ARRAY&lt;FLOAT&gt;                                                                                                                             |
| FLOAT8<br/>                                                                                      | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                                                     | ARRAY&lt;DOUBLE&gt;                                                                                                                            |
| NUMERIC(指定列的列大小>0)                                                                        | DECIMAL(指定列的列大小，获取指定列小数点右侧的数字位数)                                                                                       |
| NUMERIC(指定列的列大小<0)                                                                        | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB<br/>UUID | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                                    | ARRAY&lt;STRING&gt;                                                                                                                            |
| TIMESTAMP<br/>                                                                                   | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                                        | TIME                                                                                                                                           |
| DATE<br/>                                                                                        | DATE                                                                                                                                           |
| 其他数据类型                                                                                     | 目前不支持                                                                                                                                    |

## 选项

|                   名称                    | 类型      | 必填 |           默认            |                                                                                                                                                                                                                                                                                    描述                                                                                                                                                                                                                                                                                    |
|-------------------------------------------|---------|------|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是   | -                            | JDBC 连接的 URL。参见示例：jdbc:postgresql://localhost:5432/test <br/> 如果您使用 json 或 jsonb 类型插入，请添加 jdbc url 字符串 `stringtype=unspecified` 选项。                                                                                                                                                                                                                                                                                                                                                                                        |
| driver                                    | String  | 是   | -                            | 用于连接远程数据源的 JDBC 类名，<br/> 如果使用 PostgreSQL，则该值为 `org.postgresql.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| user                                      | String  | 否   | -                            | 连接实例的用户名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| password                                  | String  | 否   | -                            | 连接实例的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| query                                     | String  | 否   | -                            | 使用此 SQL 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 的优先级更高。                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| database                                  | String  | 否   | -                            | 使用此 `database` 和 `table-name` 自动生成 SQL，并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，并具有更高的优先级。                                                                                                                                                                                                                                                                                                                                                                                          |
| table                                     | String  | 否   | -                            | 使用数据库和此表名自动生成 SQL，并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，并具有更高的优先级。表参数可以填写一个不想的表的名称，最终将作为创建表的表名，并支持变量（`${table_name}`，`${schema_name}`）。替换规则： `${schema_name}` 将替换为传递给目标端的 SCHEMA 名称，`${table_name}` 将替换为传递给目标端的表名称。 |
| primary_keys                              | Array   | 否   | -                            | 此选项用于支持在自动生成 SQL 时进行 `insert`，`delete` 和 `update` 操作。                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| support_upsert_by_query_primary_key_exist | Boolean | 否   | false                        | 选择使用 INSERT SQL，UPDATE SQL 根据查询主键存在来处理更新事件（INSERT，UPDATE_AFTER）。此配置仅在数据库不支持 upsert 语法时使用。**注意**：此方法性能较低。                                                                                                                                                                                                                                                                                                                                      |
| connection_check_timeout_sec              | Int     | 否   | 30                           | 用于验证连接的数据库操作完成的等待时间（秒）。                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| max_retries                               | Int     | 否   | 0                            | 提交失败的重试次数（executeBatch）。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| batch_size                                | Int     | 否   | 1000                         | 对于批量写入，当缓冲记录的数量达到 `batch_size` 或时间达到 `checkpoint.interval`<br/>时，数据将刷新到数据库。                                                                                                                                                                                                                                                                                                                                                                                              |
| is_exactly_once                           | Boolean | 否   | false                        | 是否启用精确一次语义，将使用 XA 事务。如果启用，您需要<br/>设置 `xa_data_source_class_name`。                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| generate_sink_sql                         | Boolean | 否   | false                        | 根据要写入的数据库表生成 SQL 语句。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| xa_data_source_class_name                 | String  | 否   | -                            | 数据库驱动的 XA 数据源类名，例如，PostgreSQL 是 `org.postgresql.xa.PGXADataSource`，并<br/>请参阅附录以获取其他数据源。                                                                                                                                                                                                                                                                                                                                                                                                      |
| max_commit_attempts                       | Int     | 否   | 3                            | 事务提交失败的重试次数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| transaction_timeout_sec                   | Int     | 否   | -1                           | 事务开启后的超时时间，默认值为 -1（永不超时）。注意设置超时可能会影响<br/>精确一次语义。                                                                                                                                                                                                                                                                                                                                                                                                                               |
| auto_commit                               | Boolean | 否   | true                         | 默认启用自动事务提交。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| field_ide                                 | String  | 否   | -                            | 识别字段在从源到汇的同步时是否需要转换。`ORIGINAL` 表示无需转换；`UPPERCASE` 表示转换为大写；`LOWERCASE` 表示转换为小写。                                                                                                                                                                                                                                                                                                                                        |
| properties                                | Map     | 否   | -                            | 附加连接配置参数，当 properties 和 URL 具有相同参数时，优先级由<br/>驱动的具体实现决定。例如，在 MySQL 中，properties 优先于 URL。                                                                                                                                                                                                                                                                                                                                    |
| common-options                            |         | 否   | -                            | Sink 插件的公共参数，请参阅 [Sink 公共选项](../sink-common-options.md) 以获取详细信息。                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| schema_save_mode                          | Enum    | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST | 在同步任务开启之前，根据目标端现有表结构选择不同处理方案。                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| data_save_mode                            | Enum      | 否   | APPEND_DATA                  | 在同步任务开启之前，根据目标端现有数据选择不同处理方案。                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| custom_sql                                | String  | 否   | -                            | 当 `data_save_mode` 选择 `CUSTOM_PROCESSING` 时，您应该填写 `CUSTOM_SQL` 参数。此参数通常填入可执行的 SQL。SQL 将在同步任务之前执行。                                                                                                                                                                                                                                                                                                                                                                        |
| enable_upsert                             | Boolean | 否   | true                         | 通过主键存在启用 upsert，如果任务没有重复数据，设置此参数为 `false` 可以加快数据导入。                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### table [字符串]

使用 `database` 和此 `table-name` 自动生成 SQL，并接收上游输入数据写入数据库。

此选项与 `query` 互斥，并具有更高的优先级。

表参数可以填写一个不想的表的名称，最终将作为创建表的表名，并支持变量（`${table_name}`，`${schema_name}`）。替换规则：`${schema_name}` 将替换为传递给目标端的 SCHEMA 名称，`${table_name}` 将替换为传递给目标端的表名称。

例如：
1. `${schema_name}.${table_name} _test`
2. `dbo.tt_${table_name} _sink`
3. `public.sink_table`

### schema_save_mode [枚举]

在同步任务开启之前，根据目标端现有表结构选择不同处理方案。  
选项介绍：  
`RECREATE_SCHEMA` ：当表不存在时将创建，保存时删除并重建。        
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当表不存在时创建，保存时跳过。        
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当表不存在时报告错误。  
`IGNORE` ：忽略对表的处理。

### data_save_mode [枚举]

在同步任务开启之前，根据目标端现有数据选择不同处理方案。  
选项介绍：  
`DROP_DATA`：保留数据库结构并删除数据。  
`APPEND_DATA`：保留数据库结构，保留数据。  
`CUSTOM_PROCESSING`：用户定义处理。  
`ERROR_WHEN_DATA_EXISTS`：当存在数据时报告错误。
### custom_sql [字符串]

当 `data_save_mode` 选择 `CUSTOM_PROCESSING` 时，您应该填写 `CUSTOM_SQL` 参数。此参数通常填入可以执行的 SQL。SQL 将在同步任务之前执行。

### 提示

> 如果未设置 `partition_column`，它将以单线程并发运行；如果设置了 `partition_column`，它将根据任务的并发性并行执行。

## 任务示例

### 简单示例：

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 JDBC Sink。FakeSource 生成总共 16 行数据（`row.num=16`），每行有两个字段，`name`（字符串类型）和 `age`（整数类型）。最终目标表 `test_table` 也将包含 16 行数据。在运行此作业之前，您需要在 PostgreSQL 中创建数据库 `test` 和表 `test_table`。如果您还未安装和部署 SeaTunnel，请按照 [安装 SeaTunnel](../../start-v2/locally/deployment.md) 中的说明进行安装和部署。然后按照 [快速开始 SeaTunnel 引擎](../../start-v2/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业。

```
# Defining the runtime environment
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
  # If you would like to get more information about how to configure seatunnel and see full list of source plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/source
}

transform {
  # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/category/transform-v2
}

sink {
    jdbc {
       # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = root
        password = 123456
        query = "insert into test_table(name,age) values(?,?)"
     }
  # If you would like to get more information about how to configure seatunnel and see full list of sink plugins,
  # please go to https://seatunnel.apache.org/docs/connector-v2/sink
}
```

### 生成 Sink SQL


> 此示例不需要编写复杂的 SQL 语句，您可以配置数据库名称和表名称，系统将自动为您生成添加语句。

```
sink {
    Jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = org.postgresql.Driver
        user = root
        password = 123456
        
        generate_sink_sql = true
        database = test
        table = "public.test_table"
    }
}
```

### 精确一次：

> 对于精确写入场景，我们保证精确一次。

```
sink {
    jdbc {
       # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
    
        max_retries = 0
        user = root
        password = 123456
        query = "insert into test_table(name,age) values(?,?)"
    
        is_exactly_once = "true"
    
        xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
    }
}
```

### CDC（变更数据捕获）事件

> 我们也支持 CDC 变更数据。在这种情况下，您需要配置数据库、表和主键。

```
sink {
    jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = "org.postgresql.Driver"
        user = root
        password = 123456
        
        generate_sink_sql = true
        # You need to configure both database and table
        database = test
        table = sink_table
        primary_keys = ["id","name"]
        field_ide = UPPERCASE
    }
}
```

### 保存模式功能

```
sink {
    Jdbc {
        # if you would use json or jsonb type insert please add jdbc url stringtype=unspecified option
        url = "jdbc:postgresql://localhost:5432/test"
        driver = org.postgresql.Driver
        user = root
        password = 123456
        
        generate_sink_sql = true
        database = test
        table = "public.test_table"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode="APPEND_DATA"
    }
}
```

