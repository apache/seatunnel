import ChangeLog from '../changelog/connector-jdbc.md';

# OceanBase

> JDBC OceanBase Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

> 精确一次依赖 XA 事务。启用时需要配置 `is_exactly_once = true`、`max_retries = 0`，并填写 OceanBase JDBC 驱动提供的有效 XA 数据源类名。

## 描述

通过 JDBC 写入 OceanBase。该 Sink 支持批处理和流处理任务、CDC 行类型、自动生成 SQL、保存模式、多表写入，以及配置 XA 事务后的精确一次语义。

## 支持的数据源信息

| 数据源      |       支持版本       |          Driver           |                 Url                  |                                     Maven                                     |
|------------|---------------------|---------------------------|--------------------------------------|-------------------------------------------------------------------------------|
| OceanBase  | 所有 OceanBase 服务版本 | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [下载](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## 数据库相关依赖

> 请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”工作目录<br/>
> 例如: cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

### MySQL 模式

|                                                        MySQL 数据类型                                                        |                                                                 SeaTunnel 数据类型                                                                 |
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
| GEOMETRY<br/>UNKNOWN                                                                                                              | 暂不支持                                                                                                                                             |

### Oracle 模式

|                     Oracle 数据类型                      | SeaTunnel 数据类型 |
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
| UNKNOWN                                                   | 暂不支持             |

## Sink 选项

| 参数名                       | 类型    | 是否必填 | 默认值  | 描述 |
|------------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String  | 是       | -       | JDBC连接的URL。参考案例: jdbc:oceanbase://localhost:2883/test                                                                                                                                                          |
| driver                       | String  | 是       | -       | 用于连接到远程数据源的jdbc类名应为 `com.oceanbase.jdbc.Driver`.                                                                                                                                          |
| username                     | String  | 否       | -       | 连接实例用户名                                                                                                                                                                                                                  |
| password                     | String  | 否       | -       | 连接实例密码                                                                                                                                                                                                                   |
| query                        | String  | 否       | -       | 使用该 SQL 将上游数据写入 OceanBase，例如 `INSERT ...`。当 `generate_sink_sql = false` 时必须配置 `query`。自定义 `query` 模式下不会执行保存模式相关配置。 |
| compatible_mode              | String  | 是       | -       | OceanBase的兼容模式可以是“mysql”或“oracle”。                                                                                                                                                                                 |
| database                     | String  | 否       | -       | `generate_sink_sql = true` 时使用的数据库，并且此时必须配置。 |
| table                        | String  | 否       | -       | `generate_sink_sql = true` 时使用的目标表。多表写入时支持 `${schema_name}`、`${table_name}` 等占位符。 |
| primary_keys                 | Array   | 否       | -       | 此选项用于在自动生成sql时支持“insert”、“delete”和“update”等操作。                                                                                                                           |
| connection_check_timeout_sec | Int     | 否       | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                            |
| max_retries                  | Int     | 否       | 0       | 提交失败的重试次数(executeBatch)                                                                                                                                                                                          |
| batch_size                   | Int     | 否       | 1000    | 对于批量写入，当缓冲记录数达到 `batch_size` 时，数据会刷新到 OceanBase。如果 `batch_interval_ms` 大于 0，经过指定时间也会触发刷新。 |
| batch_interval_ms            | Long    | 否       | 0       | 写入触发的定时刷新间隔，单位毫秒。`0` 表示关闭定时刷新；大于 0 时，写入器会在每条记录写入时检查间隔，达到间隔后同步刷新。 |
| is_exactly_once              | Boolean | 否       | false   | 是否通过 XA 事务启用精确一次语义。启用后需要配置 `xa_data_source_class_name`，并保持 `max_retries = 0`。 |
| generate_sink_sql            | Boolean | 否       | false   | 根据要写入的数据库表生成sql语句                                                                                                                            |
| xa_data_source_class_name    | String  | 否       | -       | OceanBase JDBC 驱动提供的 XA 数据源类名。`is_exactly_once = true` 时必须配置。 |
| max_commit_attempts          | Int     | 否       | 3       | 事务提交失败的重试次数                                                                                                                                                                                          |
| transaction_timeout_sec      | Int     | 否       | -1      | 事务打开后的超时时间，默认值为 -1，表示永不超时。设置超时可能影响精确一次语义。 |
| auto_commit                  | Boolean | 否       | true    | 默认情况下启用自动事务提交                                                                                                                                                                                             |
| field_ide                    | String  | 否       | -       | 控制字段名大小写转换，可选 `ORIGINAL`、`UPPERCASE`、`LOWERCASE`。 |
| properties                   | Map     | 否       | -       | 其他连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定。例如，在MySQL中，属性优先于URL。 |
| schema_save_mode             | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 同步任务启动前，对目标表结构的处理方式。 |
| data_save_mode               | Enum    | 否       | APPEND_DATA | 同步任务启动前，对目标表已有数据的处理方式。 |
| custom_sql                   | String  | 否       | -       | `data_save_mode = CUSTOM_PROCESSING` 时，在同步前执行的 SQL。自定义 `query` 模式下不会执行该 SQL。 |
| common-options               |         | 否       | -       | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)                                                                                                                                    |
| enable_upsert                | Boolean | 否       | true    | 通过primary_keys存在启用upsert，如果任务没有键重复数据，将此参数设置为“false”可以加快数据导入                                                                                                         |
| is_primary_key_updated       | Boolean | 否       | true    | 自动生成更新语句时，是否把主键字段放入更新字段中。 |
| support_upsert_by_insert_only | Boolean | 否      | false   | 兼容方言下是否通过仅 INSERT 语句实现 upsert。 |
| multi_table_sink_replica     | Int     | 否       | 1       | 多表写入时使用的 Sink Writer 副本数量。 |

### 提示

> OceanBase MySQL 模式请配置 `compatible_mode = "mysql"`，OceanBase Oracle 模式请配置 `compatible_mode = "oracle"`。
>
> 想自己写完整写入 SQL 时使用 `query`。想让 SeaTunnel 自动生成 INSERT/UPSERT SQL 并执行保存模式时，使用 `generate_sink_sql = true`，并配置 `database` 和 `table`。
>
> 消费 CDC 数据时，建议使用自动生成 SQL 并配置 `primary_keys`，否则 UPDATE 和 DELETE 事件无法安全映射。

## 任务示例

### 简单示例

> 此示例定义了一个SeaTunnel同步任务，该任务通过FakeSource自动生成数据并将其发送到JDBC Sink。FakeSource总共生成16行数据（row.num=16），每行有两个字段，name（字符串类型）和age（int类型）。最终的目标表是test_table，表中也将有16行数据。在运行此作业之前，您需要在mysql中创建数据库测试和表test_table。如果您尚未安装和部署SeaTunnel，则需要按照[安装SeaTunnel](../../getting-started/locally/deployment.md)中的说明安装和部署SeaTunnel。然后按照[快速启动SeaTunnel引擎](../../getting-started/locally/quick-start-seatunnel-engine.md)中的说明运行此作业。

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
  # 请前往https://seatunnel.apache.org/docs/connectors/source
}

transform {
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看transform插件的完整列表，
    # 请前往https://seatunnel.apache.org/docs/transforms
}

sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "mysql"
        query = "insert into test_table(name,age) values(?,?)"
    }
  # 如果你想了解更多关于如何配置seatunnel的信息，并查看完整的sink插件列表，
  # 请前往https://seatunnel.apache.org/docs/connectors/sink
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的sql语句，您可以配置数据库名称表名以自动为您生成add语句

```
sink {
    jdbc {
        url = "jdbc:oceanbase://localhost:2883/test"
        driver = "com.oceanbase.jdbc.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "mysql"
        # 根据数据库表名自动生成sql语句
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### 自动生成 SQL 并设置保存模式

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
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
        username = "root"
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

### Oracle 兼容模式

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/TESTUSER"
    driver = "com.oceanbase.jdbc.Driver"
    username = "TESTUSER@test"
    password = ""
    compatible_mode = "oracle"
    query = "INSERT INTO SINK_TABLE (ID, NAME, CREATE_TIME) VALUES (?, ?, ?)"
  }
}
```

### 多表写入

当上游数据带有表身份信息时，可以在 `table` 中使用占位符。

```
sink {
  jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "${table_name}_sink"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

### 基于 XA 事务的精确一次

启用 XA 精确一次语义需要把 `is_exactly_once = true`，并提供 OceanBase JDBC 驱动中的 `xa_data_source_class_name`，同时把 `max_retries = 0`。写入器会把每个 checkpoint 批次包装在 XA 事务里，要么与源 checkpoint 一起提交，要么失败回滚。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

sink {
  Jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.oceanbase.jdbc.OceanBaseXADataSource"
    max_retries = 0
    batch_size = 1000
  }
}
```

### 批量 + 定时刷新组合

流式作业可以同时设置 `batch_size` 与 `batch_interval_ms`，基于距离上次刷新的耗时来刷新缓冲行。刷新是**写入触发的**：每条记录进入写入路径时都会检查耗时，达到间隔才会同步刷新，并没有后台调度线程。因此在空闲（没有新记录）的时段，缓冲行会一直保留到下一条记录到达或下一个 checkpoint 完成——`batch_interval_ms` 自身并不能保证低吞吐量流上的精确 wall-clock 时延边界。配合 `batch_size` 使用可以兼顾吞吐与单条记录时延，但请不要把它当作严格的实时定时器。

```hocon
sink {
  Jdbc {
    url = "jdbc:oceanbase://localhost:2883/test"
    driver = "com.oceanbase.jdbc.Driver"
    username = "root"
    password = "123456"
    compatible_mode = "mysql"
    generate_sink_sql = true
    database = "test"
    table = "sink_table"
    primary_keys = ["id"]
    batch_size = 2000
    batch_interval_ms = 5000
  }
}
```

## 变更日志

<ChangeLog />
