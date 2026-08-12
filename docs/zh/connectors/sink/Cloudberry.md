import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 将数据写入 Cloudberry。Cloudberry 暂未提供自己的 JDBC 驱动，因此本连接器
使用官方 PostgreSQL 驱动（`org.postgresql.Driver`），配置模型与
[PostgreSQL Sink 连接器](./PostgreSql.md) 一致。

支持批处理和流模式、并发写入以及精确一次语义（通过 XA 事务保证）。在配置了
`primary_keys` 和 `generate_sink_sql` 时，也支持接收上游的 CDC 变更事件。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 将 [PostgreSQL JDBC 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 放到 `${SEATUNNEL_HOME}/plugins/` 目录。

### 对于 SeaTunnel Zeta 引擎

> 1. 将 [PostgreSQL JDBC 驱动 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 放到 `${SEATUNNEL_HOME}/lib/` 目录。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

> 使用 `XA 事务` 来确保 `精确一次`。因此，只有支持 `XA 事务` 的数据库才支持 `精确一次`。您可以设置 `is_exactly_once=true` 和 `max_retries=0` 来启用它。

## 支持的数据源信息

| 数据源     | 支持的版本                  | 驱动程序             | URL                                | Maven                                                                       |
|------------|------------------------------|----------------------|------------------------------------|-----------------------------------------------------------------------------|
| Cloudberry | 使用 PostgreSQL 驱动协议     | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql)        |

## 数据库依赖

> 将 PostgreSQL 驱动 jar 包放到 `$SEATUNNEL_HOME/plugins/jdbc/lib/` 目录下。
> 例如：`cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

Cloudberry 沿用 PostgreSQL 的数据类型实现。数据类型兼容性与映射关系请参考
[PostgreSQL 连接器文档](./PostgreSql.md#数据类型映射)。

## 选项

由于底层驱动一致，Cloudberry Sink 继承了 [PostgreSQL Sink 连接器](./PostgreSql.md) 的全部选项。
下表仅列出与通用 JDBC 选项存在差异的连接相关选项；其余选项请参考 PostgreSQL 页面。

| 名称                         | 类型    | 是否必填 | 默认值                          | 描述                                                                                                                                                                  |
|------------------------------|---------|----------|---------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String  | 是       | -                               | JDBC 连接 URL。使用 PostgreSQL 协议，例如 `jdbc:postgresql://localhost:5432/cloudberrydb`                                                                              |
| driver                       | String  | 是       | -                               | 固定为 `org.postgresql.Driver`                                                                                                                                        |
| username                     | String  | 是       | -                               | 连接实例的用户名。同时接受 `user` 作为 `username` 的别名                                                                                                                |
| password                     | String  | 是       | -                               | 连接实例的密码                                                                                                                                                        |
| query                        | String  | 否       | -                               | 使用此 SQL 将上游输入数据写入数据库，例如 `INSERT ...`；`query` 优先级更高                                                                                              |
| database                     | String  | 否       | -                               | 使用此 `database` 和 `table` 自动生成 SQL 并接收上游输入数据写入数据库                                                                                                |
| table                        | String  | 否       | -                               | 使用 `database` 和此 `table` 自动生成 SQL 并接收上游输入数据写入数据库                                                                                                 |
| primary_keys                 | Array   | 否       | -                               | 在自动生成支持 `insert`、`delete` 和 `update` 的 SQL 时必填                                                                                                            |
| is_exactly_once              | Boolean | 否       | false                           | 是否启用精确一次语义，通过 XA 事务保证                                                                                                                                  |
| xa_data_source_class_name    | String  | 否       | -                               | 当 `is_exactly_once=true` 时使用 `org.postgresql.xa.PGXADataSource`                                                                                                    |
| generate_sink_sql            | Boolean | 否       | false                           | 根据要写入的数据库表生成 SQL 语句                                                                                                                                      |
| batch_size                   | Int     | 否       | 1000                            | 单次刷新前缓冲的最大记录数                                                                                                                                            |
| batch_interval_ms            | Long    | 否       | 0                               | 写入触发的定时刷新间隔，单位毫秒                                                                                                                                      |
| schema_save_mode             | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST     | 同步任务启动前，控制目标表结构的处理方式                                                                                                                                |
| data_save_mode               | Enum    | 否       | APPEND_DATA                     | 同步任务启动前，控制目标表已有数据的处理方式                                                                                                                            |
| custom_sql                   | String  | 否       | -                               | 当 `data_save_mode` 为 `CUSTOM_PROCESSING` 时，同步任务启动前需要执行的 SQL                                                                                            |
| enable_upsert                | Boolean | 否       | true                            | 通过主键存在启用 upsert                                                                                                                                                |
| multi_table_sink_replica     | Int     | 否       | 1                               | 多表写入时使用的 Sink Writer 副本数量                                                                                                                                  |
| common-options               |         | 否       | -                               | 接收器插件通用参数，详情请参考 [Sink Common Options](../common-options/sink-common-options.md)                                                                          |

## 注意事项

- 请配置 `driver = "org.postgresql.Driver"`，并使用 `jdbc:postgresql://...` 形式的 URL。
- 作业中请使用 `Jdbc` 作为插件名；Cloudberry 连接器没有自己的工厂名。
- 需要手写 INSERT 语句时使用 `query`；希望 SeaTunnel 自动生成 SQL 时，使用 `generate_sink_sql`、`database` 和 `table`。
- `is_exactly_once=true` 还需要配置可用的 XA 数据源类，例如 `org.postgresql.xa.PGXADataSource`。

## 任务示例

### 简单示例

```hocon
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
}

sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"
    query = "insert into test_table(name, age) values(?, ?)"
  }
}
```

### 生成 Sink SQL

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
  }
}
```

### 精确一次

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"
    query = "insert into test_table(name, age) values(?, ?)"

    is_exactly_once = true
    xa_data_source_class_name = "org.postgresql.xa.PGXADataSource"
  }
}
```

### CDC（变更数据捕获）事件

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "sink_table"
    primary_keys = ["id", "name"]
    field_ide = UPPERCASE
  }
}
```

### 保存模式功能

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### 多表写入

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    username = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "${database_name}"
    table = "${table_name}"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

有关更多详细的示例和选项，请参考 [PostgreSQL 连接器文档](./PostgreSql.md)。

## 变更日志

<ChangeLog />