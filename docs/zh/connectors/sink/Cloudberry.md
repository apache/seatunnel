import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 写入数据。Cloudberry 目前没有自己的原生驱动程序。它使用 PostgreSQL 的驱动程序进行连接，并遵循 PostgreSQL 的实现。

支持批处理模式和流模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.postgresql/postgresql) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

> 使用 `XA 事务` 来确保 `精确一次`。因此，只有支持 `XA 事务` 的数据库才支持 `精确一次`。您可以设置 `is_exactly_once=true` 来启用它。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动程序 | URL | Maven |
|--------|-----------|---------|-----|-------|
| Cloudberry | 使用 PostgreSQL 驱动程序实现 | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |

## 数据库依赖

> 请下载 PostgreSQL 驱动程序 jar 并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如：cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

Cloudberry 使用 PostgreSQL 的数据类型实现。请参考 PostgreSQL 文档了解数据类型兼容性和映射。

## 选项

Cloudberry 连接器使用与 PostgreSQL 相同的选项。有关详细的配置选项，请参考 PostgreSQL 文档。

关键选项包括：
- url（必需）：JDBC 连接 URL
- driver（必需）：驱动程序类名（org.postgresql.Driver）
- user/password：身份验证凭证
- query 或 database/table 组合：要写入的数据和方式
- is_exactly_once：使用 XA 事务启用精确一次语义
- batch_size：控制批量写入行为

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
    user = "dbadmin"
    password = "password"
    query = "insert into test_table(name,age) values(?,?)"
  }
}
```

### 生成 Sink SQL

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
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
    user = "dbadmin"
    password = "password"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = "true"
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
    user = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "sink_table"
    primary_keys = ["id","name"]
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
    user = "dbadmin"
    password = "password"

    generate_sink_sql = true
    database = "mydb"
    table = "public.test_table"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

有关更多详细的示例和选项，请参考 PostgreSQL 连接器文档。

## 变更日志

<ChangeLog />
