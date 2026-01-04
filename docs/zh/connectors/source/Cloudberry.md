import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cludberry源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 使用依赖关系

### 适用于 Spark/Flink 引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.postgresql/postgresql)已放置在目录`${SEATUNNEL_HOME}/plugins/`中。

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/org.postgresql/postgresql)已放置在目录`${SEATUNNEL_HOME}/lib/`中。

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列映射](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../concept/connector-v2-features.md)

> 支持查询SQL，可以实现映射效果。

## 描述

通过 JDBC 读取外部数据源的数据。Cloudberry 暂未提供原生 JDBC 的驱动，需使用 PostgreSQL的 驱动程序和实现。

## 支持的数据源信息

| 数据源     | 支持的版本               | 驱动程序                | URL                                     | Maven                                                        |
| :--------- | :----------------------- | :---------------------- | :-------------------------------------- | :----------------------------------------------------------- |
| Cloudberry | 使用 PostgreSQL 驱动实现 | `org.postgresql.Driver` | `jdbc:postgresql://localhost:5432/test` | [下载](https://mvnrepository.com/artifact/org.postgresql/postgresql) |

## 数据库相关性

> 请下载PostgreSQL驱动程序的jar包，并将其复制到`${SEATUNNEL_HOME}/plugins/jdbc/lib/`工作目录下。<br/>
> 例如：`cp postgresql-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/`

## 数据类型映射

Cloudberry 使用 PostgreSQL 的数据类型实现。有关数据类型的兼容性和映射关系，请参考 PostgreSQL 文档。

## 配置项

Cloudberry 连接器使用与 PostgreSQL 相同的配置项。有关详细的配置选项，请参考 PostgreSQL 连接器文档。

关键配置项包括：

- url (必需): JDBC 连接 URL。
- driver (必需): 驱动程序类名 (org.postgresql.Driver)。
- user/password: 认证凭据。
- query or table_path: 要读取的数据。
- 用于并行读取的分区选项。

## 并行读取

Cloudberry 支持与 PostgreSQL 连接器相同的并行读取规则。有关切片策略和并行读取选项的详细信息，请参考 PostgreSQL 连接器文档。

## 任务示例

### 简单

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "select * from mytable limit 100"
  }
}

sink {
  Console {}
}
```

### 使用 table_path 进行并行读取

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    table_path = "public.mytable"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### 读取多张表

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    "table_list" = [
      {
        "table_path" = "public.table1"
      },
      {
        "table_path" = "public.table2"
      }
    ]
    split.size = 10000
  }
}

sink {
  Console {}
}
```

有关更详细的示例和配置，请参阅PostgreSQL连接器文档。

## 变更日志

<ChangeLog />