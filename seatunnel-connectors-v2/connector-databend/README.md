# Databend Connector

此连接器支持从 [Databend](https://databend.rs/) 中读取数据和向 Databend 写入数据。

## 依赖项

为了设置 Databend 连接器，必须在 SeaTunnel 的目录结构下的 `plugin_config` 文件中包含以下格式的依赖项：

```
seatunnel-connectors-v2/connector-databend
```

## 源连接器 (Source)

### 支持的数据类型映射

| Databend 数据类型 | SeaTunnel 数据类型                 |
|---------------|-----------------------------------|
| BOOLEAN       | BOOLEAN                           |
| TINYINT       | TINYINT                           |
| SMALLINT      | SMALLINT                          |
| INT           | INT                               |
| BIGINT        | BIGINT                            |
| FLOAT         | FLOAT                             |
| DOUBLE        | DOUBLE                            |
| DECIMAL       | DECIMAL                           |
| STRING        | STRING                            |
| VARCHAR       | STRING                            |
| CHAR          | STRING                            |
| TIMESTAMP     | TIMESTAMP                         |
| DATE          | DATE                              |
| TIME          | TIME                              |
| BINARY        | BYTES                             |

### 源选项 (Source Options)

| 名称         | 类型    | 必需  | 默认值   | 描述                                           |
|------------|-------|-----|-------|----------------------------------------------|
| url        | String | 是   | -     | Databend JDBC 连接 URL                        |
| username   | String | 是   | -     | Databend 数据库用户名                             |
| password   | String | 是   | -     | Databend 数据库密码                              |
| database   | String | 否   | -     | Databend 数据库名称，默认使用连接 URL 中指定的数据库名         |
| table      | String | 否   | -     | Databend 表名称                                |
| query      | String | 否   | -     | Databend 查询语句，如果设置将覆盖 database 和 table 的设置 |
| fetch_size | Integer | 否   | 0     | 一次从数据库中获取的记录数，设置为0使用JDBC驱动默认值            |
| jdbc_config | Map | 否   | -     | 额外的 JDBC 连接配置，如加载均衡策略等                     |

### 示例：

从 Databend 数据库中的一个表读取数据：

```hocon
source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "users"
  }
}
```

使用自定义 SQL 查询：

```hocon
source {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    query = "SELECT id, name, age FROM default.users WHERE age > 18"
  }
}
```

## 目标连接器 (Sink)

### 支持的数据类型映射

| SeaTunnel 数据类型 | Databend 数据类型 |
|-----------------|---------------|
| BOOLEAN         | BOOLEAN       |
| TINYINT         | TINYINT       |
| SMALLINT        | SMALLINT      |
| INT             | INT           |
| BIGINT          | BIGINT        |
| FLOAT           | FLOAT         |
| DOUBLE          | DOUBLE        |
| DECIMAL         | DECIMAL       |
| STRING          | STRING        |
| BYTES           | VARBINARY     |
| DATE            | DATE          |
| TIME            | TIME          |
| TIMESTAMP       | TIMESTAMP     |

### 目标选项 (Sink Options)

| 名称                | 类型      | 必需  | 默认值                     | 描述                                   |
|-------------------|---------|-----|-------------------------|--------------------------------------|
| url               | String  | 是   | -                       | Databend JDBC 连接 URL                |
| username          | String  | 是   | -                       | Databend 数据库用户名                     |
| password          | String  | 是   | -                       | Databend 数据库密码                      |
| database          | String  | 否   | -                       | Databend 数据库名称，默认使用连接 URL 中指定的数据库名 |
| table             | String  | 否   | -                       | Databend 表名称                        |
| batch_size        | Integer | 否   | 1000                    | 批量写入的记录数                           |
| auto_commit       | Boolean | 否   | true                    | 是否自动提交事务                           |
| max_retries       | Integer | 否   | 3                       | 写入失败时的最大重试次数                       |
| schema_save_mode  | Enum    | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST | 保存 Schema 的模式                      |
| data_save_mode    | Enum    | 否   | APPEND_DATA             | 保存数据的模式                            |
| custom_sql        | String  | 否   | -                       | 自定义写入 SQL，通常用于复杂的写入场景              |
| execute_timeout_sec | Integer | 否   | 300                     | 执行SQL的超时时间（秒）                      |
| jdbc_config        | Map    | 否   | -                       | 额外的 JDBC 连接配置，如加载均衡策略等             |

### 示例：

向 Databend 写入数据：

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default" 
    table = "target_table"
    batch_size = 1000
  }
}
```

使用自定义 SQL 写入：

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root" 
    password = ""
    database = "default"
    table = "target_table"
    custom_sql = "INSERT INTO default.target_table(id, name, age) VALUES(?, ?, ?)"
  }
}
```

## 完整示例

### 从 MySQL 同步数据到 Databend

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    query = "select * from source_table"
  }
}

sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    batch_size = 1000
  }
}
```

## 相关 Databend 文档链接

- [Databend 官方网站](https://databend.rs/)
- [Databend JDBC 驱动](https://github.com/databendlabs/databend-jdbc/)
