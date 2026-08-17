import ChangeLog from '../changelog/connector-databend.md';

# Databend

> Databend sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于向 Databend 写入数据的 sink 连接器。支持批处理和流处理模式。
Databend sink 内部通过 stage attachment 实现数据的批量导入。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [Databend JDBC driver jar package](https://github.com/databendlabs/databend-jdbc/) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## 支持的数据源信息

为了使用 Databend 连接器，需要以下依赖项。它们可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源   | 支持的版本        | 依赖                                                                                   |
|----------|-------------------|----------------------------------------------------------------------------------------|
| Databend | 1.2.x 及以上版本  | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-databend) |

## Sink 选项

| 名称                  | 类型 | 是否必须 | 默认值 | 描述                                 |
|---------------------|------|----------|--------|------------------------------------|
| url                 | String | 是 | - | Databend JDBC 连接 URL，必须以 `jdbc:databend://` 开头 |
| username            | String | 是 | - | Databend 数据库用户名                    |
| password            | String | 是 | - | Databend 数据库密码                     |
| database            | String | 否 | - | Databend 数据库名称，默认使用连接 URL 中指定的数据库名 |
| table               | String | 否 | - | Databend 表名称                       |
| batch_size          | Integer | 否 | 1000 | 批量写入的记录数                           |
| auto_commit         | Boolean | 否 | true | 是否自动提交事务                           |
| max_retries         | Integer | 否 | 3 | 写入失败时的最大重试次数                       |
| schema_save_mode    | Enum | 否 | CREATE_SCHEMA_WHEN_NOT_EXIST | 保存 Schema 的模式                      |
| data_save_mode      | Enum | 否 | APPEND_DATA | 保存数据的模式                            |
| custom_sql          | String | 否 | - | 自定义写入 SQL，通常用于复杂的写入场景              |
| execute_timeout_sec | Integer | 否 | 300 | 执行SQL的超时时间（秒）                      |
| jdbc_config         | Map | 否 | - | 额外的 JDBC 连接配置，如连接超时参数等             |
| conflict_key        | String | 否 | - | cdc 模式下的冲突键，用于确定冲突解决的主键 |
| enable_delete       | Boolean | 否 | false | cdc 模式下是否允许删除操作 |
| common-options      |  | 否 | - | Sink 插件通用参数，详见 [Sink 常用选项](../common-options/sink-common-options.md)。 |

### schema_save_mode [Enum]

在开启同步任务之前，针对现有的表结构选择不同的处理方案。
选项介绍：  
`RECREATE_SCHEMA` ：表不存在时创建，表存在时删除并重建。  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时会创建，表存在时跳过。  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：表不存在时会报错。  
`IGNORE` ：忽略对表的处理。

### data_save_mode [Enum]

在开启同步任务之前，针对目标端已有的数据选择不同的处理方案。
选项介绍：  
`DROP_DATA`： 保留数据库结构并删除数据。  
`APPEND_DATA`：保留数据库结构，保留数据。  
`CUSTOM_PROCESSING`：用户自定义处理。  
`ERROR_WHEN_DATA_EXISTS`：有数据时报错。

## 数据类型映射

| SeaTunnel 数据类型 | Databend 数据类型 |
|-----------------|---------------|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| STRING | STRING |
| BYTES | VARBINARY |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |

## 任务示例

### 简单示例

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        name = string
        age = int
        score = double
      }
    }
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

### 使用自定义 SQL 写入

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    custom_sql = "INSERT INTO default.target_table(name, age, score) VALUES(?, ?, ?)"
  }
}
```

### 使用 Schema 保存模式

```hocon
sink {
  Databend {
    url = "jdbc:databend://localhost:8000"
    username = "root"
    password = ""
    database = "default"
    table = "target_table"
    schema_save_mode = "RECREATE_SCHEMA"
    data_save_mode = "APPEND_DATA"
  }
}
```

### CDC 模式

`conflict_key` 用来指定合并更新/删除事件的主键列。只有当 DELETE 事件需要删除
Databend 中的数据时，才需要设置 `enable_delete = true`。
如果不配置 `conflict_key`，sink 会按普通批量插入方式写入。

下面的端到端示例将 CDC 行类型（`INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE`）
写入 Databend。sink 会根据 `conflict_key` 指定的主键合并更新并执行删除。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 1000
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        id = "int"
        name = "string"
        position = "string"
        age = "int"
        score = "double"
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "Alice", "Engineer", 30, 95.5]
      },
      {
        kind = INSERT
        fields = [2, "Bob", "Developer", 25, 85.0]
      },
      {
        kind = UPDATE_BEFORE
        fields = [2, "Bob", "Developer", 25, 85.0]
      },
      {
        kind = UPDATE_AFTER
        fields = [2, "Bob", "Senior Developer", 25, 87.0]
      },
      {
        kind = DELETE
        fields = [2, "Bob", "Senior Developer", 25, 87.0]
      }
    ]
  }
}

sink {
  Databend {
    url = "jdbc:databend://databend:8000/default?ssl=false"
    username = "root"
    password = ""
    database = "default"
    table = "sink_table"

    # 开启 CDC 写入模式
    batch_size = 1
    conflict_key = "id"
    enable_delete = true
  }
}
```

### 将 MySQL CDC 流式写入 Databend

同一套 CDC 参数同样适用于流式任务。下面的示例将 MySQL CDC 事件持续写入 Databend。流式
CDC 场景下建议把 `batch_size` 调小一些，让每个 checkpoint 都能反映最新写入。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://mysql:3306/test"
    username = "root"
    password = "mysqlpw"
    table-names = ["test.orders"]
  }
}

sink {
  Databend {
    url = "jdbc:databend://databend:8000/default?ssl=false"
    username = "root"
    password = ""
    database = "default"
    table = "orders"
    batch_size = 500
    conflict_key = "id"
    enable_delete = true
  }
}
```

## 相关链接

- [Databend 官方网站](https://databend.rs/)
- [Databend JDBC 驱动](https://github.com/databendlabs/databend-jdbc/)

## Changelog

<ChangeLog />
