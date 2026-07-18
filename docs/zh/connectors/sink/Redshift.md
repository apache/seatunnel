import ChangeLog from '../changelog/connector-jdbc.md';

# Redshift

> JDBC Redshift 接收器连接器

## 支持的 Redshift 版本

- Amazon Redshift

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过 JDBC 写入数据。支持批处理模式和流模式，支持并发写入，支持精确一次语义（使用 XA 事务保证）。

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

> 使用 `Xa 事务` 确保 `精确一次`。因此仅支持 `精确一次` 的数据库才支持 `Xa 事务`。您可以设置 `is_exactly_once=true` 来启用它。

## 支持的数据源信息

| 数据源     |         支持的版本          |             驱动              |                   URL                   |                                       Maven                                        |
|------------|----------------------------------------------------------|---------------------------------|-----------------------------------------|------------------------------------------------------------------------------------|
| Redshift   | 不同的依赖版本有不同的驱动程序类 | com.amazon.redshift.jdbc.Driver | jdbc:redshift://localhost:5439/database | [下载](https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42) |

## 数据类型映射

|      SeaTunnel 数据类型       |  Redshift 数据类型  |
|--------------------------------|----------------------|
| BOOLEAN                        | BOOLEAN              |
| TINYINT<br/>SMALLINT           | SMALLINT             |
| INT                            | INTEGER              |
| BIGINT                         | BIGINT               |
| FLOAT                          | REAL                 |
| DOUBLE                         | DOUBLE PRECISION     |
| DECIMAL                        | NUMERIC              |
| STRING(<=65535)                | CHARACTER VARYING    |
| STRING(>65535)                 | SUPER                |
| BYTES                          | BINARY VARYING       |
| TIME                           | TIME                 |
| TIMESTAMP                      | TIMESTAMP            |
| MAP<br/>ARRAY<br/>ROW          | SUPER                |

## Sink 参数

> Redshift Sink 基于 JDBC Sink 实现。下表聚焦于 Redshift 常用参数。对于继承自 JDBC Sink 的高级参数，例如 `compatible_mode`、`dialect`、`is_primary_key_updated`、`support_upsert_by_insert_only`、`use_copy_statement`、`tablePrefix`、`tableSuffix` 和 `create_index`，请参考 [JDBC Sink](Jdbc.md)。

| 名称                           | 类型    | 是否必填 |           默认值            |                                                                                                                  描述                                                                                                                   |
|------------------------------|---------|----------|------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String  | 是      | -                            | JDBC 连接的 URL。参见示例：`jdbc:redshift://localhost:5439/mydatabase`                                                                                                                                                                        |
| driver                       | String  | 是      | -                            | 用于连接远程数据源的 JDBC 类名，值为 `com.amazon.redshift.jdbc.Driver`。                                                                                                                                                                     |
| username                     | String  | 否       | -                            | 连接实例用户名                                                                                                                                                                                                                                 |
| password                     | String  | 否       | -                            | 连接实例密码                                                                                                                                                                                                                                   |
| query                        | String  | 否       | -                            | 使用此 SQL 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 具有更高的优先级                                                                                                                                                               |
| database                     | String  | 否       | -                            | 使用此 `database` 和 `table-name` 自动生成 SQL 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，具有更高的优先级。                                                                                                                    |
| table                        | String  | 否       | -                            | 使用数据库和此表名自动生成 SQL 并接收上游输入数据写入数据库。<br/>此选项与 `query` 互斥，具有更高的优先级。                                                                                                                                    |
| schema                       | String  | 否       | -                            | Redshift 中目标表的 schema 名称。SeaTunnel 不会为该参数自动填充默认值。当目标表没有在 `table` 中显式携带 schema，或使用 `generate_sink_sql`、`schema_save_mode`、`data_save_mode` 等基于 catalog 的操作时，建议显式配置该参数。 |
| primary_keys                 | Array   | 否       | -                            | 此选项用于支持自动生成 SQL 时的 `insert`、`delete` 和 `update` 操作。                                                                                                                                                                          |
| connection_check_timeout_sec | Int     | 否       | 30                           | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                                                                                 |
| max_retries                  | Int     | 否       | 0                            | 提交失败的重试次数（executeBatch）                                                                                                                                                                                                             |
| batch_size                   | Int     | 否       | 1000                         | 对于批量写入，当缓冲记录数量达到 `batch_size` 的数量或时间达到 `checkpoint.interval` 时，<br/>数据将被刷新到数据库中                                                                                                                          |
| is_exactly_once              | Boolean | 否       | false                        | 是否启用精确一次语义，将使用 Xa 事务。如果启用，则需要设置 `xa_data_source_class_name`。                                                                                                                                                       |
| generate_sink_sql            | Boolean | 否       | false                        | 根据要写入的数据库表生成 SQL 语句                                                                                                                                                                                                              |
| xa_data_source_class_name    | String  | 否       | -                            | 数据库驱动的 XA 数据源类名，Redshift 为 `com.amazon.redshift.xa.RedshiftXADataSource`                                                                                                                                                         |
| max_commit_attempts          | Int     | 否       | 3                            | 事务提交失败的重试次数                                                                                                                                                                                                                         |
| transaction_timeout_sec      | Int     | 否       | -1                           | 事务打开后的超时时间，默认值为 -1（永不超时）。注意设置超时可能会影响精确一次语义                                                                                                                                                                 |
| auto_commit                  | Boolean | 否       | true                         | 默认启用自动事务提交                                                                                                                                                                                                                           |
| field_ide                    | String  | 否       | -                            | 确定从源同步到 Sink 时是否需要转换字段。`ORIGINAL` 表示不需要转换；`UPPERCASE` 表示转换为大写；`LOWERCASE` 表示转换为小写。                                                                                                                     |
| properties                   | Map     | 否       | -                            | 其他连接配置参数，当属性和 URL 具有相同的参数时，优先级由驱动程序的特定实现决定。                                                                                                                                                                |
| common-options               |         | 否       | -                            | Sink 插件常用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详情                                                                                                                                              |
| schema_save_mode             | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | 在启动同步任务之前，对目标端已有的表结构选择不同的处理方案。                                                                                                                                                                                     |
| data_save_mode               | Enum    | 否       | APPEND_DATA                  | 在启动同步任务之前，对目标端已有的数据选择不同的处理方案。                                                                                                                                                                                       |
| custom_sql                   | String  | 否       | -                            | 当 data_save_mode 选择 CUSTOM_PROCESSING 时，您需要填写 CUSTOM_SQL 参数。此参数通常填写一个可执行的 SQL，该 SQL 将在同步任务之前执行。                                                                                                          |
| enable_upsert                | Boolean | 否       | true                         | 通过 primary_keys 启用 upsert，如果任务只有 `insert`，将此参数设置为 `false` 可以加快数据导入                                                                                                                                                   |
| multi_table_sink_replica     | Int     | 否       | 1                            | 多表写入的副本数，当 `multi_table_sink_replica > 1` 时，数据将并行写入多个表                                                                                                                                                                    |

## 任务示例

### 简单示例

> 此示例定义了一个 SeaTunnel 同步任务，该任务通过 FakeSource 自动生成数据并将其发送到 JDBC Sink。FakeSource 总共生成 16 行数据（row.num=16），每行有两个字段：name（字符串类型）和 age（int 类型）。最终目标表 test_table 中也将有 16 行数据。在运行此作业之前，您需要在 Redshift 中创建数据库和表 test_table。如果您尚未安装和部署 SeaTunnel，请按照[安装 SeaTunnel](../../getting-started/locally/deployment.md) 中的说明进行安装和部署，然后按照[快速启动 SeaTunnel 引擎](../../getting-started/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业。

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
}

transform {
}

sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        query = "insert into test_table(name,age) values(?,?)"
    }
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的 SQL 语句，您可以配置数据库名和表名来自动为您生成插入语句

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        generate_sink_sql = true
        database = mydatabase
        schema = "public"
        table = test_table
    }
}
```

### 精确一次

> 对于需要精确写入的场景，我们保证精确一次

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        max_retries = 0
        username = "myUser"
        password = "myPassword"
        query = "insert into test_table(name,age) values(?,?)"
        is_exactly_once = "true"
        xa_data_source_class_name = "com.amazon.redshift.xa.RedshiftXADataSource"
    }
}
```

### CDC（变更数据捕获）事件

> 我们也支持 CDC 变更数据。在这种情况下，您需要配置数据库、表和主键。

```
sink {
    jdbc {
        url = "jdbc:redshift://localhost:5439/mydatabase"
        driver = "com.amazon.redshift.jdbc.Driver"
        username = "myUser"
        password = "myPassword"
        generate_sink_sql = true
        database = mydatabase
        schema = "public"
        table = sink_table
        primary_keys = ["id","name"]
        field_ide = UPPERCASE
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

### 多表同步

#### 示例1：CDC 多表同步到 Redshift

> 通过 CDC 源同步多张表到目标 Redshift 数据库，使用占位符实现动态表名映射

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role","seatunnel.user","seatunnel.order"]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:redshift://redshift-cluster.xxxx.region.redshift.amazonaws.com:5439/mydatabase"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "myUser"
    password = "myPassword"
    generate_sink_sql = true
    database = mydatabase
    schema = "public"
    table = "${table_name}_sink"
    primary_keys = ["${primary_key}"]
  }
}
```

#### 示例2：JDBC Source 多表同步到 Redshift

> 从数据库使用 JDBC Source 批量同步多张表到 Redshift

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = com.mysql.cj.jdbc.Driver
    url = "jdbc:mysql://localhost:3306/source_db"
    username = "root"
    password = "123456"
    table_list = [
      {
        table_path = "source_db.table_1"
      },
      {
        table_path = "source_db.table_2"
      }
    ]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:redshift://redshift-cluster.xxxx.region.redshift.amazonaws.com:5439/mydatabase"
    driver = "com.amazon.redshift.jdbc.Driver"
    username = "myUser"
    password = "myPassword"
    generate_sink_sql = true
    database = mydatabase
    schema = "public"
    table = "${table_name}_copy"
    primary_keys = ["${primary_key}"]
  }
}
```

## 变更日志

<ChangeLog />
