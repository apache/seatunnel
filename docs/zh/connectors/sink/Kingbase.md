import ChangeLog from '../changelog/connector-jdbc.md';

# Kingbase

> JDBC Kingbase Sink 连接器

## 支持连接器版本

- 8.6

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

> 使用 `XA 事务` 来确保 `精确一次`。因此仅支持支持 `XA 事务` 的数据库的 `精确一次`。您可以设置 `is_exactly_once=true` 来启用它。Kingbase 目前不支持

## 支持的数据源信息

| 数据源 | 支持的版本 |        驱动        |                   URL                    |                                             Maven                                              |
|--------|-----------|----------------------|------------------------------------------|------------------------------------------------------------------------------------------------|
| Kingbase   | 8.6                | com.kingbase8.Driver | jdbc:kingbase8://localhost:54321/db_test | [下载](https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/'
> 工作目录<br/>
> 例如：cp kingbase8-8.6.0.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|              Kingbase 数据类型              |                                                                SeaTunnel 数据类型                                                                |
|----------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL                                         | BOOLEAN                                                                                                                                           |
| INT2                                         | SHORT                                                                                                                                             |
| SMALLSERIAL                                  | SMALLINT                                                                                                                                          |
| SERIAL <br/>INT4                             | INT                                                                                                                                               |
| INT8 <br/>BIGSERIAL                          | BIGINT                                                                                                                                            |
| FLOAT4                                       | FLOAT                                                                                                                                             |
| FLOAT8                                       | DOUBLE                                                                                                                                            |
| NUMERIC                                      | DECIMAL((获取指定列的指定列大小),<br/>(获取指定列小数点右边的位数。))) |
| BPCHAR <br/>CHARACTER <br/>VARCHAR <br/>TEXT | STRING                                                                                                                                            |
| TIMESTAMP                                    | TIMESTAMP                                                                                                                                         |
| TIME                                         | TIME                                                                                                                                              |
| DATE                                         | DATE                                                                                                                                              |
| 其他数据类型                              | 支持。未列出的类型遵循 PostgreSQL 的转换规则，同时支持 MySQL、Oracle、SQL Server 兼容类型（如 TINYINT、MONEY、BLOB、CLOB、DATETIME、NUMBER、DATETIME2 等） |

## Sink 选项

|                   参数名                    |  类型   | 必须 | 默认值 |                                                                                                                 描述                                                                                                                  |
|-------------------------------------------|---------|------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String  | 是   | -       | JDBC 连接的 URL。参考示例：jdbc:kingbase8://localhost:54321/db_test                                                                                                                                                           |
| driver                                    | String  | 是   | -       | 用于连接到远程数据源的 jdbc 类名，<br/> 如果使用 Kingbase，则值为 `com.kingbase8.Driver`。                                                                                                            |
| username                                      | String  | 否   | -       | 连接实例用户名。旧配置名 `user` 仍可作为兼容写法使用。                                                                                                                                                                                                                |
| password                                  | String  | 否   | -       | 连接实例密码                                                                                                                                                                                                                 |
| query                                     | String  | 否   | -       | 使用此 sql 将上游输入数据写入数据库。例如 `INSERT ...`，`query` 具有更高的优先级                                                                                                                                       |
| database                                  | String  | 否   | -       | 使用此 `database` 和 `table-name` 自动生成 sql 并接收上游输入数据写入数据库。<br/>仅当 `generate_sink_sql = true` 时用于自动生成 SQL；设置 `query` 时以 `query` 为准                                                     |
| table                                     | String  | 否   | -       | 使用数据库和此 table-name 自动生成 sql 并接收上游输入数据写入数据库。<br/>仅当 `generate_sink_sql = true` 时用于自动生成 SQL；设置 `query` 时以 `query` 为准                                                         |
| primary_keys                              | Array   | 否   | -       | 此选项用于在自动生成 sql 时支持 `insert`、`delete` 和 `update` 等操作。                                                                                                                          |
| connection_check_timeout_sec              | Int     | 否   | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                          |
| max_retries                               | Int     | 否   | 0       | 提交失败的重试次数 (executeBatch)                                                                                                                                                                                        |
| batch_size                                | Int     | 否   | 1000    | 对于批量写入，当缓冲记录数达到 `batch_size` 数量或时间达到 `checkpoint.interval` 时<br/>，数据将被刷新到数据库                                                         |
| batch_interval_ms                         | Long    | 否   | 0       | 写入触发的定时刷新间隔，单位毫秒。`0` 表示关闭定时刷新；大于 `0` 时，每次写入会检查距离上次刷新是否已超过该间隔，超过则同步刷新。 |
| is_exactly_once                           | Boolean | 否   | false   | 是否启用精确一次语义，这将使用 Xa 事务。如果启用，您需要<br/>设置 `xa_data_source_class_name`。Kingbase 目前不支持                                                                        |
| generate_sink_sql                         | Boolean | 否   | false   | 根据您要写入的数据库表生成 sql 语句                                                                                                                                                                     |
| xa_data_source_class_name                 | String  | 否   | -       | 数据库驱动程序的 xa 数据源类名，Kingbase 目前不支持                                                                                                                                                     |
| max_commit_attempts                       | Int     | 否   | 3       | 事务提交失败的重试次数                                                                                                                                                                                        |
| transaction_timeout_sec                   | Int     | 否   | -1      | 事务打开后的超时时间，默认为 -1（永不超时）。请注意，设置超时可能会影响<br/>精确一次语义                                                                                          |
| auto_commit                               | Boolean | 否   | true    | 默认启用自动事务提交                                                                                                                                                                           |
| common-options                            |         | 否   | -       | Sink 插件通用参数，请参考 [Sink 通用选项](../common-options/sink-common-options.md) 详见                                                                                                                                  |
| enable_upsert                             | Boolean | 否   | true    | 如果存在 primary_keys，启用 upsert。如果任务没有重复数据，将此参数设置为 `false` 可以加快数据导入                                                                                                       |
| compatible_mode                           | String  | 否   | -       | KingbaseES 服务端的兼容模式。当 KingbaseES 以 MySQL 兼容模式运行时，将其设为 `mysql`，连接器会使用 MySQL 风格的类型映射、行转换和 `ON DUPLICATE KEY UPDATE` 形式的 upsert；不设置时保持原生 Kingbase/PostgreSQL 行为。同时接受 `compatibleMode` 作为备选键名。 |

### 提示

> 1. 如果未设置 partition_column，它将以单并发运行，如果设置了 partition_column，它将根据任务的并发性并行执行。
> 2. 当 KingbaseES 以 MySQL 兼容模式运行时，请设置 `compatible_mode = "mysql"` 以复用现有的 Kingbase 连接器配置：连接器会使用 MySQL 风格的类型映射、行转换，以及 `ON DUPLICATE KEY UPDATE` 形式的 upsert。本改动没有引入新的选项名，`compatible_mode` 原本就已存在（供其他多模式数据库使用）。
> 3. 兼容模式**只从配置读取**：**选择**兼容模式时不会建立连接，也不会探测服务端。未设置或无法识别的模式会确定性地保持原生 Kingbase/PostgreSQL 行为，而不会去“猜”一个模式，也不会在瞬时连接故障后静默降级。这一承诺只覆盖模式选择本身，连接器照常建立其正常连接：设置 `compatible_mode = "mysql"` 后，表结构发现使用 MySQL catalog，它会像原生 Kingbase catalog 一样连接数据库并读取元数据（包括查询服务端版本）。
> 4. 未设置 `compatible_mode` 的现有作业行为不变，也不会新增规划阶段的连接。已经设置 `compatible_mode = "mysql"` 的作业在本版本中行为会发生变化：MySQL 风格的转换、`ON DUPLICATE KEY UPDATE` 形式的 upsert 以及 MySQL catalog 元数据会取代原生实现，升级前请予以验证。详见[不向前兼容的更新](../../introduction/concepts/incompatible-changes.md)。

## 任务示例

### 简单

> 此示例定义了一个 SeaTunnel 同步任务，通过 FakeSource 自动生成数据并将其发送到 JDBC Sink。FakeSource 生成总共 16 行数据 (row.num=16)，每行有 12 个字段。最终目标表 test_table 也将有 16 行数据。
> 在运行此作业之前，您需要在 Kingbase 中创建数据库 test 和表 test_table。如果您还没有安装和部署 SeaTunnel，您需要按照 [安装 SeaTunnel](../../getting-started/locally/deployment.md) 中的说明进行安装和部署。然后按照 [使用 SeaTunnel 引擎快速开始](../../getting-started/locally/quick-start-seatunnel-engine.md) 中的说明运行此作业。

```
# 定义运行时环境
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  # 这是一个示例源插件 **仅用于测试和演示源插件功能**
  FakeSource {
    parallelism = 1
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(30, 8)"
            c_date = date
            c_time = time
            c_timestamp = timestamp
      }
    }
  }
  # 如果您想了解更多关于如何配置 seatunnel 和查看源插件的完整列表，
  # 请访问 https://seatunnel.apache.org/docs/connectors/source
}

transform {
  # 如果您想了解更多关于如何配置 seatunnel 和查看转换插件的完整列表，
    # 请访问 https://seatunnel.apache.org/docs/transforms
}

sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        username = "root"
        password = "123456"
        query = "insert into test_table(c_string,c_boolean,c_tinyint,c_smallint,c_int,c_bigint,c_float,c_double,c_decimal,c_date,c_time,c_timestamp) values(?,?,?,?,?,?,?,?,?,?,?,?)"
        }
  # 如果您想了解更多关于如何配置 seatunnel 和查看 sink 插件的完整列表，
  # 请访问 https://seatunnel.apache.org/docs/connectors/sink
}
```

### 生成 Sink SQL

> 此示例不需要编写复杂的 sql 语句，您可以配置数据库名称表名称来自动为您生成添加语句

```
sink {
    jdbc {
        url = "jdbc:kingbase8://127.0.0.1:54321/dbname"
        driver = "com.kingbase8.Driver"
        username = "root"
        password = "123456"
        # 根据数据库表名自动生成 sql 语句
        generate_sink_sql = true
        database = test
        table = test_table
    }
}
```

### 写入 Schema 表

> 使用自定义 `query` 写入时，占位符数量必须和上游字段数量一致。Kingbase 带 schema 的表可以写成 `public.table_name`。

```
sink {
  Jdbc {
    driver = "com.kingbase8.Driver"
    url = "jdbc:kingbase8://localhost:54321/test"
    user = "SYSTEM"
    password = "123456"
    query = "INSERT INTO public.e2e_table_sink (c1, c2, c3) VALUES (?, ?, ?)"
  }
}
```

## 变更日志

<ChangeLog />

