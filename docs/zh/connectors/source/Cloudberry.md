import ChangeLog from '../changelog/connector-cloudberry.md';

# Cloudberry

> JDBC Cloudberry 源连接器

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

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)

> 支持查询 SQL，可以实现列投影效果。

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

Cloudberry 是 JDBC Source 中 PostgreSQL 方言的轻量封装。作业仍然使用 `Jdbc` 插件名，只有连接参数与
PostgreSQL 略有差异。下表中的每一个选项都和 PostgreSQL/JDBC 源的同名选项完全一致，详细说明、合法取值和
默认行为请参考 [PostgreSQL 源连接器文档](../source/PostgreSQL.md) 以及共享的 [JDBC 源选项](../source/Jdbc.md)。

关键配置项包括：

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| url | String | 是 | - | JDBC 连接 URL。Cloudberry 使用 PostgreSQL 线协议，请使用 `jdbc:postgresql://host:port/database` 形式的 URL。 |
| driver | String | 是 | - | JDBC 驱动类名，恒为 `org.postgresql.Driver`。 |
| user / username | String | 是 | - | 数据库登录名。`user` 也是 `username` 的兼容写法。 |
| password | String | 是 | - | 数据库密码。生产环境请使用密钥管理或环境变量注入，不要把明文密码写入作业配置。 |
| query | String | 条件必填 | - | 驱动读取的 SELECT 语句。在不使用 `table_path` 或 `table_list` 时必须填写。 |
| table_path | String | 条件必填 | - | 要读取的 `schema.table`。在不使用 `query` 时必须填写。 |
| table_list | List | 否 | - | 一次读取多张表，每一项是 `{ table_path = "schema.table" }`。设置后并行度会在表之间拆分。 |
| split.size | Int | 否 | 8096 | 每个拆分的目标行数。仅在设置了 `partition_column` 时生效。 |
| split.even_partition_num | Boolean | 否 | false | 是否强制让分区数平均；否则交给优化器自行决定。 |
| split.sample_shard_threshold | Int | 否 | 1000 | 计算分区数时用于采样的行数阈值。 |
| split.inverse_parallelism | Int | 否 | 1 | 拆分数与并行度的换算系数，数值越大拆分越细。 |
| partition_column | String | 否 | - | 用于分区的数值列，建议为单调递增或唯一列（如 `id`、`created_at`）。 |
| partition_upper_bound | Long | 否 | - | 分区列的上界。已知范围时显式设置，可避免耗时的 `MAX()` 探测。 |
| partition_lower_bound | Long | 否 | - | 分区列的下界。已知范围时显式设置，可避免耗时的 `MIN()` 探测。 |
| fetch_size | Int | 否 | 0 | JDBC fetch size，`0` 表示由驱动决定。大字段或宽表场景下适当调大。 |
| common-options | Config | 否 | - | 源连接器通用配置，见 [源通用选项](../common-options/source-common-options.md)。 |

## 并行读取

Cloudberry 沿用 JDBC 源的 PostgreSQL 并行读取策略：

- **未设置分区列** —— 连接器在单个 SubTask 上执行 `query`。适合查询本身已经聚合或数据量较小的场景。
- **数值型 `partition_column`** —— 连接器探测 `MIN()` / `MAX()`（如果显式设置了
  `partition_lower_bound` / `partition_upper_bound` 则直接使用），然后把每个拆分包装成
  `WHERE partition_column BETWEEN ? AND ?` 子查询并行执行。这是大事实表的典型模式。
- **`table_list`** —— 当并行度 ≥ 表数量时，每个 SubTask 负责一张表；否则按上述分区策略继续拆分每张表。

完整的拆分策略说明和边界场景（例如非数值分区列、并行 `UPDATE` 快照）请参考
[PostgreSQL 源连接器文档](../source/PostgreSQL.md)。

## 注意事项

- Cloudberry 作业使用 `Jdbc` 插件名。
- Cloudberry 使用 PostgreSQL JDBC 驱动和兼容实现，请配置 `driver = "org.postgresql.Driver"`。
- URL 使用 PostgreSQL 风格，例如 `jdbc:postgresql://host:5432/database`。
- 不要把真实数据库密码写进共享示例、日志或截图。
- Cloudberry 暂未提供 CDC 接入能力。如需变更数据捕获，请使用对应上游数据库的专用 CDC 源，或在数据库侧建立
  逻辑复制槽并通过 JDBC 源消费。
- 对超大规模的并行扫描，建议显式设置 `partition_lower_bound` / `partition_upper_bound` 而不是让连接器
  执行 `MIN()` / `MAX()`。探测查询往往是作业启动最慢的环节。

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
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 1000000
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

### 基于查询的流式拉取（单 SubTask）

持续接入（例如轮询 `WHERE updated_at > now() - interval '5 minutes'`）时，把 `parallelism` 设为 1，
由数据库侧过滤即可。查询必须保证幂等并显式 `ORDER BY` 一个稳定的列。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/cloudberrydb"
    driver = "org.postgresql.Driver"
    user = "dbadmin"
    password = "password"
    query = "select id, payload, updated_at from events where updated_at > now() - interval '5 minutes' order by updated_at"
  }
}

sink {
  Console {}
}
```

有关更详细的示例和配置，请参阅 [PostgreSQL 源连接器文档](../source/PostgreSQL.md) 以及共享的 [JDBC 源选项](../source/Jdbc.md)。

## 变更日志

<ChangeLog />
