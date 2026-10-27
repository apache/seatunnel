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
| table_list | List | 否 | - | 一次读取多张表，每一项包含 `table_path`；生成的分片会分配给读取器。 |
| split.size | Int | 否 | 8096 | 动态切分规划器使用的每个分片目标行数。 |
| split.even-distribution.factor.lower-bound | Double | 否 | 0.05 | 用于选择均匀切分策略的数据分布因子下界。 |
| split.even-distribution.factor.upper-bound | Double | 否 | 100.0 | 用于选择均匀切分策略的数据分布因子上界。 |
| split.sample-sharding.threshold | Int | 否 | 1000 | 数据分布不均匀时，触发采样切分策略的预估分片数阈值。 |
| split.inverse-sampling.rate | Int | 否 | 1000 | 采样率的分母；`1000` 表示采样率为 `1/1000`。 |
| partition_column | String | 否 | - | 旧版分区切分使用的列；支持的类型和切分键选择规则见 JDBC 源文档。 |
| partition_upper_bound | Long | 否 | - | 分区列的上界。已知范围时显式设置，可避免耗时的 `MAX()` 探测。 |
| partition_lower_bound | Long | 否 | - | 分区列的下界。已知范围时显式设置，可避免耗时的 `MIN()` 探测。 |
| fetch_size | Int | 否 | 0 | JDBC fetch size，`0` 表示由驱动决定。大字段或宽表场景下适当调大。 |
| common-options | Config | 否 | - | 源连接器通用配置，见 [源通用选项](../common-options/source-common-options.md)。 |

## 并行读取

Cloudberry 使用 JDBC 源的切分规划器。切分键、查询/表配置以及 `enable_concurrent_read` 决定一张表
是否可以切分。枚举器根据分片 ID 把生成的分片分配给读取器；`table_list` 不保证每个读取器只负责一张表。

切分策略和配置要求请参考 [JDBC 源连接器文档](../source/Jdbc.md#并行读取)。

## 注意事项

- Cloudberry 作业使用 `Jdbc` 插件名。
- Cloudberry 使用 PostgreSQL JDBC 驱动和兼容实现，请配置 `driver = "org.postgresql.Driver"`。
- URL 使用 PostgreSQL 风格，例如 `jdbc:postgresql://host:5432/database`。
- 不要把真实数据库密码写进共享示例、日志或截图。
- JDBC 源执行有界读取，不会定期轮询查询或消费逻辑复制槽。持续变更捕获应使用明确支持对应上游数据库的 CDC 连接器。
- 对超大规模的并行扫描，建议显式设置 `partition_lower_bound` / `partition_upper_bound` 而不是让连接器
  执行 `MIN()` / `MAX()`。探测成本取决于表数据和索引。

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

### 通过查询过滤进行有界读取

该过滤条件只限制本次批任务的结果集。任务执行一次有界读取，不会重复运行查询，也不会保存供下次任务使用的增量水位。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
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
