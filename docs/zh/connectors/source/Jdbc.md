import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

## 描述

JDBC Source 通过数据库厂商提供的 JDBC 驱动读取表或自定义查询结果，支持列投影、行过滤、并行快照读取，以及在一个 Source 配置中读取多张表。

JDBC Source 是有界数据源：它读取数据库查询当前可见的数据后结束。如果任务需要持续捕获后续的新增、更新和删除，请使用对应的 CDC Connector。

第一次配置 JDBC Source 时，请先阅读[选择读取模式](#选择读取模式)和[快速入门](#快速入门postgresql)，再按需查阅后面的完整参数说明。

## 使用依赖

先安装 `connector-jdbc` 插件：

```plugin_config
--seatunnel-connectors--
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
```

不同数据库厂商的 JDBC 驱动具有不同的许可证和再分发条款，而且驱动版本还必须同时兼容数据库和 Java 运行时，因此 SeaTunnel 不会统一内置所有 JDBC 驱动。请自行下载合适的驱动，并在启动任务前把 JAR 放入对应引擎的目录。

### Spark 和 Flink 引擎

把 JDBC 驱动放到每个 SeaTunnel 执行节点的 `${SEATUNNEL_HOME}/plugins/Jdbc/lib/`。

### Zeta 引擎

把 JDBC 驱动放到每个 SeaTunnel 节点的 `${SEATUNNEL_HOME}/lib/`，然后重启受影响的 SeaTunnel 进程，让驱动进入类路径。

常用驱动类名和下载地址见[驱动参考](#驱动参考)。

## 选择读取模式

配置并行读取前，先选择单表或多表布局。单表布局中，`table_path` 和 `query` 可以单独使用，也可以组合使用；多表 `table_list` 与顶层 `table_path`、`query` 互斥。

| 使用场景 | 配置方式 | 行为 |
|----------|----------|------|
| 读取单表，并自动发现 schema 和动态分片 | `table_path` | 推荐用于整表快照。SeaTunnel 读取表元数据；表存在可用分片键时，通过 `split.size` 控制分片大小。 |
| 自定义读取列、JOIN 或数据库表达式 | `query`，可以同时配置 `table_path` | SeaTunnel 执行用户提供的 SQL。需要明确表身份和元数据时可同时配置 `table_path`。部分 JOIN 无法安全推断查询主键，详见 [Query 与主键注意事项](#query-与主键注意事项)。 |
| 读取多张表或按表名模式匹配 | `table_list` | 每一项可以配置 `table_path`、可选 `query` 和分片参数。使用 `table_list` 时，不能再配置顶层 `table_path` 或 `query`。 |

只有需要给所有选中表或查询追加同一过滤条件时才使用 `where_condition`。它必须以 `where` 开头，例如 `where updated_at >= '2026-01-01'`。

## 快速入门：PostgreSQL

下面从 PostgreSQL 读取三行数据，并通过 Console Sink 输出到 SeaTunnel 日志。

1. 按照[使用依赖](#使用依赖)的说明放置兼容版本的 PostgreSQL JDBC 驱动。

2. 使用 PostgreSQL 管理员账号连接已有的 `sales` 数据库，创建专用教程表，并为只读账号授权。如果 `seatunnel_reader` 已经存在，请省略 `CREATE ROLE` 并复用该账号：

```sql
CREATE ROLE seatunnel_reader WITH LOGIN PASSWORD 'change_me';

DROP TABLE IF EXISTS public.seatunnel_jdbc_source_quick_start;

CREATE TABLE public.seatunnel_jdbc_source_quick_start (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);

INSERT INTO public.seatunnel_jdbc_source_quick_start VALUES
  (1, 'Alice', 120.50),
  (2, 'Bob', 80.00),
  (3, 'Carol', 42.00);

GRANT CONNECT ON DATABASE sales TO seatunnel_reader;
GRANT USAGE ON SCHEMA public TO seatunnel_reader;
GRANT SELECT ON TABLE public.seatunnel_jdbc_source_quick_start TO seatunnel_reader;
```

`DROP TABLE` 用于保证教程数据可以重复初始化，请勿对业务表执行这条语句。

3. 把下面的任务保存为 `${SEATUNNEL_HOME}/config/jdbc-source-quick-start.conf`，并按实际环境替换主机、账号和数据库名。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:postgresql://postgresql.example.com:5432/sales"
    driver = "org.postgresql.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM public.seatunnel_jdbc_source_quick_start ORDER BY id"
  }
}

sink {
  Console {}
}
```

4. 运行任务：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-source-quick-start.conf -m local
```

5. 确认 Console Sink 输出包含以下字段值：

| id | customer_name | amount |
|----|---------------|-------:|
| 1 | Alice | 120.50 |
| 2 | Bob | 80.00 |
| 3 | Carol | 42.00 |

如果任务在读取数据前失败，请先检查[故障排查](#故障排查)。

:::note

连接 MariaDB 时，请使用 MariaDB Connector/J 以及匹配的 URL 和驱动：

```hocon
url = "jdbc:mariadb://localhost:3306/database"
driver = "org.mariadb.jdbc.Driver"
```

不要使用 MySQL Connector/J 和 `jdbc:mysql:` URL 连接 MariaDB。该配置会选择 MySQL 方言，可能将 MariaDB 服务端版本判定为不支持的 MySQL 版本。

:::

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

使用 `query` 可以只读取需要的列。

- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 选项

`url` 和 `driver` 始终必填。由于部分数据库允许无认证连接，账号和密码不是统一必填项。请选择顶层单表布局或 `table_list`；顶层 `table_path` 与 `query` 可以组合使用。账号建议使用规范键 `username`，历史配置中的 `user` 仍作为兼容键继续支持。

| 参数名                                       | 类型    | 必须   | 默认值   | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | 是    | -       | JDBC 连接的 URL。参考示例：jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | 是    | -       | 用于连接到远程数据源的 jdbc 类名，如果您使用 MySQL，值为 `com.mysql.cj.jdbc.Driver`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| username                                   | String  | 否    | -       | 数据库账号。历史配置键 `user` 仍作为兼容键支持。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                   | String  | 否    | -       | 数据库账号的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| query                                      | String  | 否    | -       | 要执行的 SQL。需要同时明确表身份和元数据时，可以与 `table_path` 组合使用。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| compatible_mode                            | String  | 否    | -       | 数据库的兼容模式，当数据库支持多种兼容模式时需要。<br/> 例如，使用 OceanBase 数据库时，需要将其设置为 'mysql' 或 'oracle'。<br/> 使用 starrocks 时，需要将其设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                             |
| dialect                                    | String  | 否    | -       | 指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。<br/> 例如，使用 starrocks 时，需要将其设置为 `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                                               |
| connection_check_timeout_sec               | Int     | 否    | 30      | 等待用于验证连接的数据库操作完成的时间（秒）。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| connect_timeout_ms                         | Int     | 否    | 86400000 | 建立 JDBC 连接时的连接超时时间，单位毫秒。`0` 表示不超时。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| socket_timeout_ms                          | Int     | 否    | 86400000 | JDBC 连接建立后的 socket 读取超时时间，单位毫秒。`0` 表示不超时。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_column                           | String  | 否    | -       | 用于分割数据的列名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | String  | 否    | -       | query 分区读取使用的闭区间上界。未配置时，SeaTunnel 会从源表查询最大值。对于 DATE 分区列，支持 ISO-8601 日期字符串（`yyyy-MM-dd`）或兼容历史配置的 epoch-day 数字。                                                                                                                                                                                                                                                                                                                                                                                                             |
| partition_lower_bound                      | String  | 否    | -       | query 分区读取使用的闭区间下界。未配置时，SeaTunnel 会从源表查询最小值。对于 DATE 分区列，支持 ISO-8601 日期字符串（`yyyy-MM-dd`）或兼容历史配置的 epoch-day 数字。                                                                                                                                                                                                                                                                                                                                                                                                             |
| partition_num                              | Int     | 否    | 10      | 顶层 `query` 与 `partition_column` 触发 fixed splitter 时的分片数，无论是否同时配置 `table_path`。动态 `table_path` 和 `table_list` 布局改用 `split.size`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| decimal_type_narrowing                     | Boolean | 否    | true    | 十进制类型缩小，如果为 true，十进制类型将缩小为 int 或 long 类型（如果没有精度损失）。目前仅支持 Oracle。请参考下面的 `decimal_type_narrowing`                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| int_type_narrowing                         | Boolean | 否    | true    | Int 类型缩小，如果为 true，tinyint(1) 类型将缩小为布尔类型（如果没有精度损失）。目前支持 MySQL。请参考下面的 `int_type_narrowing`                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| handle_blob_as_string                      | Boolean | 否    | false   | 如果为 true，BLOB 类型将转换为 STRING 类型。**仅支持 Oracle 数据库**。这对于处理超过默认大小限制的 Oracle 中的大 BLOB 字段很有用。将 Oracle 的 BLOB 字段传输到 Doris 等系统时，将其设置为 true 可以使数据传输更高效。                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_kerberos                               | Boolean | 否    | false   | 是否为 JDBC 连接启用 Kerberos 认证。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| kerberos_principal                         | String  | 否    | -       | `use_kerberos = true` 时使用的 Kerberos principal。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| kerberos_keytab_path                       | String  | 否    | -       | `use_kerberos = true` 时使用的 Kerberos keytab 文件路径。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| krb5_path                                  | String  | 否    | /etc/krb5.conf | Kerberos krb5 配置文件路径。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_select_count                           | Boolean | 否    | false   | 在动态块分割阶段使用 select count 来获取表计数，而不是其他方法。这目前仅适用于 jdbc-oracle。在这种情况下，当使用 sql 从分析表更新统计信息更快时，直接使用 select count                                                                                                                                                                                                                                                                                                                                                                                                     |
| skip_analyze                               | Boolean | 否    | false   | 在动态块分割阶段跳过表计数分析。这目前仅适用于 jdbc-oracle。在这种情况下，您定期安排分析表 sql 来更新相关表统计信息，或您的表数据不经常更改                                                                                                                                                                                                                                                                                                                                                                                                    |
| use_regex                                  | Boolean | 否    | false   | 控制 table_path 的正则表达式匹配。设置为 `true` 时，table_path 将被视为正则表达式模式。设置为 `false` 或未指定时，table_path 将被视为精确路径（无正则表达式匹配）。 |
| fetch_size                                 | Int     | 否    | 0       | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。                                                                                                                                                                                                                                                                                                                                                                                                               |
| properties                                 | Map     | 否    | -       | 其他连接配置参数，当 properties 和 URL 具有相同参数时，优先级由<br/>驱动程序的具体实现确定。例如，在 MySQL 中，properties 优先于 URL。                                                                                                                                                                                                                                                                                                                                                                                                     |
| table_path                                 | String  | 否    | -       | 表的完整路径。在单表布局中可以单独使用，也可以与 `query` 组合。<br/>示例：<br/>`- mysql: "testdb.table1" `<br/>`- oracle: "test_schema.table1" `<br/>`- sqlserver: "testdb.test_schema.table1"` <br/>`- postgresql: "testdb.test_schema.table1"`  <br/>`- iris: "test_schema.table1"`                                                                                                                                                                                                                                                                                                                                                                       |
| table_list                                 | Array   | 否    | -       | 要读取的表列表，您可以使用此配置代替 `table_path`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| where_condition                            | String  | 否    | -       | 所有表/查询的通用行过滤条件，必须以 `where` 开头。例如 `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.size                                 | Int     | 否    | 8096    | 使用动态分片时每个 split 的目标行数，适用于 `table_path` 和 `table_list` 布局；不控制顶层 `query` 与 `partition_column` 的 fixed partition 模式。                                                                                                                                                                                                                                                                                                                                                                                           |
| split.even-distribution.factor.lower-bound | Double  | 否    | 0.05            | 不建议修改。<br/> 分片键分布因子的下界。该因子用于判断表数据是否均匀分布。如果计算出的分布因子大于或等于此下界（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化为均匀分布的分片方式。否则，如果分布因子更小，则表将被视为非均匀分布，当预估的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 0.05。  |
| split.even-distribution.factor.upper-bound | Double  | 否    | 100             | 不建议修改。<br/> 分片键分布因子的上界。该因子用于判断表数据是否均匀分布。如果计算出的分布因子小于或等于此上界（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化为均匀分布的分片方式。否则，如果分布因子更大，则表将被视为非均匀分布，当预估的分片数超过 `sample-sharding.threshold` 指定的值时，将使用基于采样的分片策略。默认值为 100.0。 |
| split.sample-sharding.threshold            | Int     | 否    | 1000            | 此配置指定触发采样分片策略的预估分片数阈值。当分布因子超出 `split.even-distribution.factor.upper-bound` 和 `split.even-distribution.factor.lower-bound` 指定的范围，且预估分片数（计算方式为近似行数 / 分片大小）超过此阈值时，将使用采样分片策略。这有助于更高效地处理大数据集。默认值为 1000。                                                                                                                 |
| split.inverse-sampling.rate                | Int     | 否    | 1000            | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则表示在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理大数据集时尤为有用，此时较低的采样率可能更合适。默认值为 1000。                                                                                                            |
| split.allow-sampling                       | Boolean | 否    | true            | 是否启用基于采样的分片策略。当设置为 false 时，无论预估分片数是否超过阈值，系统都将回退到非均匀分片方式（迭代查询方式）。默认值为 true。                                                                                                                                                                                              |
| enable_concurrent_read                     | Boolean | 否    | true            | 是否在快照阶段启用基于分片的并发读取。当设置为 false 时，source 会跳过分片分析，并以单个 split 读取整张表，适合没有索引的表。默认值为 true。                                                                                                                                                                                          |
| split.string_split_mode                    | String  | 否    | sample          | 支持不同的字符串分割算法。默认使用 `sample`，通过采样字符串值来确定分割。可以切换为 `charset_based` 启用基于字符集的字符串分割算法。设置为 `charset_based` 时，算法假定 partition_column 的字符在 ASCII 32-126 范围内，覆盖了大多数基于字符的分割场景。                                                                                                                                                    |
| split.string-strategy                      | String  | 否    | -               | 控制 String 分区列的分片方式，可选值为 `none`、`hash`、`range`、`auto`。`range` 和 `auto` 当前要求 MySQL binary collation，且键值为固定长度的可打印 ASCII 字符串。其他 JDBC 方言在显式验证 range 分片支持前会拒绝 `range` 和 `auto`。`auto` 会优先尝试 range 分片，range 不安全时回退为 hash 分片。未设置该参数时，SeaTunnel 保持现有 `split.string_split_mode` 行为。                                                                                                                                                                                                                                                                       |
| split.string_split_mode_collate            | String  | 否    | -               | 当 string_split_mode 设置为 `charset_based` 且表具有特殊排序规则时，指定要使用的排序规则。如果未指定，将使用数据库的默认排序规则。                                                                                                                                                                                                            |
| common-options                             |         | 否    | -       | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                                                                                                                                                                                                                                                                                                                                                                                                                                 |

### 表匹配

请按照数据库方言要求填写完整表路径：

| 数据库类型 | 示例 |
|------------|------|
| MySQL | `sales.orders` |
| PostgreSQL 和 SQL Server | `sales.public.orders` |
| Oracle | `SALES.ORDERS` |

`use_regex = false` 表示精确匹配，也是最安全的默认方式。只有明确需要把 `table_path` 的表名部分作为正则表达式时，才设置 `use_regex = true`：

```text
table_path = "sales.orders_\\d+"
use_regex = true
```

HOCON 字符串中的正则反斜杠需要转义，因此正则中的 `\d+` 在配置文件里要写成 `\\d+`。最后一个未转义的点用于分隔数据库/schema 路径和表名模式。

许多 JDBC 驱动会把传给 `DatabaseMetaData` 的 schema 和表名参数当作 SQL `LIKE` 模式。SeaTunnel 会在发现元数据后再次精确核对标识符；对于大小写敏感的数据库，仍应确保配置与数据库中的真实标识符大小写完全一致。

:::note 视图与表匹配

`table_path`（无论是否启用 `use_regex`）是否会连带匹配到数据库视图（而不仅仅是基础表），取决于各方言内部列举表所使用的查询方式，目前没有可以显式包含或排除视图的配置项：

- MySQL 和 PostgreSQL 会把视图和基础表列在一起，因此像 `db.*` 这样宽泛的匹配模式也会匹配到视图。
- SQL Server 通过 `TABLE_TYPE = 'BASE TABLE'` 过滤，会排除视图。
- Oracle 和 Dameng 查询的是 `ALL_TABLES`，视图不在其中，因此会被间接排除。

如果需要在任意方言下都只读取指定的基础表，建议直接在 `table_list` 中显式列出表名，而不要依赖宽泛的正则表达式。

:::

### decimal_type_narrowing

十进制类型缩小，如果为 true，十进制类型将缩小为 int 或 long 类型（如果没有精度损失）。目前仅支持 Oracle。

例如：

decimal_type_narrowing = true

| Oracle        | SeaTunnel |
|---------------|-----------|
| NUMBER(1, 0)  | Boolean   |
| NUMBER(6, 0)  | INT       |
| NUMBER(10, 0) | BIGINT    |

decimal_type_narrowing = false

| Oracle        | SeaTunnel      |
|---------------|----------------|
| NUMBER(1, 0)  | Decimal(1, 0)  |
| NUMBER(6, 0)  | Decimal(6, 0)  |
| NUMBER(10, 0) | Decimal(10, 0) |

### int_type_narrowing

Int 类型缩小，如果为 true，tinyint(1) 类型将缩小为布尔类型（如果没有精度损失）。目前支持 MySQL。

例如：

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

### dialect [string]

指定的方言，如果不存在，仍然根据 url 获取，优先级高于 url。例如，使用 starrocks 时，需要将其设置为 `starrocks`。类似地，使用 mysql 时，需要将其值设置为 `mysql`。

如果 SeaTunnel 不支持某个方言，它将使用默认方言 `GenericDialect`。只需确保您提供的驱动程序支持您想要连接的数据库。

#### 方言列表

|           | 方言名称 |          |
|-----------|---------|----------|
| Greenplum | DB2     | Dameng   |
| Gbase8a   | HIVE    | KingBase |
| MySQL     | StarRocks | Oracle |
| Phoenix   | Postgres | Redshift |
| SapHana   | Snowflake | Sqlite |
| SqlServer | Tablestore | Teradata |
| Vertica   | OceanBase | XUGU |
| IRIS      | Inceptor | Highgo |
| YashanDB  |          |          |

达梦 `NCHAR` 源字段会映射为 SeaTunnel `STRING`。

## 并行读取器

任务 `parallelism` 决定最多可以同时运行多少个 Reader；分片配置决定实际有多少个独立 split 可以分配给 Reader。

### 推荐方式：`table_path` 动态分片

整表快照建议配置 `table_path`，通常让 SeaTunnel 自动发现分片键。显式配置 `partition_column` 时优先使用该列；否则依次从主键和唯一索引中选择第一个受支持的列。支持 String、数值和 Date 类型的分片键。

`split.size` 表示每个分片的目标行数，并不是严格的行数上限，实际大小会受到键值分布和数据库统计信息影响。如果表没有可用的主键、唯一索引或显式 `partition_column`，即使任务并行度大于 1，该表仍然只有一个读取分片。

### 顶层 `query` 固定分区

只有顶层同时配置 `query` 和 `partition_column` 时才会使用旧版 fixed splitter，再通过 `partition_num` 控制分片数。分区列必须包含在 query 结果中。配置上下界可以避免额外执行 `MIN`/`MAX` 查询，但错误的边界会漏读源数据，因此只有确认完整数据范围时才应设置。

`table_list` 中的条目即使配置了 `query` 或分区参数，仍然使用动态分片。`split.size` 不影响顶层 fixed partition 模式。

### Query 与主键注意事项

SeaTunnel 为 `query` 推断主键时，会继承结果集第一列所属底层表的元数据。对于 JOIN 或多表查询，该主键不保证在完整结果集中唯一。此类查询应使用单 Reader，或者显式选择能够安全划分查询结果的分区列。

## 驱动参考

下表仅作为起点。部署前应向数据库厂商确认驱动制品、许可证、数据库版本兼容性和 Java 版本兼容性。

| 数据源        | 驱动                                              | URL                                                                    | Maven                                                                                                                         |
|-------------|---------------------------------------------------|--------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| mysql             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| postgresql        | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| dm                | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| phoenix           | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| oracle            | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlserver         | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| sqlite            | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| gbase8a           | com.gbase.jdbc.Driver                               | jdbc:gbase://localhost:5258/test                                       | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                            |
| starrocks         | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| tablestore        | com.alicloud.openservices.tablestore.jdbc.OTSDriver | `jdbc:ots:https://<instance_name>.<region_id>.ots.aliyuncs.com/<instance_name>` | https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc                                           |
| saphana           | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| doris             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver           | jdbc&#58;snowflake://&lt;account_name&gt;.snowflakecomputing.com        | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                              |
| Redshift          | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb?defaultRowFetchSize=1000         | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Vertica           | com.vertica.jdbc.Driver                             | jdbc:vertica://localhost:5433                                          | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| kingbase          | com.kingbase8.Driver                                | jdbc:kingbase8://localhost:54321/db_test                               | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| oceanbase         | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| hive              | org.apache.hive.jdbc.HiveDriver                     | jdbc:hive2://localhost:10000                                           | https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar                                 |
| xugu              | com.xugu.cloudjdbc.Driver                           | jdbc:xugu://localhost:5138                                             | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver                    | jdbc:IRIS://localhost:1972/%SYS                                        | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |
| opengauss         | org.opengauss.Driver                                | jdbc:opengauss://localhost:5432/postgres                               | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar                              |
| Highgo            | com.highgo.jdbc.Driver                              | jdbc:highgo://localhost:5866/highgo                                    | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                                                   |
| Presto            | com.facebook.presto.jdbc.PrestoDriver               | jdbc:presto://localhost:8080/presto                                    | https://repo1.maven.org/maven2/com/facebook/presto/presto-jdbc/0.279/presto-jdbc-0.279.jar                                    |
| Trino             | io.trino.jdbc.TrinoDriver                           | jdbc:trino://localhost:8080/trino                                      | https://repo1.maven.org/maven2/io/trino/trino-jdbc/460/trino-jdbc-460.jar                                                     |
| YashanDB          | com.yashandb.jdbc.Driver                            | jdbc:yasdb://localhost:1688/SYS                                        |  https://repo1.maven.org/maven2/com/yashandb/yashandb-jdbc/1.10.7/yashandb-jdbc-1.10.7.jar                                    |

## 常用模式

### 自定义查询

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM orders WHERE status = 'PAID'"
  }
}

sink {
  Console {}
}
```

### Oracle BLOB 读取为 STRING

需要把 Oracle BLOB 暴露为 SeaTunnel STRING 时，可以设置 `handle_blob_as_string = true`，例如后续需要写入 Doris 的场景。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@oracle.example.com:1521/SERVICE_NAME"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT ID, NAME, CONTENT_BLOB FROM MY_TABLE"
    handle_blob_as_string = true  # 为 Oracle 启用 BLOB 到字符串转换
  }
}

sink {
  Console {}
}
```

### 按列并行执行自定义 query

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM orders"
    partition_column = "id"
    partition_num = 10
  }
}

sink {
  Console {}
}
```

### 显式配置分区边界

只有上下界能够覆盖完整源数据范围时才应设置边界。SeaTunnel 不会读取配置区间之外的值。

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    query = "SELECT id, customer_name, amount FROM orders"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 500
    partition_num = 10
  }
}

sink {
  Console {}
}
```

### 通过主键或唯一索引动态分片

本示例只配置 `table_path`，没有同时配置顶层 `query + partition_column`，因此使用动态分片。建议先使用默认分片参数，再根据源库负载和任务吞吐实测结果调整 `split.size`。

```hocon
env {
  parallelism = 10
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"
    table_path = "sales.orders"
    split.size = 10000
  }
}

sink {
  Console {}
}
```

### 读取多张表

不同表需要不同 query 或匹配规则时使用 `table_list`：

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}

source {
  Jdbc {
    url = "jdbc:mysql://mysql.example.com:3306/sales?serverTimezone=UTC"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "seatunnel_reader"
    password = "change_me"

    table_list = [
      {
        table_path = "sales.orders"
      },
      {
        table_path = "sales.customers"
        query = "SELECT id, name FROM customers WHERE id > 100"
      },
      {
        table_path = "sales.archive_\\d+"
        use_regex = true
      }
    ]
  }
}

sink {
  Console {}
}
```

## 故障排查

### 找不到 JDBC 驱动类

确认每个执行节点的引擎对应目录中都有驱动 JAR，配置的 `driver` 类真实存在于该 JAR，并且添加 JAR 后已经重启受影响的进程。

### SQL 客户端能连接，但 SeaTunnel 连接失败

主机名必须能从 SeaTunnel 进程所在环境访问，而不只是能从个人电脑访问。检查网络路由、防火墙、TLS、账号、数据库名和 `connection_check_timeout_sec`，不要直接使用未替换的示例主机名。

### 无法发现表或字段

检查数据库对应的 `table_path` 格式、标识符大小写，以及账号是否具有元数据和 `SELECT` 权限。使用自定义 query 时，先在数据库客户端中用同一账号执行完全相同的 SQL。

### 提高 parallelism 后 Reader 数量没有增加

使用动态分片时，确认表存在受支持的主键或唯一索引，或者显式配置 `partition_column`。要让顶层 `query` 使用旧版 fixed splitter，需要同时配置 `partition_column` 和合适的 `partition_num`。没有安全分片键的表会有意使用单个 split 读取。

### 配置分区边界后出现漏数

`partition_lower_bound` 和 `partition_upper_bound` 定义 SeaTunnel 实际读取的数据范围，并不只是性能提示。可以删除它们，让 SeaTunnel 自动发现边界；也可以修正边界，确保覆盖所有需要读取的行。

## 变更日志

<ChangeLog />
