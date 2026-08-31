import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

## 描述

JDBC Sink 通过数据库厂商提供的 JDBC 驱动写入数据。它支持批处理和流处理、并行写入、自动生成 SQL 或自定义 SQL、多表写入、CDC 事件，以及基于 XA 事务的可选精确一次语义。

第一次配置 JDBC Sink 时，请先阅读[选择写入模式](#选择写入模式)和[快速入门](#快速入门postgresql)，再按需查阅后面的完整参数说明。

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

不同数据库厂商的 JDBC 驱动具有不同的许可证和再分发条款，而且驱动版本还必须同时兼容目标数据库和 Java 运行时，因此 SeaTunnel 不会统一内置所有 JDBC 驱动。请自行下载合适的驱动，并在启动任务前把 JAR 放入对应引擎的目录。

### Spark 和 Flink 引擎

把 JDBC 驱动放到每个 SeaTunnel 执行节点的 `${SEATUNNEL_HOME}/plugins/Jdbc/lib/`。

### Zeta 引擎

把 JDBC 驱动放到每个 SeaTunnel 节点的 `${SEATUNNEL_HOME}/lib/`，然后重启受影响的 SeaTunnel 进程，让驱动进入类路径。

常用驱动类名和下载地址见[驱动参考](#驱动参考)。

## 选择写入模式

JDBC Sink 有两种互斥的写入模式。请先确定模式，再配置其余参数。

| 使用场景 | 必需配置 | 行为 |
|----------|----------|------|
| 由 SeaTunnel 生成 SQL | `generate_sink_sql = true`、`database`，通常还要配置 `table` | 推荐大多数任务使用。SeaTunnel 可以根据上游 schema 和 RowKind 生成 INSERT、数据库原生 UPSERT、UPDATE 和 DELETE；可以使用 SaveMode 和自动建表。 |
| 用户提供 SQL | `query = "INSERT ... VALUES (?, ...)"` | 适合必须完全控制目标 SQL 的场景。`?` 参数按照上游字段顺序绑定；此模式不会执行 SaveMode 相关配置。 |

不要同时配置两种模式。`generate_sink_sql` 默认是 `false`，因此没有显式设置 `generate_sink_sql = true` 的任务必须提供 `query`。

使用自动生成 SQL 时，如果目标端需要 Upsert、Update 或 Delete，请配置 `primary_keys`。未配置时，SeaTunnel 会尝试从上游 Catalog 元数据继承主键，再尝试第一个唯一键；仍然没有可用键时，会退化为普通 INSERT。

## 快速入门：PostgreSQL

下面使用自动生成 SQL 写入一张已经创建的 PostgreSQL 表。这份配置和预期结果已经在 PostgreSQL 14 中验证，包括示例数据写入和最终数据库值。

1. 按照[使用依赖](#使用依赖)的说明放置兼容版本的 PostgreSQL JDBC 驱动。

2. 创建目标表：

```sql
CREATE TABLE public.orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(10, 2) NOT NULL
);
```

3. 把下面的任务保存为 `${SEATUNNEL_HOME}/config/jdbc-sink-quick-start.conf`，并按实际环境替换主机、账号和数据库名。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 3
    schema = {
      fields {
        id = bigint
        customer_name = string
        amount = "decimal(10, 2)"
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice", 120.50] }
      { kind = INSERT, fields = [2, "Bob", 80.00] }
      { kind = INSERT, fields = [3, "Carol", 42.00] }
    ]
  }
}

sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/sales"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "change_me"
    generate_sink_sql = true
    database = "sales"
    table = "public.orders"
    primary_keys = ["id"]
    schema_save_mode = "ERROR_WHEN_SCHEMA_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

4. 运行任务：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-sink-quick-start.conf -m local
```

5. 验证结果：

```sql
SELECT id, customer_name, amount
FROM public.orders
ORDER BY id;
```

预期结果：

| id | customer_name | amount |
|----|---------------|-------:|
| 1 | Alice | 120.50 |
| 2 | Bob | 80.00 |
| 3 | Carol | 42.00 |

如果任务在写入前失败，请先检查[故障排查](#故障排查)。

:::note

连接 MariaDB 时，请使用 MariaDB Connector/J 以及匹配的 URL 和驱动：

```hocon
url = "jdbc:mariadb://localhost:3306/database"
driver = "org.mariadb.jdbc.Driver"
```

不要使用 MySQL Connector/J 和 `jdbc:mysql:` URL 连接 MariaDB。该配置会选择 MySQL 方言，可能将 MariaDB 服务端版本判定为不支持的 MySQL 版本。

:::

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)

Exactly-once 依赖 XA 事务，因此数据库和 JDBC 驱动都必须支持 XA。具体要求见 [Exactly-once 前置条件](#exactly-once-前置条件)。

- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)（仅 Zeta 引擎）

## Options

连接器的 OptionRule 始终要求提供 `url`、`driver`、`schema_save_mode` 和 `data_save_mode`。两个 SaveMode 参数有默认值，任务配置通常可以省略。其他参数会随写入模式变为必填：

- 自动生成 SQL：设置 `generate_sink_sql = true` 并配置 `database`；除非目标表由上游元数据动态提供，否则还要配置 `table`。
- 自定义 SQL：保持 `generate_sink_sql = false` 并配置 `query`。
- Exactly-once：设置 `is_exactly_once = true`、`xa_data_source_class_name` 和 `max_retries = 0`。

| 名称                                        | 类型      | 是否必须 | 默认值                          |
|-------------------------------------------|---------|------|------------------------------|
| url                                       | String  | 是    | -                            |
| driver                                    | String  | 是    | -                            |
| username                                  | String  | 否    | -                            |
| password                                  | String  | 否    | -                            |
| query                                     | String  | 否    | -                            |
| compatible_mode                           | String  | 否    | -                            |
| dialect                                   | String  | 否    | -                            | 
| database                                  | String  | 否    | -                            |
| table                                     | String  | 否    | -                            |
| tablePrefix                               | String  | 否    | -                            |
| tableSuffix                               | String  | 否    | -                            |
| primary_keys                              | Array   | 否    | -                            |
| multi-table_config                        | Object  | 否    | -                            |
| connection_check_timeout_sec              | Int     | 否    | 30                           |
| connect_timeout_ms                        | Int     | 否    | 86400000                     |
| socket_timeout_ms                         | Int     | 否    | 86400000                     |
| max_retries                               | Int     | 否    | 0                            |
| batch_size                                | Int     | 否    | 1000                         |
| batch_interval_ms                         | Long    | 否    | 0                            |
| is_exactly_once                           | Boolean | 否    | false                        |
| generate_sink_sql                         | Boolean | 否    | false                        |
| xa_data_source_class_name                 | String  | 否    | -                            |
| max_commit_attempts                       | Int     | 否    | 3                            |
| transaction_timeout_sec                   | Int     | 否    | -1                           |
| auto_commit                               | Boolean | 否    | true                         |
| field_ide                                 | String  | 否    | -                            |
| properties                                | Map     | 否    | -                            |
| common-options                            |         | 否    | -                            |
| schema_save_mode                          | Enum    | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode                            | Enum    | 否    | APPEND_DATA                  |
| custom_sql                                | String  | 否    | -                            |
| enable_upsert                             | Boolean | 否    | true                         |
| is_primary_key_updated                    | Boolean | 否    | true                         |
| support_upsert_by_insert_only             | Boolean | 否    | false                        |
| table_options                             | Map     | 否    | -                            |
| use_copy_statement                        | Boolean | 否    | false                        |
| oracle_insert_mode                        | Enum    | 否    | CONVENTIONAL                 |
| create_index                              | Boolean | 否    | true                         |
| use_kerberos                              | Boolean | 否    | false                        |
| kerberos_principal                        | String  | 否    | -                            |
| kerberos_keytab_path                      | String  | 否    | -                            |
| krb5_path                                 | String  | 否    | /etc/krb5.conf               |
| access_key_id                             | String  | 否       |                              |
| secret_access_key                         | String  | 否       |                              |
| region                                    | String  | 否       |                              |
### driver [string]

用于连接远程数据源的 jdbc 类名，如果使用MySQL，则值为`com.mysql.cj.jdbc.Driver`

### username [string]

数据库登录用户名。`username` 是规范参数名；未设置 `username` 时，仍兼容旧参数 `user`。

### password [string]

密码

### url [string]

JDBC 连接的 URL。参考案例：`jdbc:postgresql://localhost/test`

### query [string]

用于写入每条上游数据的参数化 SQL，例如 `INSERT INTO target(id, name) VALUES (?, ?)`。SeaTunnel 按上游字段顺序绑定 `?` 参数。该参数只用于自定义 SQL 模式，不能与 `generate_sink_sql = true` 同时使用。

当前限制：当 sink 配置了 `query`（自定义写入 SQL）时，JDBC sink 不会执行 save mode 处理。此模式下 `schema_save_mode`、`data_save_mode`、`custom_sql` 不生效。如需使用 save mode，请改用 `generate_sink_sql = true` 并配置 `database`、`table`。

### compatible_mode [string]

数据库的兼容模式，当数据库支持多种兼容模式时需要。

例如，使用 OceanBase 数据库时，需要将其设置为 'mysql' 或 'oracle' 。使用StarRocks时，需要将其设置为`starrocks`。

Postgres 9.5及以下版本，请设置为 `postgresLow` 来支持 CDC

### dialect [string]

指定的方言，如果不存在，仍然按照url获取，优先级高于url。例如，当使用 starrocks 时，你需要将其值设置为 starrocks，同理，当使用mysql时，你需要将其值设置为mysql。

如果 SeaTunnel 不支持某种方言，它将使用默认方言 `GenericDialect`。请确保您提供的驱动程序支持您想要连接的数据库。

#### 示例可选

|           | 方言名称       |          |
|-----------|------------|----------|
| Greenplum | DB2        | Dameng   |
| Gbase8a   | HIVE       | KingBase |
| MySQL     | StarRocks  | Oracle   |
| Phoenix   | Postgres   | Redshift |
| SapHana   | Snowflake  | Sqlite   |
| SqlServer | Tablestore | Teradata |
| Vertica   | OceanBase  | XUGU     |
| IRIS      | Inceptor   | Highgo   |
| DSQL      |            |          |
| YashanDB  |            |          |

### database [string]

自动生成 SQL 模式下的目标 database 或 catalog。`generate_sink_sql = true` 时必填，不能与 `query` 同时使用。

### table [string]

自动生成 SQL 模式下的目标表，不能与 `query` 同时使用。

table 参数可以填入目标表名，这个名字最终会被用作创建或写入的表名，并且支持变量（`${table_name}`，`${schema_name}`）。
替换规则如下：`${schema_name}` 将替换传递给目标端的 SCHEMA 名称，`${table_name}` 将替换传递给目标端的表名。

mysql 接收器示例:

1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

pgsql (Oracle Sqlserver ...) 接收器示例:

1. ${schema_name}.${table_name}_test
2. dbo.tt_${table_name}_sink
3. public.sink_table

Tip: 如果目标数据库有 SCHEMA 的概念，则表参数必须写成 `xxx.xxx`

### tablePrefix [string]

已过时。请改用带表占位符的 `table` 参数。例如，使用 `table = "prefix_${table_name}_suffix"` 替代 `tablePrefix` 和 `tableSuffix`。

### tableSuffix [string]

已过时。请改用带表占位符的 `table` 参数。例如，使用 `table = "prefix_${table_name}_suffix"` 替代 `tablePrefix` 和 `tableSuffix`。

### primary_keys [array]

生成数据库原生 UPSERT、UPDATE 和 DELETE 语句时使用的目标键列。未配置时，SeaTunnel 会尝试从上游 catalog 元数据继承主键或第一组唯一键；仍无可用键时，自动生成的 SQL 退回普通 INSERT。

### multi-table_config [object]

用于多表自动生成 SQL 场景的按表主键映射，优先级高于顶层 `primary_keys` 选项：当上游表名匹配到 `primary_keys` 下声明的某个模式时，使用该映射；否则回退到现有 `primary_keys` / catalog 元数据逻辑。

配置结构如下：

```hocon
multi-table_config {
  primary_keys {
    "<表名正则>" = ["key1", "key2"]
  }
}
```

- 每个 key 是匹配上游表名的 Java 正则表达式，使用全匹配语义（`tableName.matches(pattern)`）。例如使用 `"^t_nova_.*$"` 或 `"t_nova_.*"`，不要使用 `"t_nova_*"` 这类 glob 写法。
- 每个 value 是列名列表。仅在该选项内支持 `${primary_key}` 和 `${unique_key}` 占位符，且可与静态列混用。`${primary_key}` 展开为上游主键列，`${unique_key}` 展开为上游第一组唯一键列。
- 若一张表同时匹配多个模式，按声明顺序取第一个匹配。
- 若命中的表使用了 `${primary_key}`（或 `${unique_key}`）但上游没有主键（或唯一键），任务会以清晰错误信息失败。

示例：为表名以 `t_nova_` 开头的表，使用上游主键加上共享的 `DATA_SOURCE` 列作为复合主键。

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    generate_sink_sql = true
    multi-table_config {
      primary_keys {
        "^t_nova_.*$" = ["${primary_key}", "DATA_SOURCE"]
      }
    }
  }
}
```

示例：`multi-table_config` 与顶层 `primary_keys` 混用。被映射命中的表使用映射值，未命中的表回退到 `primary_keys = ["merchant_id"]`。

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    generate_sink_sql = true
    primary_keys = ["merchant_id"]
    multi-table_config {
      primary_keys {
        "t_tyuen_txn_ext.*"          = ["id_txn_ctrl", "DATA_SOURCE"]
        "t_nova_merge_settle_serial" = ["${primary_key}", "DATA_SOURCE"]
      }
    }
  }
}
```

### connection_check_timeout_sec [int]

用于验证数据库连接的有效性时等待数据库操作完成所需的时间，单位是秒

### connect_timeout_ms [int]

建立 JDBC 连接时的连接超时时间，单位毫秒。默认值为 24 小时。设置为 `0` 表示不超时。

### socket_timeout_ms [int]

JDBC 连接建立后的 socket 读取超时时间，单位毫秒。默认值为 24 小时。设置为 `0` 表示不超时。

### max_retries [int]

JDBC `executeBatch` 失败后的重试次数。Exactly-once 模式要求设置为 `0`，重试失败的 XA batch 可能破坏事务保证。

### batch_size [int]

每个 batch 最多缓存的行数。达到 `batch_size`、checkpoint 准备提交或 writer 关闭时会执行 flush。增大该值可能提高吞吐，但会占用更多内存，并增加故障后需要重试的数据量。

### batch_interval_ms [long]

刷新间隔（毫秒）。当设置值大于 0 时，若距上次 flush 的时间超过该间隔，下一次 `writeRecord` 调用将同步触发 flush，即使尚未达到 `batch_size`。默认值为 `0`（禁用）。此为**写入触发**的时间检查，而非后台定时器——若无新记录到达（空闲分区），不会触发基于时间的 flush；缓冲数据将在下一次 `prepareCommit`（checkpoint）或 `close` 时刷出。注意：当 `auto_commit = false` 时，已 flush 的数据在下次 commit（如 checkpoint）之前对其他事务不可见。

### is_exactly_once [boolean]

是否通过 XA 事务启用 exactly-once。开启时必须配置 `xa_data_source_class_name`、`max_retries = 0`，且数据库和驱动都要支持 XA；该模式不支持定时 flush。

### generate_sink_sql [boolean]

为 `true` 时，根据上游 schema 和 RowKind 自动生成写入语句。需要配置 `database`，通常还要配置 `table`，并且不能配置 `query`。默认值为 `false`，此时必须配置 `query`。

### xa_data_source_class_name [string]

指数据库驱动的 XA 数据源的类名。以 MySQL 为例，其类名为 com.mysql.cj.jdbc.MysqlXADataSource。了解其他数据库的数据源类名，可以参考文档的附录部分

### max_commit_attempts [int]

事务提交失败的最大重试次数

### transaction_timeout_sec [int]

在事务开启后的超时时间，默认值为-1（即永不超时）。请注意，设置超时时间可能会影响到精确一次（exactly-once）的语义

### auto_commit [boolean]

默认启用自动事务提交

### field_ide [String]

字段 `field_ide` 用于在从 source 同步到 sink 时，确定字段是否需要转换为大写或小写。'ORIGINAL' 表示不需要转换，'UPPERCASE'
表示转换为大写，'LOWERCASE' 表示转换为小写

### properties

附加连接配置参数，当属性和URL具有相同参数时，优先级由驱动程序的具体实现确定。例如，在 MySQL 中，属性配置优先于 URL。

### common options

Sink插件常用参数，请参考 [Sink常用选项](../common-options/sink-common-options.md) 了解详情

### schema_save_mode [Enum]

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA`：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST`：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST`：当表不存在时将抛出错误<br/>
`IGNORE` ：忽略对表的处理<br/>

### data_save_mode [Enum]

在启动同步任务之前，针对目标侧已存在的数据选择不同的处理方案<br/>
选项介绍：<br/>
`DROP_DATA`：保留数据库结构，删除数据<br/>
`APPEND_DATA`：保留数据库结构，保留数据<br/>
`CUSTOM_PROCESSING`：允许用户自定义数据处理方式<br/>
`ERROR_WHEN_DATA_EXISTS`：当有数据时抛出错误<br/>

### table_options [Map]

Sink 在自动建表（SaveMode DDL）时附加的表级选项。仅在 `schema_save_mode` 触发建表时生效，例如 `CREATE_SCHEMA_WHEN_NOT_EXIST`、`RECREATE_SCHEMA`；**不影响**数据写入阶段的 INSERT/UPSERT，也**不会**对已存在表执行 `ALTER TABLE`。

当前支持情况：

| 方言 | 是否支持 | 可用 key |
|------|----------|----------|
| MySQL | 是 | `engine`、`charset`、`collate` |
| TiDB | 是 | `engine`、`charset`、`collate`（通过 MySQL JDBC 协议与 `jdbc:mysql://` 连接） |
| OceanBase（MySQL 模式） | 是 | `engine`、`charset`、`collate` |
| PostgreSQL | 是 | `tablespace`、`fillfactor` |
| 达梦 Dameng | 是 | `tablespace`、`fillfactor` |
| Oracle | 是 | `tablespace`、`pctfree` |
| OceanBase（Oracle 模式） | 是 | `tablespace`、`pctfree`（`compatible_mode=oracle` 时走 Oracle 方言 / DDL 路径） |
| Kingbase | 是 | `tablespace`、`fillfactor` |
| 其他 JDBC 方言 | 否 | 配置非空 `table_options` 时任务启动即校验失败 |

非法或不支持的 key 会在 `JdbcSinkFactory` 的 option 规则阶段提前校验（`--check` 与作业提交），而非仅在运行时 DDL 阶段失败。

**方言说明：**

- **MySQL**：`engine`、`charset`、`collate` 均会写入 `CREATE TABLE` 并生效。
- **TiDB**：通过 `jdbc:mysql://` 与 MySQL JDBC 驱动连接时，与 MySQL 使用相同的 key 白名单与 DDL 拼接方式。`charset`、`collate` 会生效；`engine` 仅为 MySQL 兼容语法，TiDB 会解析但**忽略**存储引擎设置。
- **OceanBase（MySQL 模式）**：`jdbc:oceanbase://` 且非 Oracle 兼容模式时支持上述三个 key。`charset`、`collate` 须为 OceanBase 当前版本支持的字符集与排序规则（通常为 MySQL 兼容子集，请以目标库 `SHOW CHARSET` / `SHOW COLLATION` 为准）；不支持的取值会在执行 `CREATE TABLE` 时报错，而非在作业提交阶段校验。
- **PostgreSQL**：`fillfactor` 会生成 `WITH (fillfactor=<n>)`，取值须为 `[10, 100]` 的整数；`tablespace` 会生成 `TABLESPACE "..."`，按配置字面量引用（**不受** `fieldIde` 大小写改写）。空白值，以及 `tablespace` 中的非法字符（例如 `"`）会在作业提交阶段被拒绝。仅接受上述 curated key（不支持任意 `WITH` 参数）。OpenGauss、HighGo 通过 Postgres catalog/dialect 继承同一套校验与 DDL。
- **达梦 Dameng**：`fillfactor` 与 `tablespace` 会写入达梦 `STORAGE (...)` 子句（`FILLFACTOR <n>`、`ON "<表空间>"`）。`fillfactor` 取值须为 `[0, 100]` 的整数；`tablespace` 按配置字面量引用（**不受** `fieldIde` 大小写改写）。空白值，以及 `tablespace` 中的非法字符（例如 `"`）会在作业提交阶段被拒绝。仅接受上述 curated key（不支持透传任意 `STORAGE` 参数如 `INITIAL` / `NEXT`）。表空间须已在目标库存在。
- **Oracle / OceanBase（Oracle 模式）**：`pctfree` 会生成 `PCTFREE <n>`，取值须为 `[0, 99]` 的整数；`tablespace` 会生成 `TABLESPACE "..."`，按配置字面量引用（**不受** `fieldIde` 大小写改写）。空白值，以及 `tablespace` 中的非法字符（例如 `"`）会在作业提交阶段被拒绝。仅接受上述 curated key（不支持嵌套 `STORAGE (...)`、LOB/分区子句）。目标库中 tablespace 须事先存在。
- **Kingbase**：`fillfactor` 会写入 `WITH (fillfactor=<n>)`，取值须为 `[10, 100]` 的整数（PostgreSQL 兼容）；`tablespace` 会写入 `TABLESPACE "..."`，按配置字面量引用（**不受** `fieldIde` 大小写改写）。空白值，以及 `tablespace` 中的非法字符（例如 `"`）会在作业提交阶段被拒绝。仅接受上述 curated key（不支持透传任意 `WITH (...)` 参数）。表空间须已在目标库存在。

SeaTunnel 在提交时会对所有支持 `table_options` 的方言校验 **key 白名单**。对 PostgreSQL（以及 OpenGauss / HighGo 同源路径）、达梦与 Kingbase，还会校验空白值与 `fillfactor` 数值区间。对 Oracle / OceanBase（Oracle 模式），还会校验空白值与 `pctfree` 数值区间。其他方言（例如 MySQL）在白名单之外不额外校验具体取值是否被目标库支持。

示例（MySQL 自动建表时指定存储引擎与字符集）：

```hocon
sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3307/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "password"
    database = "mydb"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "engine" = "InnoDB"
      "charset" = "utf8mb4"
      "collate" = "utf8mb4_general_ci"
    }
  }
}
```

生成的 DDL 会追加 `ENGINE`、`DEFAULT CHARSET`、`COLLATE` 子句。未在白名单内的 key（如 `bucket_num`）会在作业提交阶段报错。

示例（PostgreSQL 自动建表时指定 tablespace 与 fillfactor）：

```hocon
sink {
  Jdbc {
    url = "jdbc:postgresql://localhost:5432/mydb"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "password"
    database = "mydb"
    table = "public.orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "pg_default"
      "fillfactor" = "70"
    }
  }
}
```

示例（达梦自动建表时指定表空间与填充比例）：

```hocon
sink {
  Jdbc {
    url = "jdbc:dm://localhost:5236"
    driver = "dm.jdbc.driver.DmDriver"
    username = "SYSDBA"
    password = "SYSDBA"
    database = "DAMENG"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "MAIN"
      "fillfactor" = "80"
    }
  }
}
```

生成的 DDL 会追加 `STORAGE (FILLFACTOR 80, ON "MAIN")`。

示例（Oracle 自动建表时指定 tablespace 与 pctfree）：

```hocon
sink {
  Jdbc {
    url = "jdbc:oracle:thin:@localhost:1521/ORCLPDB1"
    driver = "oracle.jdbc.OracleDriver"
    username = "scott"
    password = "tiger"
    database = "SCOTT"
    table = "ORDERS"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "USERS"
      "pctfree" = "10"
    }
  }
}
```

示例（Kingbase 自动建表时指定表空间与 fillfactor）：

```hocon
sink {
  Jdbc {
    url = "jdbc:kingbase8://localhost:54321/test"
    driver = "com.kingbase8.Driver"
    username = "SYSTEM"
    password = "123456"
    database = "test"
    table = "orders"
    generate_sink_sql = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    primary_keys = ["id"]
    table_options = {
      "tablespace" = "pg_default"
      "fillfactor" = "70"
    }
  }
}
```

生成的 DDL 会追加 `WITH (fillfactor=70)` 与 `TABLESPACE "pg_default"`。

### custom_sql [String]

当`data_save_mode`选择`CUSTOM_PROCESSING`时，需要填写`CUSTOM_SQL`参数。该参数通常填写一条可以执行的SQL。SQL将在同步任务之前执行

注意：在 sink 的 `query` 模式下，`custom_sql` 不会执行。这是 JDBC sink 的当前限制。

### enable_upsert [boolean]

启用通过主键更新插入，如果任务没有key重复数据，设置该参数为 false 可以加快数据导入速度

### is_primary_key_updated [boolean]

生成 update 语句时是否包含主键字段。除非目标数据库要求更新时跳过主键列，一般保持默认值即可。

### support_upsert_by_insert_only [boolean]

是否通过仅 insert 的语句支持 upsert 行为。该参数属于高级兼容选项，默认关闭。

### use_copy_statement [boolean]

使用 `COPY ${table} FROM STDIN` 语句导入数据。仅支持具有 `getCopyAPI()` 方法连接的驱动程序。例如：Postgresql
驱动程序 `org.postgresql.Driver`

注意：不支持 `MAP`、`ARRAY`、`ROW`类型

### oracle_insert_mode [Enum]

Oracle 插入模式。默认值为 `CONVENTIONAL`，保持现有 JDBC insert 行为。

设置为 `APPEND_VALUES` 时，SeaTunnel 会为自动生成的 Oracle insert SQL 添加 `APPEND_VALUES` hint：

```sql
INSERT /*+ APPEND_VALUES */ INTO ...
```

该选项仅支持 Oracle JDBC Sink 的 insert-only 写入。使用时必须配置 `generate_sink_sql = true`、`auto_commit = true`，不能配置自定义 `query`，不能配置 `primary_keys`，并且 `is_exactly_once = false`、`support_upsert_by_insert_only = false`。

### create_index [boolean]

自动建表时是否创建索引（包含主键和其他索引）。迁移大表时可关闭该选项以提升写入速度，但迁移完成后需要手动创建索引来保证查询性能。

### use_kerberos [boolean]

是否为 JDBC 连接启用 Kerberos 认证。开启后，请根据环境同时配置 `kerberos_principal`、`kerberos_keytab_path` 和 `krb5_path`。

### access_key_id [String]
AWS IAM 认证中所需要的access_key_id 。 该参考仅适用于 dialect="dsql"

### secret_access_key [String]
AWS IAM 认证中所需要的secret_access_key。 该参考仅适用于 dialect="dsql"

### region [String]
Amazon Aurora DSQL 所在的区域。 该参考仅适用于 dialect="dsql"

## Exactly-once 前置条件

`is_exactly_once = true` 时，JDBC Sink 使用 XA 事务。启用前请确认：

- 设置 `max_retries = 0`，并为已安装的驱动配置正确的 `xa_data_source_class_name`。
- PostgreSQL 必须允许 prepared transaction。把 `max_prepared_transactions` 设置为能够容纳预期并发事务的正数；如果数据库要求，修改后需重启 PostgreSQL。
- MySQL Server 和 Connector/J 的组合必须支持 Sink 使用的 XA 操作。执行 XA recovery 的账号还可能需要 `XA_RECOVER_ADMIN` 权限，请按所用 MySQL 版本的要求配置。
- 不要配置 `sink.flush.interval`，XA 事务边界由 checkpoint 控制。

对于非 XA 的 MySQL 批量任务，可以尝试在 JDBC URL 中加入 `rewriteBatchedStatements=true` 提升吞吐；实际效果应结合驱动版本和业务负载验证。

## 驱动参考

下表仅作为起点；驱动制品和版本应以数据库厂商的兼容矩阵为准。

| 数据源        | driver                                       | url                                                                | xa_data_source_class_name                          | maven                                                                                              |
|------------|----------------------------------------------|--------------------------------------------------------------------|----------------------------------------------------|----------------------------------------------------------------------------------------------------|
| MySQL      | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | com.mysql.cj.jdbc.MysqlXADataSource                | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                      |
| PostgreSQL | org.postgresql.Driver                        | jdbc:postgresql://localhost:5432/postgres                          | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                       |
| DM         | dm.jdbc.driver.DmDriver                      | jdbc:dm://localhost:5236                                           | dm.jdbc.driver.DmdbXADataSource                    | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                       |
| Phoenix    | org.apache.phoenix.queryserver.client.Driver | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF | /                                                  | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client               |
| SQL Server | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433                                    | com.microsoft.sqlserver.jdbc.SQLServerXADataSource | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                              |
| Oracle     | oracle.jdbc.OracleDriver                     | jdbc:oracle:thin:@localhost:1521/xepdb1                            | oracle.jdbc.xa.OracleXADataSource                  | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                 |
| sqlite     | org.sqlite.JDBC                              | jdbc:sqlite:test.db                                                | /                                                  | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                          |
| GBase8a    | com.gbase.jdbc.Driver                        | jdbc:gbase://localhost:5258/test                                   | /                                                  | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar |
| StarRocks  | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                      |
| db2        | com.ibm.db2.jcc.DB2Driver                    | jdbc:db2://localhost:50000/testdb                                  | com.ibm.db2.jcc.DB2XADataSource                    | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                  |
| saphana    | com.sap.db.jdbc.Driver                       | jdbc:sap://localhost:39015                                         | /                                                  | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                     |
| Doris      | com.mysql.cj.jdbc.Driver                     | jdbc:mysql://localhost:3306/test                                   | /                                                  | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                      |
| teradata   | com.teradata.jdbc.TeraDriver                 | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test              | /                                                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                      |
| Redshift   | com.amazon.redshift.jdbc42.Driver            | jdbc:redshift://localhost:5439/testdb                              | com.amazon.redshift.xa.RedshiftXADataSource        | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                             |
| Snowflake  | net.snowflake.client.jdbc.SnowflakeDriver    | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com         | /                                                  | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                    |
| Vertica    | com.vertica.jdbc.Driver                      | jdbc:vertica://localhost:5433                                      | /                                                  | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar    |
| Kingbase   | com.kingbase8.Driver                         | jdbc:kingbase8://localhost:54321/db_test                           | /                                                  | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                 |
| OceanBase  | com.oceanbase.jdbc.Driver                    | jdbc:oceanbase://localhost:2881                                    | /                                                  | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar   |
| opengauss  | org.opengauss.Driver                         | jdbc:opengauss://localhost:5432/postgres                           | /                                                  | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar   |
| Highgo     | com.highgo.jdbc.Driver                       | jdbc:highgo://localhost:5866/highgo                                | /                                                  | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                        |
| Dsql       | org.postgresql.Driver                        | jdbc:postgresql://Amazon Aurora DSQL Cluster Endpoint:5432/postgres | org.postgresql.xa.PGXADataSource                   | https://mvnrepository.com/artifact/org.postgresql/postgresql                                       |
| YashanDB   | com.yashandb.jdbc.Driver                     | jdbc:yasdb://localhost:1688/SYS                                    | /                                                  | https://repo1.maven.org/maven2/com/yashandb/yashandb-jdbc/1.10.7/yashandb-jdbc-1.10.7.jar          |

## 常用模式

### 自定义 SQL

```hocon
jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"
}

```

### 自定义 SQL 的 Exactly-once

通过设置 `is_exactly_once` 开启精确一次语义

```hocon
jdbc {

    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"

    max_retries = 0
    username = "root"
    password = "123456"
    query = "insert into test_table(name,age) values(?,?)"

    is_exactly_once = true

    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
}
```

### Zeta 定时刷新

该引擎级能力仅由 Zeta 支持。Spark 和 Flink 不会注入 `FlushSignal`，因此在这两个引擎中配置 `sink.flush.interval` 不能启用定时刷新。在 Zeta 中，可以在 `env` 块配置 `sink.flush.interval`；引擎会定期向记录流注入 `FlushSignal`，JDBC Sink 收到后会立即刷出全部缓冲数据，无论是否达到 `batch_size`。

:::tip

当 `is_exactly_once = true` 时不支持定时刷新。精确一次模式下 sink 使用 XA 事务，其事务边界由 checkpoint 管理；定时触发的 flush 会破坏事务一致性保证。

:::

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 30000
  sink.flush.interval = 5000
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    database = "sink_database"
    table = "sink_table"
    generate_sink_sql = true
    primary_keys = ["id"]
    batch_size = 10000
  }
}
```

### 变更数据捕获事件

jdbc 接收 CDC 示例

```hocon
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        username = "root"
        password = "123456"
        generate_sink_sql = true
        database = "sink_database"
        table = "sink_table"
        primary_keys = ["key1", "key2"]
    }
}
```

### 自动创建不存在的目标表

通过设置 `schema_save_mode` 配置为 `CREATE_SCHEMA_WHEN_NOT_EXIST` 来支持不存在表时创建表

```hocon
sink {
    jdbc {
        url = "jdbc:mysql://localhost:3306"
        driver = "com.mysql.cj.jdbc.Driver"
        username = "root"
        password = "123456"
        generate_sink_sql = true
        database = "sink_database"
        table = "sink_table"
        primary_keys = ["key1", "key2"]
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

### PostgreSQL 9.5 及以下版本 CDC 兼容模式

Postgres 9.5及以下版本，通过设置 `compatible_mode` 配置为 `postgresLow` 来支持 Postgres CDC 操作

```hocon
sink {
    jdbc {
        url = "jdbc:postgresql://localhost:5432"
        driver = "org.postgresql.Driver"
        username = "root"
        password = "123456"
        compatible_mode = "postgresLow"
        database = "sink_database"
        table = "sink_table"
        generate_sink_sql = true
        primary_keys = ["key1", "key2"]
    }
}

```

### 多表写入

#### MySQL CDC Source

```hocon
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

    table-names = ["seatunnel.role", "seatunnel.user", "galileo.Bucket"]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    generate_sink_sql = true

    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

#### JDBC Source

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "123456"
    generate_sink_sql = true

    database = "${schema_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

#### Amazon Aurora DSQL

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
    Jdbc {
        dialect="Dsql"
        driver = "org.postgresql.Driver"
        url="jdbc:postgresql://ixxxxxxxxxxxxx.dsql.us-east-1.on.aws:5432/postgres"
        username = "admin"
        access_key_id = "ACCESSKEYIDEXAMPLE"
        secret_access_key = "SECRETACCESSKEYEXAMPLE"
        region = "us-east-1"
        database = "postgres"
        generate_sink_sql = true
        primary_keys = ["id"]
        max_retries = 3
        batch_size = 1000

    }
}
```

## 故障排查

### JDBC Sink 支持自动建表吗？

支持。通过 `schema_save_mode` 参数控制建表行为：

- `CREATE_SCHEMA_WHEN_NOT_EXIST`：表不存在时创建，已存在则跳过。
- `RECREATE_SCHEMA`：每次任务启动时删除并重建表。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：表不存在时抛出异常。
- `IGNORE`：跳过所有建表逻辑。

配合 `generate_sink_sql = true` 以及 `database`、`table` 参数可自动生成 INSERT/UPSERT SQL。

### 如何用 JDBC Sink 实现精确一次（exactly-once）？

JDBC Sink 通过 XA 事务支持 exactly-once：

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306/mydb"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "root"
    password = "password"
    max_retries = 0
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
    generate_sink_sql = true
    database = "mydb"
    table = "target_table"
    primary_keys = ["id"]
  }
}
```

不是所有数据库都支持 XA 事务，启用前请确认数据库和 JDBC 驱动均支持。

### 如何配置 Upsert（INSERT OR UPDATE）行为？

SeaTunnel 只有在最终拿到了主键/唯一键信息时，才会进入 upsert / update 路径。这个 key 既可以来自显式配置的 `primary_keys`，也可以在未显式配置时从上游 catalog 元数据里继承主键；如果没有主键，还会尝试继承第一组 unique key。

当最终存在 key 且 `enable_upsert = true` 时，SeaTunnel 会优先使用目标数据库方言支持的原生 upsert 语句。例如 PostgreSQL 会生成 `INSERT ... ON CONFLICT (...) DO UPDATE`（如果所有字段都是主键，没有可更新列，则退化为 `DO NOTHING`）：

```hocon
sink {
  jdbc {
    url = "jdbc:postgresql://localhost:5432/sales"
    driver = "org.postgresql.Driver"
    username = "postgres"
    password = "password"
    generate_sink_sql = true
    database = "sales"
    table = "public.orders"
    primary_keys = ["id"]
  }
}
```

当最终存在 key 但 `enable_upsert = false` 时，SeaTunnel 不再生成数据库原生 upsert 语句，而是回到按行类型执行的 insert/update 路径：

- `INSERT` 行执行普通 INSERT
- CDC `UPDATE_AFTER` 行执行 UPDATE
- CDC `DELETE` 行执行 DELETE

因此，`enable_upsert = false` 不适合依赖重复键自动覆盖的普通批量导入场景。

### 未显式配置 `primary_keys` 时会发生什么？

如果你没有显式配置 `primary_keys`，SeaTunnel 会先尝试从上游 catalog 元数据继承主键；如果没有主键，则再尝试继承第一组 unique key。

只有在“显式配置也没有、上游元数据里也没有可继承 key”时，JDBC Sink 才会退回普通 INSERT。进入这个无 key 模式后，不仅不会生成数据库原生 upsert 语句，Sink 也不会再使用按 `RowKind` 执行 UPDATE / DELETE 的写入器。对于 CDC 输入，这条写链路会实质上退化为普通 INSERT batching，重复键是否报错完全取决于目标表自身约束。

### 什么时候应该开启 `use_copy_statement`？

`use_copy_statement = true` 会让 JDBC Sink 直接优先走 `COPY <table> (...) FROM STDIN WITH CSV` 路径，而不是常规的 INSERT / UPSERT 语句。即使同时配置了 `primary_keys`，也会优先进入 COPY 路径。

这个选项更适合 PostgreSQL 大批量导入场景，但要同时满足下面几个前提：

- JDBC 驱动连接对象必须提供 `getCopyAPI()` 能力；否则任务会直接报错，并提示把 `use_copy_statement` 改回 `false`
- 它不是 `ON CONFLICT` 的替代品，不负责处理重复键覆盖逻辑
- 当前不支持 `MAP`、`ARRAY`、`ROW` 类型

### 如何在一个任务中写入多张表？

使用 `table = "${table_name}"`、`database = "${schema_name}"` 占位符，SeaTunnel 会从上游记录的元数据中解析实际值（与 CDC 数据源或多表配置配合使用）。搭配 `generate_sink_sql = true` 可实现全自动 SQL 生成。

### 为什么提示 JDBC 驱动未找到？

SeaTunnel 不内置所有 JDBC 驱动。Spark 和 Flink 需要把 JAR 放到每个执行节点的 `${SEATUNNEL_HOME}/plugins/Jdbc/lib/`；Zeta 需要放到每个 SeaTunnel 节点的 `${SEATUNNEL_HOME}/lib/`，然后重启受影响的进程。常见驱动文件名包括：

- MySQL：`mysql-connector-j-8.x.x.jar`
- PostgreSQL：`postgresql-42.x.x.jar`
- Oracle：`ojdbc8.jar`
- SQL Server：`mssql-jdbc-12.x.x.jre11.jar`

## 变更日志

<ChangeLog />
