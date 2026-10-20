import ChangeLog from '../changelog/connector-jdbc.md';

# OceanBase

> JDBC OceanBase 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 描述

通过 JDBC 读取 OceanBase 数据。OceanBase 支持 MySQL 兼容模式和 Oracle 兼容模式，因此 OceanBase 任务应将 `compatible_mode` 设置为 `mysql` 或 `oracle`。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动 | 连接串 | Maven |
|--------|-----------|------|--------|-------|
| OceanBase | 所有 OceanBase 服务器版本 | com.oceanbase.jdbc.Driver | jdbc:oceanbase://localhost:2883/test | [下载](https://mvnrepository.com/artifact/com.oceanbase/oceanbase-client) |

## 数据库依赖

> 请下载对应 'Maven' 的支持列表，并将其复制到 '$SEATUNNEL_HOME/plugins/jdbc/lib/' 工作目录<br/>
> 例如：cp oceanbase-client-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

### MySQL 模式

| MySQL 数据类型 | SeaTunnel 数据类型 |
|---------------|------------------|
| BIT(1)<br/>TINYINT(1) | BOOLEAN |
| TINYINT | BYTE |
| TINYINT<br/>TINYINT UNSIGNED | SMALLINT |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT | BIGINT |
| BIGINT UNSIGNED | DECIMAL(20,0) |
| DECIMAL(x,y)(<38) | DECIMAL(x,y) |
| DECIMAL(x,y)(>38) | DECIMAL(38,18) |
| DECIMAL UNSIGNED | DECIMAL |
| FLOAT<br/>FLOAT UNSIGNED | FLOAT |
| DOUBLE<br/>DOUBLE UNSIGNED | DOUBLE |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM | STRING |
| DATE | DATE |
| TIME | TIME |
| DATETIME<br/>TIMESTAMP | TIMESTAMP |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINAR<br/>BIT(n)<br/>GEOMETRY | BYTES |

### Oracle 模式

| Oracle 数据类型 | SeaTunnel 数据类型 |
|---------------|------------------|
| Integer | DECIMAL(38,0) |
| Number(p), p <= 9 | INT |
| Number(p), p <= 18 | BIGINT |
| Number(p), p > 18 | DECIMAL(38,18) |
| Number(p,s) | DECIMAL(p,s) |
| Float | DECIMAL(38,18) |
| REAL<br/> BINARY_FLOAT | FLOAT |
| BINARY_DOUBLE | DOUBLE |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>VARCHAR2<br/>NVARCHAR2<br/>NCLOB<br/>CLOB<br/>LONG<br/>XML<br/>ROWID | STRING |
| DATE | TIMESTAMP |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE | TIMESTAMP |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE | BYTES |
| UNKNOWN | 暂不支持 |

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | JDBC 连接的 URL。参考示例：jdbc:oceanbase://localhost:2883/test |
| driver | String | 是 | - | 用于连接到远程数据源的 jdbc 类名，应为 `com.oceanbase.jdbc.Driver`。 |
| username | String | 否 | - | 连接实例用户名 |
| password | String | 否 | - | 连接实例密码 |
| compatible_mode | String | 是 | - | OceanBase 的兼容模式，可以是 'mysql' 或 'oracle'。 |
| query | String | 否 | - | 查询语句。`query`、`table_path`、`table_list` 三者至少配置一个。 |
| table_path | String | 否 | - | 完整表路径，可替代 `query` 使用，例如 `test.source`。 |
| table_list | Array | 否 | - | 待读取的表列表，用于多表读取。每个表配置中可以包含 `table_path`、`query`、`partition_column` 等表级参数。 |
| where_condition | String | 否 | - | 所有表或查询共用的过滤条件，必须以 `where` 开头，例如 `where id > 100`。 |
| connection_check_timeout_sec | Int | 否 | 30 | 等待用于验证连接的数据库操作完成的时间（秒） |
| partition_column | String | 否 | - | 用于并行性分割的列名，仅支持数值类型列和字符串类型列。 |
| partition_lower_bound | BigDecimal | 否 | - | partition_column 的最小值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound | BigDecimal | 否 | - | partition_column 的最大值用于扫描，如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num | Int | 否 | job parallelism | 分片数量，仅支持正整数。使用 `table_path` 读取时，推荐通过 `split.size` 控制单个分片大小。 |
| fetch_size | Int | 否 | 0 | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，以通过减少满足选择条件所需的数据库命中次数来提高性能。零表示使用 jdbc 默认值。 |
| split.size | Int | 否 | 8096 | 使用 `table_path` 读取时，每个分片包含的行数。 |
| split.even-distribution.factor.lower-bound | Double | 否 | 0.05 | 判断分片键数据是否均匀分布的下限。 |
| split.even-distribution.factor.upper-bound | Double | 否 | 100 | 判断分片键数据是否均匀分布的上限。 |
| split.sample-sharding.threshold | Int | 否 | 1000 | 数据分布不均时，触发采样分片的预估分片数阈值。 |
| split.inverse-sampling.rate | Int | 否 | 1000 | 采样分片使用的采样率分母。 |
| split.allow-sampling | Boolean | 否 | true | 是否允许使用采样分片策略。 |
| split.string_split_mode | String | 否 | sample | 字符串分片算法，可选 `sample`、`charset_based`。 |
| split.string-strategy | String | 否 | - | 字符串分片策略，可选 `none`、`hash`、`range`、`auto`。 |
| split.string_split_mode_collate | String | 否 | - | `split.string_split_mode` 为 `charset_based` 时使用的排序规则。 |
| use_select_count | Boolean | 否 | false | 动态分片阶段使用 `select count` 统计行数，主要用于 Oracle 兼容读取场景。 |
| skip_analyze | Boolean | 否 | false | 动态分片阶段跳过表行数分析，主要用于 Oracle 兼容读取场景。 |
| use_regex | Boolean | 否 | false | 是否将 `table_path` 当作正则表达式匹配表。 |
| decimal_type_narrowing | Boolean | 否 | true | Oracle 兼容模式下，在不丢失精度时将 Decimal 收窄为 INT 或 BIGINT。 |
| int_type_narrowing | Boolean | 否 | true | MySQL 兼容模式下，在不丢失精度时将 `TINYINT(1)` 收窄为 BOOLEAN。 |
| dialect | String | 否 | - | 指定 JDBC 方言。OceanBase 通常会根据 URL 自动识别，只有特殊兼容场景才需要显式配置。 |
| properties | Map | 否 | - | 其他连接配置参数，当 properties 和 URL 具有相同参数时，优先级由驱动程序的具体实现确定。例如，在 MySQL 中，properties 优先于 URL。 |
| common-options | | 否 | - | 源插件通用参数，详见 [源通用选项](../common-options/source-common-options.md)。 |

### 提示

> `query`、`table_path`、`table_list` 三者至少配置一个。
>
> 如果未设置 `partition_column`，并且 SeaTunnel 无法从表元数据中找到合适的主键或唯一键，则源端会以单并发读取。配置了支持的分片列后，SeaTunnel 可以并行读取。
>
> OceanBase MySQL 模式的 JDBC URL 通常会带上 `rewriteBatchedStatements=true` 等 MySQL 兼容参数；OceanBase Oracle 模式需要使用 Oracle 兼容租户，并配置 `compatible_mode = "oracle"`。

## 任务示例

### 简单

```
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
  }
}

transform {
    # 如果您想了解有关如何配置 seatunnel 的更多信息并查看完整的转换插件列表，
    # 请访问 https://seatunnel.apache.org/docs/transforms/sql
}

sink {
    Console {}
}
```

### 并行

> 使用您配置的分片字段和分片数据并行读取查询表。如果您想读取整个表，可以这样做

```
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    # 并行分片读取字段
    partition_column = "id"
    # 分片数量
    partition_num = 10
  }
}
sink {
  Console {}
}
```

### 并行边界

> 根据您配置的上下边界读取数据源更高效

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
    username = "root"
    password = ""
    compatible_mode = "mysql"
    query = "select * from source"
    partition_column = "id"
    partition_num = 10
    # 读取开始边界
    partition_lower_bound = 1
    # 读取结束边界
    partition_upper_bound = 500
  }
}
```

### 表路径读取

希望 SeaTunnel 自动发现表结构并进行分片时，可以使用 `table_path`。

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_path = "test.source"
    split.size = 8096
  }
}
```

### Oracle 兼容模式

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/TESTUSER"
    username = "TESTUSER@test"
    password = ""
    compatible_mode = "oracle"
    query = "SELECT ID, NAME, CREATE_TIME FROM SOURCE"
  }
}
```

### 多表读取

```
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_list = [
      {
        table_path = "test.source_1"
      },
      {
        table_path = "test.source_2"
      }
    ]
    where_condition = "where id > 100"
  }
}
```

### 单表自定义 SQL

当 `table_list` 中的多张表需要不同的 SQL 过滤或投影时，可以在每个条目上单独设置 `query`，让 SeaTunnel 直接按这条 SQL 读取，跳过表元数据查找。

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_list = [
      {
        table_path = "test.orders"
        query = "select id, amount, status from orders where status = 'PAID'"
      },
      {
        table_path = "test.refunds"
        query = "select id, order_id, amount from refunds where amount > 0"
      }
    ]
  }
}
```

### 表名正则匹配

当 OceanBase 租户中存在大量结构相似的表（例如按时间分区的 `orders_2024_q1`、`orders_2024_q2` ...），设置 `use_regex = true` 并在 `table_path` 中传入正则表达式。SeaTunnel 会枚举匹配的表，并按 `partition_num` 进行并行读取。

```hocon
source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    table_path = "test.orders_2024_q[1-4]"
    use_regex = true
    partition_column = "id"
    partition_num = 8
  }
}
```

### 流式增量区间读取

OceanBase Source 本质上是一个批连接器。设置 `job.mode = "STREAMING"` 只用于开启 checkpoint 以便在失败时恢复作业；source 本身仍然是有界的，每次作业只会读取一次配置好的 `[partition_lower_bound, partition_upper_bound)` 区间。如需周期性地拉取新增数据，必须在外部重新提交作业（例如按计划滑动区间窗口），或改用 OceanBase CDC 做持续变更捕获。

```hocon
env {
  parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Jdbc {
    driver = "com.oceanbase.jdbc.Driver"
    url = "jdbc:oceanbase://localhost:2883/test"
    username = "root@test"
    password = ""
    compatible_mode = "mysql"
    query = "select * from orders where id >= ? and id < ?"
    partition_column = "id"
    partition_lower_bound = 1
    partition_upper_bound = 1000000
    partition_num = 16
  }
}
```

## 变更日志

<ChangeLog />
