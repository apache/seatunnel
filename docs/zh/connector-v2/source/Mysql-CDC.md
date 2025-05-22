import ChangeLog from '../changelog/connector-cdc-mysql.md';

# MySQL CDC

> MySQL CDC 源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 描述

MySQL CDC 连接器允许从 MySQL 数据库读取快照数据和增量数据，本文件描述了如何设置 MySQL CDC 连接器，以便对 MySQL 数据库执行SQL 查询。

## 主要特性

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../concept/connector-v2-features.md)

## 支持的数据源信息

| 数据源   | 支持的版本                                                                                                                                                | 驱动                       |               Url                |                                Maven                                 |
|-------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |

## 使用依赖

### 安装 Jdbc 驱动

#### 对于 Flink 引擎

> 1. 你需要确保 [jdbc 驱动 jar包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经被放置在目录 `${SEATUNNEL_HOME}/plugins/`中。

#### 对于 SeaTunnel Zeta 引擎

> 1. 你需要确保 [jdbc 驱动 jar包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经被放置在目录 `${SEATUNNEL_HOME}/lib/`中。

### 创建 MySQL 用户

你必须为 Debezium MySQL 连接器 所监控的 所有数据库 创建一个具有适当权限的 MySQL 用户。

1. 创建用户:

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. 为用户授权必须的权限:

```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

3. 刷新权限使生效:

```sql
mysql> FLUSH PRIVILEGES;
```

### 启用 MySQL 二进制日志（Binlog）

你必须确保为 MySQL 复制启用二进制日志记录。二进制日志会记录事务更新，以供复制工具传播变更。

1. 检查 `log-bin` 选项是否开启:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| 字段名                    | 字段值          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | ON             |
| gtid_mode                | ON             |
| log_bin                  | ON             |
+--------------------------+----------------+
```

2. 如果 `log_bin` 选项未开启, 请使用下表中描述的属性配置您的 MySQL 服务器配置文件(`$MYSQL_HOME/mysql.cnf`):

```
# Enable binary replication log and set the prefix, expiration, and log format.
# The prefix is arbitrary, expiration can be short for integration tests but would
# be longer on a production system. Row-level info is required for ingest to work.
# Server ID is required, but this will vary on production systems
server-id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 10
binlog_format     = row
# mysql 5.6+ requires binlog_row_image to be set to FULL
binlog_row_image  = FULL

# optional enable gtid mode
# mysql 5.6+ requires gtid_mode to be set to ON, but not required by mysql 8.0+
gtid_mode = on
enforce_gtid_consistency = on
```

重启MySQL 服务

```shell
/etc/init.d/mysqld restart
```

4. 通过再次检查二进制日志状态来确认您的更改生效:

MySQL 5.5:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| 字段名                    | 字段值          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| log_bin                  | ON             |
+--------------------------+----------------+
```

MySQL 5.6+:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| 字段名                    | 字段值          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | ON             |
| gtid_mode                | ON             |
| log_bin                  | ON             |
+--------------------------+----------------+
```
MySQL 8.0+:
```sql
show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency')
+--------------------------+----------------+
| 字段名                    | 字段值          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | OFF            |
| gtid_mode                | OFF            |
| log_bin                  | ON             |
+--------------------------+----------------+  
     
```


### 注意

#### 设置 MySQL 会话超时时间

当为大型数据库创建初始一致快照时，已建立的连接可能会在读取表的过程中超时。您可以通过在 MySQL 配置文件中配置 interactive_timeout 和 wait_timeout 来防止这种情况。
- `interactive_timeout`: 服务器在关闭一个 交互式连接 之前等待其活动的时长。 更多细节可查看文档： [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout) 
- `wait_timeout`: 服务器在关闭 非交互式连接 之前等待其活动的时长。 更多细节可查看文档： [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout)

*获取更多数据库的配置信息： [Debezium MySQL Connector](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#setting-up-mysql)*

## 数据类型映射

| Mysql 数据类型                                                                                      | SeaTunnel 数据类型 |
|-------------------------------------------------------------------------------------------------|----------------|
| BIT(1)<br/>TINYINT(1)                                                                           | BOOLEAN        |
| TINYINT                                                                                         | TINYINT        |
| TINYINT UNSIGNED<br/>SMALLINT                                                                   | SMALLINT       |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR             | INT            |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                    | BIGINT         |
| BIGINT UNSIGNED                                                                                 | DECIMAL(20,0)  |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED        | DECIMAL(p,s)   |
| FLOAT<br/>FLOAT UNSIGNED                                                                        | FLOAT          |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                           | DOUBLE         |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON            | STRING         |
| DATE                                                                                            | DATE           |
| TIME(s)                                                                                         | TIME(s)        |
| DATETIME<br/>TIMESTAMP(s)                                                                       | TIMESTAMP(s)   |
| BINARY<br/>VARBINARY<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB <br/>GEOMETRY | BYTES          |

## 源选项

| 名称                                             | 类型       | 必需 | 默认      | 描述                                                                                                                                                                                                                                |
|------------------------------------------------|----------|----|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | 是  | -       | JDBC 连接的 URL，参考案例 `jdbc:mysql://localhost:3306/test`。                                                                                                                                                                             |
| username                                       | String   | 是  | -       | 连接到数据库服务器时使用的用户名称。                                                                                                                                                                                                                |
| password                                       | String   | 是  | -       | 连接到数据库服务器时使用的用户密码。                                                                                                                                                                                                                |
| database-names                                 | List     | 否  | -       | 需要监听的数据库名。                                                                                                                                                                                                                        |Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| database-pattern                               | String   | 否  | .*      | 需要捕获的数据库名称正则表达式, 例如: `database_prefix.*`。                                                                                                                                                                                         |
| table-names                                    | List     | 是  | -       | 需要监控的数据库表名称。表名称需要包含数据库名称，例如： `database_name.table_name`。                                                                                                                                                                          |
| table-pattern                                  | String   | 是  | -       | 需要捕获的表名称正则表达式。 表名称需要包含数据库名称，例如： `database.*\\.table_.*` 。                                                                                                                                                                         |
| table-names-config                             | List     | 否  | -       | 表配置列表。例如: [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]。                                                                                                                                |
| startup.mode                                   | Enum     | 否  | INITIAL | MySQL CDC 消费者的可选启动模式， 有效枚举值为 `initial`, `earliest`, `latest` 和 `specific`. <br/> `initial`:启动时同步历史数据，然后同步增量数据。<br/> `earliest`:从最早偏移量启动。<br/> `latest`:从最新偏移量启动。<br/> `specific`: 从用户提供的特定偏移量启动。                                  |
| startup.specific-offset.file                   | String   | 否  | -       | 从指定的binlog文件名开始。 **注意，仅当 `startup.mode` 选项使用 `specific` 时才需要此配置。**                                                                                                                                                                | **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| startup.specific-offset.pos                    | Long     | 否  | -       | 从指定的binlog文件位置开始。 **注意，仅当 `startup.mode` 选项使用 `specific` 时才需要此配置。**                                                                                                                                                               | **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| stop.mode                                      | Enum     | 否  | NEVER   | MySQL CDC 消费者可选的停止模式, 有效枚举值为 `never`, `latest` 或者 `specific`. <br/> `never`: 实时任务永不停止。<br/> `latest`: 从最新偏移量停止。 <br/> `specific`: 从用户提供的特定偏移量停止。                                                                                  |
| stop.specific-offset.file                      | String   | 否  | -       | 从指定的binlog文件名停止。 **注意, 仅当 `stop.mode` 选项使用 `specific`时才需要此配置。**                                                                                                                                                                   |
| stop.specific-offset.pos                       | Long     | 否  | -       | 从指定的binlog文件位置停止。 **注意, 仅当 `stop.mode` 选项使用 `specific`时才需要此配置。**                                                                                                                                                                  |
| snapshot.split.size                            | Integer  | 否  | 8096    | 表快照的拆分大小（行数），捕获的表在读取表快照时被拆分成多个拆分。                                                                                                                                                                                                 |
| snapshot.fetch.size                            | Integer  | 否  | 1024    | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                |
| server-id                                      | String   | 否  | -       | 这个数据库客户端的数字 ID 或数字 ID 范围, 数字ID的定义语法 类似于 `5400`, 数字ID的范围语法 类似于 `5400-5408`。 <br/> 每个ID必须在MySQL集群中所有当前运行的数据库进程中是唯一的。这个连接器作为 <br/> 一台服务器（使用这个唯一 ID）加入 MySQL 集群，以便能够读取二进制日志。<br/> 默认情况下，生成一个介于6500和2,148,492,146之间的随机数，但我们建议设置一个显式的值。 |
| server-time-zone                               | String   | 否  | UTC     | 数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。                                                                                                                                                                           |
| connect.timeout.ms                             | Duration | 否  | 30000   | 连接器在尝试连接到数据库服务器后应等待的最大时间，以防超时。                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | 否  | 3       | 连接器重试建立数据库服务器连接的最大重试次数。                                                                                                                                                                                                           |
| connection.pool.size                           | Integer  | 否  | 20      | JDBC 连接池大小。                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否  | 100     | 块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更大，则将认为该表分布不均匀，并且如果估计的分片数量超过`sample-sharding.threshold`指定的值，则将使用基于采样的分片策略。默认值为 100.0。                                        |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否  | 0.05    | 块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更小，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold`指定的值，则将使用基于采样的分片策略。默认值为 0.05。                                        |
| sample-sharding.threshold                      | Integer  | 否  | 1000    | 此配置指定触发采样分片策略的估计分片数量阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound`指定的范围, 且估计的分片数量（计算为近似行数 / 块大小）超过此阈值时, 将使用采样分片策略。这可以帮助更有效地处理大数据集。默认值为 1000 个分片。                 |
| inverse-sampling.rate                          | Integer  | 否  | 1000    | 在采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大数据集时，较低的采样率尤为有用。默认值为 1000。                                                                                                          |
| exactly_once                                   | Boolean  | 否  | false   | 启用精确一次语义。                                                                                                                                                                                                                         |
| format                                         | Enum     | 否  | DEFAULT | MySQL CDC可选的输出格式, 有效枚举为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`。                                                                                                                                                                     |
| schema-changes.enabled                         | Boolean  | 否  | false   | 模式演变在默认情况下是禁用的。当前我们仅支持 `add column`、`drop column`、`rename column` 和 `modify column`。                                                                                                                                              |
| debezium                                       | Config   | 否  | -       | 通过 [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#connector-properties) 传递给用于捕获 MySQL 服务器数据更改的 Debezium 嵌入式引擎。                             |
| common-options                                 |          | 否  | -       | 源插件的公共参数，请参阅 [Source Common Options](../source-common-options.md) 获取详细信息。                                                                                                                                                         |

## 任务示例

### 简单

> 支持多表读取

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/testdb"
    username = "root"
    password = "root@123"
    table-names = ["testdb.table1", "testdb.table2"]
    
    startup.mode = "initial"
  }
}

sink {
  Console {
  }
}
```

### 支持以与 Debezium 兼容的格式发送到 Kafka。

> 必须与 Kafka 连接器 sink 配合使用, 查看 [compatible debezium format](../formats/cdc-compatible-debezium-json.md) 获取详细信息。

### 支持自定义表的主键

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    base-url = "jdbc:mysql://localhost:3306/testdb"
    username = "root"
    password = "root@123"
    
    table-names = ["testdb.table1", "testdb.table2"]
    table-names-config = [
      {
        table = "testdb.table2"
        primaryKeys = ["id"]
      }
    ]
  }
}

sink {
  Console {
  }
}
```
### 支持模式演变
```
env {
  # 您可以在这里设置引擎配置
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    database = shop
    table = mysql_cdc_e2e_sink_table_with_schema_change_exactly_once
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
  }
}

```
### 支持多表读取的模式

> `table-pattern` 和 `table-names` 的配置是互斥的。


```hocon
env {
  # 您可以在这里设置引擎配置
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    database-pattern = "source.*"
    table-pattern = "source.*\\..*"
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306"
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />

