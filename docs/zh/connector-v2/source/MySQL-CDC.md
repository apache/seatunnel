import ChangeLog from '../changelog/connector-cdc-mysql.md';

# MySQL CDC

> MySQL CDC source 连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 描述

MySQL CDC连接器允许从MySQL数据库读取快照和增量数据. 本文档描述了如何配置MySQL CDC连接器以对MySQL数据库运行SQL查询.

## 主要功能

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持自定义分片](../../concept/connector-v2-features.md)

## 支持的数据源信息

| 数据源 |                                                                  支持的版本                                                                  |          Driver          |               Url                |                                Maven                                 |
|------------|------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL      | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |

## 依赖

### 安装Jdbc驱动

#### 对于Flink引擎

> 1. 你需要确保 [jdbc 驱动 jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在目录 `${SEATUNNEL_HOME}/plugins/`.

#### 对于SeaTunnel Zeta引擎

> 1. 你需要确保 [jdbc 驱动 jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在目录 `${SEATUNNEL_HOME}/lib/`.

### 创建MySQL用户

你必须定义一个MySQL用户，该用户对Debezium MySQL连接器所监控的所有数据库拥有适当的权限.

1. 创建MySQL用户:

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. 给用户赋予所需权限:

```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

3. 最终确定用户权限:

```sql
mysql> FLUSH PRIVILEGES;
```

### 启用MySQL Binlog

一定要为MySQL复制启用binlog。binlog记录事务更新以供复制工具传播更改.

1. 检查`log-bin`是否已经设置为on:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| Variable_name            | Value          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | ON             |
| gtid_mode                | ON             |
| log_bin                  | ON             |
+--------------------------+----------------+
```

2. 如果`log_bin`的值不是`on`, 配置你的MySQL server配置文件(`$MYSQL_HOME/mysql.cnf`)，配置文件中包含以下属性，这些属性在以下表格中有描述:

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

3. 重启MySQL Server

```shell
/etc/inint.d/mysqld restart
```

4. 修改之后再检查一次binlog的状态:

MySQL 5.5:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| Variable_name            | Value          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| log_bin                  | ON             |
+--------------------------+----------------+
```

MySQL 5.6+:

```sql
mysql> show variables where variable_name in ('log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'enforce_gtid_consistency');
+--------------------------+----------------+
| Variable_name            | Value          |
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
| Variable_name            | Value          |
+--------------------------+----------------+
| binlog_format            | ROW            |
| binlog_row_image         | FULL           |
| enforce_gtid_consistency | OFF            |
| gtid_mode                | OFF            |
| log_bin                  | ON             |
+--------------------------+----------------+  
     
```


### 提示

#### 配置MySQL session超时时长

当为大型数据库初始一致快照时，已建立的连接可能在读取表时超时。可以通过在MySQL配置文件中配置interactive_timeout（交互超时时间）和wait_timeout（等待超时时间）来防止这种行为.
- `interactive_timeout`: 服务器在关闭交互连接之前等待活动（交互操作）的秒数. 详见 [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: 服务器在关闭非交互式连接之前等待其活动的秒数. 详见 [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).

*更多的数据库配置，见 [Debezium MySQL Connector](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#setting-up-mysql)*

## 数据类型映射

|                                        Mysql数据类型                                         | SeaTunnel数据类型 |
|------------------------------------------------------------------------------------------------|---------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN       |
| TINYINT                                                                                        | TINYINT       |
| TINYINT UNSIGNED<br/>SMALLINT                                                                  | SMALLINT      |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT           |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT        |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0) |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED       | DECIMAL(p,s)  |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT         |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                          | DOUBLE        |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON<br/>ENUM  | STRING        |
| DATE                                                                                           | DATE          |
| TIME(s)                                                                                        | TIME(s)       |
| DATETIME<br/>TIMESTAMP(s)                                                                      | TIMESTAMP(s)  |
| BINARY<br/>VARBINAR<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB <br/>GEOMETRY | BYTES         |

## 配置参数选项

| 参数名称                                      | 类型       | 是否必须 | 默认值     | 描述                                                                                                                                                                                                                                           |
|-------------------------------------------|----------|------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | 是    | -       | JDBC连接的URL. 例如: `jdbc:mysql://localhost:3306/test`.                                                                                                                                                                                          |
| username                                  | String   | 是    | -       | 用来连接到数据库服务的数据库名称.                                                                                                                                                                                                                            |
| password                                  | String   | 是    | -       | 连接到数据库服务所使用的密码.                                                                                                                                                                                                                              |
| database-names                            | List     | 否    | -       | 要监控的数据库名称.                                                                                                                                                                                                                                   |
| database-pattern                          | String   | 否    | .*      | 要捕获的数据库名称的正则表达式, 例如: `database_prefix.*`.                                                                                                                                                                                                    |
| table-names                               | List     | 是    | -       | 要监控的表名. 表名需要包括库名, 例如: `database_name.table_name`                                                                                                                                                                                             |
| table-pattern                             | String   | 是    | -       | 要捕获的表名称的正则表达式. 表名需要包括库名, 例如: `database.*\\.table_.*`                                                                                                                                                                                         |
| table-names-config                        | List     | 否    | -       | 表配置的列表集合. 例如: [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]                                                                                                                                        |
| startup.mode                              | Enum     | 否    | INITIAL | MySQL CDC 消费者的可选启动模式, 有效枚举值为 `initial`, `earliest`, `latest` , `specific` 和 `timestamp`. <br/> `initial`: 启动时同步历史数据, 然后同步增量数据.<br/> `earliest`: 从尽可能最早的偏移量开始启动.<br/> `latest`: 从最近的偏移量启动.<br/> `timestamp`: 从用户提供的特定时间戳开始启动.                 |
| startup.specific-offset.file              | String   | 否    | -       | 从指定的binlog日志文件名开始. **注意, 当使用 `startup.mode` 选项为 `specific` 时，此选项为必填项.**                                                                                                                                                                      |
| startup.specific-offset.pos               | Long     | 否    | -       | 从指定的binlog日志文件位置开始. **注意, 当使用 `startup.mode` 选项为 `specific` 时，此选项为必填项.**                                                                                                                                                                     |
| startup.timestamp                         | Long     | No    | -       | 从指定的binlog时间戳文件位置开始. **注意, 当使用 `startup.mode` 选项为 `timestamp` 时，此选项为必填项.**                                                                                                                                                                    |
| stop.mode                                 | Enum     | 否    | NEVER   | MySQL CDC 消费者的可选停止模式, 有效枚举值为 `never`, `latest` 和 `specific`. <br/> `never`: 实时任务一直运行不停止.<br/> `latest`: 从最新的偏移量处停止.<br/> `specific`: 从用户提供的特定偏移量处停止.                                                                                         |
| stop.specific-offset.file                 | String   | 否    | -       | 从指定的binlog日志文件名停止. **注意, 当使用 `stop.mode` 选项为 `specific` 时，此选项为必填项.**                                                                                                                                                                         |
| stop.specific-offset.pos                  | Long     | 否    | -       | 从指定的binlog日志文件位置停止. **注意, 当使用 `stop.mode` 选项为 `specific` 时，此选项为必填项.**                                                                                                                                                                        |
| snapshot.split.size                       | Integer  | 否    | 8096    | 表快照的分割大小（行数）,读取表的快照时,被捕获的表会被分割成多个分割块.                                                                                                                                                                                                        |
| snapshot.fetch.size                       | Integer  | 否    | 1024    | 每次轮询读取表快照时的最大获取大小.                                                                                                                                                                                                                           |
| server-id                                 | String   | 否    | -       | 此数据库客户端的数字 ID 或数字 ID 范围, 数字 ID 的语法如 `5400`, 数字 ID 范围的语法如 '5400-5408'. <br/> 每个 ID 在 MySQL 集群中所有当前正在运行的数据库进程里必须是唯一的. 此连接加入 <br/> MySQL服务以另外一个服务的身份 (带有此唯一 ID) 以便于能够读取binlog. <br/> 默认情况下, 会生成一个介于 6500 到 2,148,492,146 之间的数字, 然而我们建议设置一个明确的值. |
| server-time-zone                          | String   | 否    | UTC     | 数据库服务中的会话时区. 如果没设置, 使用 ZoneId.systemDefault() 来确定服务的时区.                                                                                                                                                                                      |
| connect.timeout.ms                        | Duration | 否    | 30000   | 连接器在尝试连接数据库服务器后，在超时之前应等待的最长时间.                                                                                                                                                                                                               |
| connect.max-retries                       | Integer  | 否    | 3       | 连接器在构建数据库服务器连接时应重试的最大重试次数.                                                                                                                                                                                                                   |
| connection.pool.size                      | Integer  | 否    | 20      | jdbc连接池大小.                                                                                                                                                                                                                                   |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否    | 100     | 块键分布因子的上限. 该因子用于确定表数据是否分布均匀. 如果分布式因子计算结果小于或等于此上限 (即., (MAX(id) - MIN(id) + 1) / row count), 表的分块将被优化以实现均匀分布. 否则, 如果分布因子大于此上限, 该表将被视为分布不均, 并且如果估计的分片数量超过 `sample-sharding.threshold` 所指定的值, 则将使用基于采样的分片策略. 默认值是100.0.                         |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否    | 0.05    | 块键分布因子的下限. 该因子用于确定表数据是否分布均匀. 如果计算得出的分布因子大于或等于此下限 (即., (MAX(id) - MIN(id) + 1) / row count), 表的分块将被优化以实现均匀分布. 否则, 如果分布因子小于此下限, 该表将被视为分布不均, 并且如果预估的分片数量超过了 `sample-sharding.threshold` 所指定的值，则将使用基于采样的分片策略. 默认值是 0.05.                         |
| sample-sharding.threshold                 | Integer  | 否    | 1000    | 此配置指定了触发采样分片策略的预估分片数量阈值. 当分配因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 所指定的范围时, 如果估计的分片数量 (按近似行数/块大小 计算) 超过此阈值, 则将使用样本分片策略. 这有助于更高效地处理大型数据集. 默认值为 1000 分片.                    |
| inverse-sampling.rate                     | Integer  | 否    | 1000    | 采样分片策略中使用的采样率的倒数. 例如, 如果该值设置为 1000, 则表示在采样过程中应用了 1/1000 的采样率. 此选项在控制采样的粒度方面提供了灵活性, 从而影响最终的分片数量. 在处理非常大的数据集时非常有用, 因为此时更倾向于使用较低的采样率. 默认值为 1000.                                                                                                |
| exactly_once                              | Boolean  | 否    | false   | 启用精确一次语义.                                                                                                                                                                                                                                    |
| format                                    | Enum     | 否    | DEFAULT | MySQL CDC 的可选输出格式, 有效的枚举值为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                             |
| schema-changes.enabled                    | Boolean  | 否    | false   | 模式演进默认是禁用的. 当前我们只支持 `add column`、`drop column`、`rename column` 和 `modify column`.                                                                                                                                                            |
| debezium                                  | Config   | 否    | -       | 传递 [Debezium的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#connector-properties) 给Debezium嵌入式引擎, 该引擎用于捕获 MySQL 服务的数据变更.                                                  |
| int_type_narrowing                        | Boolean  | 否    | true    | Int类型收窄，如果为 true，则 tinyint(1) 类型将被收窄为 boolean 类型（如果没有精度损失）。目前仅支持 MySQL。                                                                                                                                                                      |
| common-options                            |          | 否    | -       | Source插件通用参数, 详见 [Source Common Options](../source-common-options.md)                                                                                                                                                                        |

### int_type_narrowing

Int类型收窄，如果为 true，则 tinyint(1) 类型将被收窄为 boolean 类型（如果没有精度损失）。目前仅支持 MySQL。

例：

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

## 任务示例

### 简单的示例

> 支持多表读取

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306/testdb"
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

### 支持向Kafka发送与Debezium兼容的格式

> 一定是使用kafka作为sink, 详见 [compatible debezium format](../formats/cdc-compatible-debezium-json.md)

### 支持表的自定义主键

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306/testdb"
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
### 支持模式演变（表结构变更）
```
env {
  # You can set engine configuration here
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
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
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
### 表名支持正则以读取多个表

> `table-pattern` 和 `table-names` 只能选择一个

```hocon
env {
  # You can set engine configuration here
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
    url = "jdbc:mysql://mysql_cdc_e2e:3306"
  }
}

sink {
  Console {
  }
}
```

## 更新日志

<ChangeLog />

