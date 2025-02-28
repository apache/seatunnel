# MySQL CDC

> MySQL CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 描述

MySQL CDC连接器允许从MySQL数据库读取快照和增量数据. 本文档描述了如何设置MySQL CDC连接器以针对MySQL数据库运行SQL查询.

## 主要功能

- [ ] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持自定义分片](../../concept/connector-v2-features.md)

## 支持的数据源信息

| 数据源   | 支持的版本                                                                                                                                                | 驱动                       |               Url                |                                Maven                                 |
|-------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |

## 依赖

### 安装Jdbc驱动

#### 对于Flink引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在目录 `${SEATUNNEL_HOME}/plugins/`.

#### 对于SeaTunnel Zeta引擎

> 1. 你需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 已经放在目录 `${SEATUNNEL_HOME}/lib/`.

### 创建MySQL用户

您必须定义一个MySQL用户，该用户对Debezium MySQL连接器所监控的所有数据库拥有适当的权限.

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

一定要为MySQL复制启用二进制日志。二进制日志记录事务更新以供复制工具传播更改.

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
5 rows in set (0.00 sec)
```

2. 如果以上结果不一致, 配置你的MySQL server配置文件(`$MYSQL_HOME/mysql.cnf`) ，配置文件中包含以下属性，这些属性在以下表格中有描述:

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

# enable gtid mode
# mysql 5.6+ requires gtid_mode to be set to ON
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
5 rows in set (0.00 sec)
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
5 rows in set (0.00 sec)
```

### 提示

#### 配置MySQL session超时时长

当为量大的数据库初始一致快照时，在读取表的过程中，您已建立的连接可能会超时。您可以通过在MySQL配置文件中配置interactive_timeout（交互超时时间）和wait_timeout（等待超时时间）来防止这种行为.
- `interactive_timeout`: 服务器在关闭交互连接之前等待活动（交互操作）的秒数. 详见[MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout).
- `wait_timeout`: 服务器在关闭非交互式连接之前等待其活动的秒数. 详见[MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout).

* 更多的数据库配置，见 [Debezium MySQL Connector](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#setting-up-mysql)*

## 数据类型映射

| Mysql数据类型                                                                                      | SeaTunnel数据类型 |
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

| 参数名称                                           | 类型     | 是否必须 | 默认值     | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|------------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost:3306:3306/test`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| username                                       | String   | Yes      | -       | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | No       | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| database-pattern                               | String   | No       | .*      | The database names RegEx of the database to capture, for example: `database_prefix.*`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| table-names                                    | List     | Yes      | -       | Table name of the database to monitor. The table name needs to include the database name, for example: `database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-pattern                                  | String   | Yes      | -       | The table names RegEx of the database to capture. The table name needs to include the database name, for example: `database.*\\.table_.*`                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names-config                             | List     | No       | -       | Table config list. for example: [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                                   | Enum     | No       | INITIAL | Optional startup mode for MySQL CDC consumer, valid enumerations are `initial`, `earliest`, `latest` and `specific`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.<br/> `specific`: Startup from user-supplied specific offsets.                                                                                                                                                                                                                        |
| startup.specific-offset.file                   | String   | No       | -       | Start from the specified binlog file name. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.pos                    | Long     | No       | -       | Start from the specified binlog file position. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| stop.mode                                      | Enum     | No       | NEVER   | Optional stop mode for MySQL CDC consumer, valid enumerations are `never`, `latest` or `specific`. <br/> `never`: Real-time job don't stop the source.<br/> `latest`: Stop from the latest offset.<br/> `specific`: Stop from user-supplied specific offset.                                                                                                                                                                                                                                                                                                                                                         |
| stop.specific-offset.file                      | String   | No       | -       | Stop from the specified binlog file name. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.specific-offset.pos                       | Long     | No       | -       | Stop from the specified binlog file position. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| snapshot.split.size                            | Integer  | No       | 8096    | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                            | Integer  | No       | 1024    | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-id                                      | String   | No       | -       | A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like `5400`, the numeric ID range syntax is like '5400-5408'. <br/> Every ID must be unique across all currently-running database processes in the MySQL cluster. This connector joins the <br/> MySQL cluster as another server (with this unique ID) so it can read the binlog. <br/> By default, a random number is generated between 6500 and 2,148,492,146, though we recommend setting an explicit value.                                                                                                                 |
| server-time-zone                               | String   | No       | UTC     | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                             | Duration | No       | 30000   | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | No       | 3       | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                           | Integer  | No       | 20      | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100     | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05    | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                      | Integer  | No       | 1000    | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                          | Integer  | No       | 1000    | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| exactly_once                                   | Boolean  | No       | false   | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| format                                         | Enum     | No       | DEFAULT | Optional output format for MySQL CDC, valid enumerations are `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| schema-changes.enabled                         | Boolean  | No       | false   | Schema evolution is disabled by default. Now we only support `add column`、`drop column`、`rename column` and `modify column`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| debezium                                       | Config   | No       | -       | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#connector-properties) to Debezium Embedded Engine which is used to capture data changes from MySQL server.                                                                                                                                                                                                                                                                                                                                                        |
| common-options                                 |          | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

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
### 表名支持正则以读取多个表
> `table-pattern` 和 `table-names`只能选择一个
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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306"
  }
}

sink {
  Console {
  }
}
```


## 更新日志

- 新增MySQL CDC源连接器

### 下个版本

