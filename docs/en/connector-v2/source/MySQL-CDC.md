# MySQL CDC

> MySQL CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document
describes how to set up the MySQL CDC connector to run SQL queries against MySQL databases.

## Supported DataSource Info

| Datasource |                                                               Supported versions                                                                |          Driver          |               Url                |                                Maven                                 |
|------------|-------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL      | <li> [MySQL](https://dev.mysql.com/doc): 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |

## Database Dependency

### Install Jdbc Driver

Please download and put mysql driver in `${SEATUNNEL_HOME}/lib/` dir. For example: cp mysql-connector-java-xxx.jar `$SEATNUNNEL_HOME/lib/`

### Creating MySQL user

You have to define a MySQL user with appropriate permissions on all databases that the Debezium MySQL connector monitors.

1. Create the MySQL user:

```sql
mysql> CREATE USER 'user'@'localhost' IDENTIFIED BY 'password';
```

2. Grant the required permissions to the user:

```sql
mysql> GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user' IDENTIFIED BY 'password';
```

3. Finalize the user’s permissions:

```sql
mysql> FLUSH PRIVILEGES;
```

### Enabling the MySQL binlog

You must enable binary logging for MySQL replication. The binary logs record transaction updates for replication tools to propagate changes.

1. Check whether the `log-bin` option is already on:

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

2. If inconsistent with the above results, configure your MySQL server configuration file(`$MYSQL_HOME/mysql.cnf`) with the following properties, which are described in the table below:

```
# Enable binary replication log and set the prefix, expiration, and log format.
# The prefix is arbitrary, expiration can be short for integration tests but would
# be longer on a production system. Row-level info is required for ingest to work.
# Server ID is required, but this will vary on production systems
server-id         = 223344
log_bin           = mysql-bin
expire_logs_days  = 10
binlog_format     = row
binlog_row_image  = FULL

# enable gtid mode
gtid_mode = on
enforce_gtid_consistency = on
```

3. Restart MySQL Server

```shell
/etc/inint.d/mysqld restart
```

4. Confirm your changes by checking the binlog status once more:

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

### Notes

#### Setting up MySQL session timeouts

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring interactive_timeout and wait_timeout in your MySQL configuration file.
- `interactive_timeout`: The number of seconds the server waits for activity on an interactive connection before closing it. See [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout) for more details.
- `wait_timeout`: The number of seconds the server waits for activity on a non-interactive connection before closing it. See [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout) for more details.

*For more database settings see [Debezium MySQL Connector](https://debezium.io/documentation/reference/1.6/connectors/mysql.html#setting-up-mysql)*

## Data Type Mapping

|                                     Mysql Data type                                      | SeaTunnel Data type |
|------------------------------------------------------------------------------------------|---------------------|
| BIT(1)<br/>TINYINT(1)                                                                    | BOOLEAN             |
| TINYINT                                                                                  | TINYINT             |
| TINYINT UNSIGNED<br/>SMALLINT                                                            | SMALLINT            |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR      | INT                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                             | BIGINT              |
| BIGINT UNSIGNED                                                                          | DECIMAL(20,0)       |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED | DECIMAL(p,s)        |
| FLOAT<br/>FLOAT UNSIGNED                                                                 | FLOAT               |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                    | DOUBLE              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON     | STRING              |
| DATE                                                                                     | DATE                |
| TIME                                                                                     | TIME                |
| DATETIME<br/>TIMESTAMP                                                                   | TIMESTAMP           |
| BINARY<br/>VARBINAR<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB         | BYTES               |

## Source Options

|                      Name                      |   Type   | Required | Default |                                                                                                                                                                                                                                                                                                     Description                                                                                                                                                                                                                                                                                                      |
|------------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost:3306:3306/test`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| username                                       | String   | Yes      | -       | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | No       | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                                    | List     | Yes      | -       | Table name of the database to monitor. The table name needs to include the database name, for example: `database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
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
| exactly_once                                   | Boolean  | No       | true    | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| format                                         | Enum     | No       | DEFAULT | Optional output format for MySQL CDC, valid enumerations are `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| debezium                                       | Config   | No       | -       | Pass-through [Debezium's properties](https://debezium.io/documentation/reference/1.6/connectors/mysql.html#mysql-connector-properties) to Debezium Embedded Engine which is used to capture data changes from MySQL server.                                                                                                                                                                                                                                                                                                                                                                                          |
| common-options                                 |          | no       | -       | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |

## Task Example

### Simple

> Support multi-table reading

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    catalog = {
      factory = MySQL
    }
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

### Support debezium-compatible format send to kafka

> Must be used with kafka connector sink, see [compatible debezium format](../formats/cdc-compatible-debezium-json.md) for details

## Changelog

- Add MySQL CDC Source Connector

### next version

