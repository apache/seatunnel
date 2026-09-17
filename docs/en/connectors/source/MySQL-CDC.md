import ChangeLog from '../changelog/connector-cdc-mysql.md';

# MySQL CDC

> MySQL CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Description

The MySQL CDC connector allows for reading snapshot data and incremental data from MySQL database. This document
describes how to set up the MySQL CDC connector to run SQL queries against MySQL databases.

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

| Datasource |                                                                  Supported versions                                                                  |          Driver          |               Url                |                                Maven                                 |
|------------|------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------|----------------------------------|----------------------------------------------------------------------|
| MySQL      | <li> [MySQL](https://dev.mysql.com/doc): 5.5, 5.6, 5.7, 8.0.x </li><li> [RDS MySQL](https://www.aliyun.com/product/rds/mysql): 5.6, 5.7, 8.0.x </li> | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test | https://mvnrepository.com/artifact/mysql/mysql-connector-java/8.0.28 |

## Using Dependency

### Install Jdbc Driver

#### For Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

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

### Enabling the MySQL Binlog

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
```

2. If the value of `log_bin` is not `on`, configure your MySQL server configuration file(`$MYSQL_HOME/mysql.cnf`) with the following properties, which are described in the table below:

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

3. Restart MySQL Server

```shell
/etc/inint.d/mysqld restart
```

4. Confirm your changes by checking the binlog status once more:

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


### Notes

#### Setting up MySQL session timeouts

When an initial consistent snapshot is made for large databases, your established connection could timeout while the tables are being read. You can prevent this behavior by configuring interactive_timeout and wait_timeout in your MySQL configuration file.
- `interactive_timeout`: The number of seconds the server waits for activity on an interactive connection before closing it. See [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_interactive_timeout) for more details.
- `wait_timeout`: The number of seconds the server waits for activity on a non-interactive connection before closing it. See [MySQL’s documentation](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_wait_timeout) for more details.

*For more database settings see [Debezium MySQL Connector](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#setting-up-mysql)*

## Data Type Mapping

|                                        Mysql Data Type                                         | SeaTunnel Data Type |
|------------------------------------------------------------------------------------------------|---------------------|
| BIT(1)<br/>TINYINT(1)                                                                          | BOOLEAN             |
| TINYINT                                                                                        | TINYINT             |
| TINYINT UNSIGNED<br/>SMALLINT                                                                  | SMALLINT            |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR            | INT                 |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                   | BIGINT              |
| BIGINT UNSIGNED                                                                                | DECIMAL(20,0)       |
| DECIMAL(p, s) <br/>DECIMAL(p, s) UNSIGNED <br/>NUMERIC(p, s) <br/>NUMERIC(p, s) UNSIGNED       | DECIMAL(p,s)        |
| FLOAT<br/>FLOAT UNSIGNED                                                                       | FLOAT               |
| DOUBLE<br/>DOUBLE UNSIGNED<br/>REAL<br/>REAL UNSIGNED                                          | DOUBLE              |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>ENUM<br/>JSON<br/>ENUM  | STRING              |
| DATE                                                                                           | DATE                |
| TIME(s)                                                                                        | TIME(s)             |
| DATETIME<br/>TIMESTAMP(s)                                                                      | TIMESTAMP(s)        |
| BINARY<br/>VARBINAR<br/>BIT(p)<br/>TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB <br/>GEOMETRY | BYTES               |

## Source Options

| Name                                      | Type     | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | Yes      | -       | The URL of the JDBC connection. Refer to a case: `jdbc:mysql://localhost:3306/test`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| username                                  | String   | Yes      | -       | Username used to connect to the MySQL server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| password                                  | String   | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | No       | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| database-pattern                          | String   | No       | .*      | The database names RegEx of the database to capture, for example: `database_prefix.*`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| table-names                               | List     | Conditionally required | -       | Table names to monitor. Each value must include the database name, for example: `database_name.table_name`. Configure either `table-names` or `table-pattern`.                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| table-pattern                             | String   | Conditionally required | -       | Regular expression for table names to capture. Each matched table name includes the database name, for example: `database.*\\.table_.*`. Configure either `table-names` or `table-pattern`.                                                                                                                                                                                                                                                                                                                                                                                                                           |
| scan.newly-added-table.enabled            | Boolean  | No       | true    | Whether to scan tables newly captured by `database-pattern` or `table-pattern` after restoring from a checkpoint or savepoint. `true` adds newly matched tables to the snapshot phase; `false` keeps the checkpointed table set and does not snapshot newly matched tables. This is a new option: set it explicitly during an upgrade if restore-time discovery is not wanted.                                                                                                                                                                                                                                                                                   |
| scan.binlog.newly-added-table.enabled     | Boolean  | No       | false   | Whether to register and read newly created tables during the binlog reading phase. This option starts from the `CREATE TABLE` binlog position and does not snapshot historical data for those tables. The new table must match the configured capture pattern and use ordinary unquoted MySQL identifiers (`[A-Za-z_][A-Za-z0-9_$]*`). The downstream sink must be able to route or create the target table.                                                                                                                                                                                                 |
| table-names-config                        | List     | No       | -       | Per-table config list. For example: `[{"table": "db1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`. Use this when the table has no primary key, needs a custom primary key, or needs an explicit snapshot split column. `snapshotSplitColumn` should be a primary key or unique key. If a non-unique column is provided, SeaTunnel ignores it and automatically selects an appropriate split column internally.                                                                                                                                                                                                                                               |
| startup.mode                              | Enum     | No       | INITIAL | Optional startup mode for MySQL CDC consumer, valid enumerations are `initial`, `earliest`, `latest` , `specific` and `timestamp`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.<br/> `specific`: Startup from user-supplied specific offsets.<br/> `timestamp`: Startup from user-supplied timestamp.                                                                                                                                                  |
| startup.specific-offset.file              | String   | No       | -       | Start from the specified binlog file name. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.pos               | Long     | No       | -       | Start from the specified binlog file position. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| startup.specific-offset.gtid-set          | String   | No       | -       | Optional MySQL GTID set for `specific` startup mode. This option is used together with `startup.specific-offset.file` and `startup.specific-offset.pos`.                                                                                                                                                                                                                                                                                                                                                                                               |
| startup.specific-offset.skip-events       | Long     | No       | 0       | Number of binlog events to skip after the configured specific startup offset. This option can only be used when `startup.mode` is `specific`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.skip-rows         | Long     | No       | 0       | Number of rows to skip after the configured specific startup offset. This option can only be used when `startup.mode` is `specific`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| startup.timestamp                         | Long     | No       | -       | Start from the specified timestamp, in milliseconds since Unix epoch. **Note, This option is required when the `startup.mode` option uses `timestamp`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.mode                                 | Enum     | No       | NEVER   | Optional stop mode for MySQL CDC consumer, valid enumerations are `never`, `latest` or `specific`. <br/> `never`: Real-time job don't stop the source.<br/> `latest`: Stop from the latest offset.<br/> `specific`: Stop from user-supplied specific offset.                                                                                                                                                                                                                                                                                                                                                         |
| stop.specific-offset.file                 | String   | No       | -       | Stop from the specified binlog file name. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.specific-offset.pos                  | Long     | No       | -       | Stop from the specified binlog file position. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| snapshot.split.size                       | Integer  | No       | 8096    | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | No       | 1024    | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-id                                 | String   | No       | -       | Numeric ID or numeric ID range used by this CDC reader, for example `5400` or `5400-5408`. Each ID must be unique in the MySQL cluster. When the job has multiple readers or reads multiple tables in parallel, configure an ID range large enough for the job. If this option is omitted, SeaTunnel generates a random ID, but an explicit value is recommended for production.                                                                                                                 |
| server-time-zone                          | String   | No       | UTC     | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                        | Duration | No       | 30000   | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | No       | 3       | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | No       | 20      | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100     | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05    | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                 | Integer  | No       | 1000    | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                     | Integer  | No       | 1000    | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| split.allow-sampling                      | Boolean  | No       | true    | Whether to allow sampling-based sharding strategy. When set to false, the system will fall back to unevenly-sized chunk splitting (iterative query approach) regardless of the shard count. The default value is true. |
| enable_concurrent_read                    | Boolean  | No       | true    | Whether to enable concurrent read with split during the snapshot phase. When set to false, the source skips split analysis and reads the table as a single split, which is useful for tables without indexes. The default value is true. |
| exactly_once                              | Boolean  | No       | false   | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| format                                    | Enum     | No       | DEFAULT | Optional output format for MySQL CDC, valid enumerations are `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| schema-changes.enabled                    | Boolean  | No       | false   | Schema evolution is disabled by default. Now we only support `add column`、`drop column`、`rename column` and `modify column`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| schema-changes.include                     | List     | No       | -       | Only the listed schema change event types are sent downstream (when `schema-changes.enabled = true`). Empty means all are eligible. See [Schema change event filtering](#schema-change-event-filtering).                                                                                                                                                                                                                                                                                                                                                                                                              |
| schema-changes.exclude                     | List     | No       | -       | Schema change event types listed here are NOT sent downstream. Applied after `schema-changes.include`; exclude wins on conflict. See [Schema change event filtering](#schema-change-event-filtering).                                                                                                                                                                                                                                                                                                                                                                                                                  |
| debezium                                  | Config   | No       | -       | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#connector-properties) to Debezium Embedded Engine which is used to capture data changes from MySQL server.                                                                                                                                                                                                                                                                                                                                                        |
| int_type_narrowing                        | Boolean  | No       | true    | Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now. Please refer to `int_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                             |
| common-options                            |          | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

### Newly Added Tables

`scan.newly-added-table.enabled` is evaluated only when the job is restored from a checkpoint or savepoint. After restore, SeaTunnel compares the current captured tables with the checkpointed tables. With the default `true`, newly matched tables are added to the snapshot phase and historical data is read before the job continues to the binlog phase. Set it to `false` to retain only the table set recorded in the checkpoint. Because this option is new, existing wildcard jobs should set it explicitly when upgrading.

`scan.binlog.newly-added-table.enabled` works while the job is already reading binlog. It registers table metadata from the `CREATE TABLE` schema record and then reads following DML records for that table. It does not backfill rows that existed before the `CREATE TABLE` binlog position. Runtime-registered tables and their writer states are retained in the checkpoint and restored without a new snapshot.

At present, the JDBC at-least-once writer (`exactly_once = false`) implements runtime-created table support. Set `generate_sink_sql = true` when the JDBC sink must create the target table. Other sinks can reject a runtime table; creation and schema-application failures follow the configured multi-table failure policy. Grant the sink account only the permissions needed for the intended target schema and keep the capture pattern as narrow as possible.

For example, this job registers tables created under `source` after the job starts and creates matching tables in `sink`:

```
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://localhost:3306/source"
    username = "cdc_reader"
    password = "******"
    table-pattern = "source\\..*"
    schema-changes.enabled = true
    scan.binlog.newly-added-table.enabled = true
  }
}

sink {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/sink"
    username = "sink_writer"
    password = "******"
    database = "sink"
    table = "${database_name}_${table_name}"
    generate_sink_sql = true
    exactly_once = false
  }
}
```

### int_type_narrowing

Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now.

eg:

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

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

### Support debezium-compatible format send to kafka

> Must be used with kafka connector sink, see [compatible debezium format](../formats/cdc-compatible-debezium-json.md) for details

### Support custom primary key for table

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
### Support schema evolution
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

### Schema change event filtering

When `schema-changes.enabled = true`, you can further control which schema change event types are
propagated downstream using `schema-changes.include` / `schema-changes.exclude`. 

Use these SeaTunnel-owned canonical names:

| Canonical name  | Operation                                                                 |
|-----------------|---------------------------------------------------------------------------|
| `add.column`    | add a column                                                              |
| `drop.column`   | drop a column                                                             |
| `modify.column` | change a column's type/attributes, name unchanged                         |
| `change.column` | rename a column, optionally re-type                                       |
| `update.columns`| group alias for all four column-level changes above                        |

Precedence is deterministic:

1. if `schema-changes.include` is set, only included event types are eligible;
2. `schema-changes.exclude` is then applied;
3. **exclude wins** when a type appears in both lists.

```hocon
source {
  MySQL-CDC {
    # ...
    schema-changes.enabled = true
    schema-changes.include = ["add.column", "drop.column"]
    schema-changes.exclude = ["change.column"]
  }
}
```

**Data handling when `drop.column` is excluded.** For a retained **NOT NULL** column the `NULL` write is rejected
by the sink, so excluding `drop.column` for a NOT NULL column that the source has stopped supplying
will fail at the sink.

### Support table-pattern for multi-table reading

> `table-pattern` and `table-names` are mutually exclusive


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

### Configure Debezium heartbeat

For low-traffic tables, the MySQL binlog only advances when row changes occur. Use a Debezium heartbeat to keep the binlog position moving so downstream checkpoints record fresh offsets and replication lag stays measurable. The heartbeat table must exist on the MySQL server before the job starts.

```hocon
source {
  MySQL-CDC {
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    debezium {
      heartbeat.interval.ms = 100
      heartbeat.action.query = "INSERT INTO mysql_cdc.heartbeat (ts) VALUES (NOW())"
    }
  }
}
```

### Flush on a timer without waiting for `batch_size`

When the source has very low write volume, the JDBC sink can sit idle until a checkpoint fires. Enable timer flush in the sink so buffered rows are written even if `batch_size` is not reached.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 500
}

source {
  MySQL-CDC {
    server-id = 5680-5690
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.timer_flush_src"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
  }
}

sink {
  Jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    driver = "com.mysql.cj.jdbc.Driver"
    username = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    database = mysql_cdc
    table = timer_flush_sink
    primary_keys = ["id"]
    batch_size = 100000000
    batch_interval_ms = 0
  }
}
```

`sink.flush.interval` is configured in the `env` block and applies to the sink pipeline regardless of `batch_size`.

### Read tables without a primary key

Pick the path that matches what the source table guarantees:

- **Append-only workload** (no UPDATE/DELETE will ever be produced downstream): keep
  `exactly_once = false` and do not declare a primary key. The source falls back to a best-effort
  row identity. Without a usable key, the connector cannot apply UPDATE/DELETE events safely.
- **Unique non-primary column is available**: declare it via `table-names-config.primaryKeys` and
  set `exactly_once = true` so the snapshot and binlog phases both use the configured key for
  consistent row identity.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table_no_primary_key"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    table-names-config = [
      {
        table = "mysql_cdc.mysql_cdc_e2e_source_table_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
    exactly_once = true
  }
}
```

Without a usable primary key (configured or physical) the connector cannot safely apply UPDATE/DELETE events. Use this mode only for append-only workloads or when downstream sink behavior does not depend on row identity.

### Start From a Specific Binlog Offset

Use `startup.mode = "specific"` when the first record must be read from a known binlog file and position.

```hocon
source {
  MySQL-CDC {
    server-id = 5654
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    startup.mode = "specific"
    startup.specific-offset.file = "mysql-bin.000001"
    startup.specific-offset.pos = 154
  }
}
```

### Bounded Read: Stop at a Specific Binlog Offset

Use `stop.mode = "specific"` to make the job a bounded read: it reads the binlog between the
startup offset (or startup timestamp) and the configured stop offset, then terminates
(`FINISHED`) instead of running forever.

> **Note**: bounded-read termination is currently supported on the **Zeta** engine only.
> Flink and Spark engines do not support bounded incremental-split termination yet.

```hocon
source {
  MySQL-CDC {
    server-id = 5654
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    startup.mode = "specific"
    startup.specific-offset.file = "mysql-bin.000001"
    startup.specific-offset.pos = 154
    stop.mode = "specific"
    stop.specific-offset.file = "mysql-bin.000010"
    stop.specific-offset.pos = 4096
  }
}
```

`stop.mode = "specific"` can also be combined with `startup.mode = "timestamp"` to bound the
read both by time and by binlog position:

```hocon
source {
  MySQL-CDC {
    server-id = 5654
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
    startup.mode = "timestamp"
    startup.timestamp = 1716076800000
    stop.mode = "specific"
    stop.specific-offset.file = "mysql-bin.000010"
    stop.specific-offset.pos = 4096
  }
}
```

### Route Multiple Source Tables to JDBC

When one MySQL CDC source reads multiple tables, JDBC sink placeholders can keep the original table name.

```hocon
source {
  MySQL-CDC {
    plugin_output = "customers_mysql_cdc"
    server-id = 5652-5660
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["mysql_cdc.orders", "mysql_cdc.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
  }
}

sink {
  jdbc {
    plugin_input = "customers_mysql_cdc"
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc2"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    database = "mysql_cdc2"
    table = "${table_name}"
    primary_keys = ["${primary_key}"]
    generate_sink_sql = true
  }
}
```

## FAQ

### What MySQL permissions are required for CDC?

The MySQL user must have the following privileges:

```sql
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'user'@'%';
```

Also enable binary logging in `my.cnf` / `my.ini`:

```ini
[mysqld]
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

### Can SeaTunnel read CDC from a MySQL replica?

Yes. SeaTunnel subscribes to MySQL binary logs, which are also streamed to replicas. You can point SeaTunnel at a replica to offload the primary. Ensure the replica has binary logging enabled and `log_slave_updates = ON` set in its configuration.

### Does MySQL CDC support tables without primary keys?

By default, MySQL CDC expects primary keys. If the source table does not declare a primary key but
does have another unique column that can identify rows, you can override it with
`table-names-config.primaryKeys` as shown in the existing source options example. Without a stable
unique key, UPDATE and DELETE events cannot be applied safely downstream.

### How does the full snapshot phase work, and when does it switch to incremental reading?

On first startup, SeaTunnel takes a consistent full snapshot of the configured tables. After the snapshot completes, it automatically switches to reading binlog from the position recorded at the beginning of the snapshot, ensuring no events are lost during the transition.

### Does MySQL CDC support DDL propagation?

Yes, but only in a limited form. Enable `schema-changes.enabled = true`, then follow the current
schema evolution contract already documented on this page and in the
[Schema Evolution guide](../../introduction/configuration/schema-evolution.md). The current
documented support covers `add column`, `drop column`, `rename column`, and `modify column`.

### How do I avoid `server-id` conflicts when running multiple CDC jobs?

Each CDC job must use a unique `server-id` or a non-overlapping range. Duplicate `server-id` values cause the MySQL server to disconnect one of the clients. Assign distinct ranges, for example `5400-5600` for one job and `5601-5800` for another.

### Why is the initial snapshot very slow?

Snapshot speed depends on table size, JDBC fetch size, and network bandwidth. You can tune
`snapshot.split.size` and `snapshot.fetch.size` to control chunking and fetch behavior. For very
large tables where historical data is not needed, set `startup.mode = "latest"` to start from the
latest offset and skip the initial snapshot.

### How do I handle timezone and character set issues?

Set `server-time-zone` to match the MySQL server's timezone, for example `"Asia/Shanghai"`. For character set issues, append `characterEncoding=UTF-8&useUnicode=true` to the JDBC connection URL.

## See Also

For a production-grade end-to-end guide covering full + incremental synchronization lifecycle,
2PC sink configuration, schema evolution, and troubleshooting, see
[CDC Production Cookbook](../cdc-production-cookbook.md).

## Changelog

<ChangeLog />
