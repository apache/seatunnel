# SQL Server CDC

> Sql Server CDC source connector

## Support SQL Server Version

- server:2019 (Or later version for information only)

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key Features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

The Sql Server CDC connector allows for reading snapshot data and incremental data from SqlServer database. This document
describes how to setup the Sql Server CDC connector to run SQL queries against SqlServer databases.

## Supported DataSource Info

| Datasource |                      Supported versions                       |                    Driver                    |                              Url                              |                                 Maven                                 |
|------------|---------------------------------------------------------------|----------------------------------------------|---------------------------------------------------------------|-----------------------------------------------------------------------|
| SqlServer  | <li> server:2019 (Or later version for information only)</li> | com.microsoft.sqlserver.jdbc.SQLServerDriver | jdbc:sqlserver://localhost:1433;databaseName=column_type_test | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc |

## Using Dependency

### Install Jdbc Driver

#### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Data Type Mapping

|                         SQLserver Data Type                          | SeaTunnel Data Type |
|----------------------------------------------------------------------|---------------------|
| CHAR<br/>VARCHAR<br/>NCHAR<br/>NVARCHAR<br/>TEXT<br/>NTEXT<br/>XML   | STRING              |
| BINARY<br/>VARBINARY<br/>IMAGE                                       | BYTES               |
| INTEGER<br/>INT                                                      | INT                 |
| SMALLINT<br/>TINYINT                                                 | SMALLINT            |
| BIGINT                                                               | BIGINT              |
| FLOAT(1~24)<br/>REAL                                                 | FLOAT               |
| DOUBLE<br/>FLOAT(>24)                                                | DOUBLE              |
| NUMERIC(p,s)<br/>DECIMAL(p,s)<br/>MONEY<br/>SMALLMONEY               | DECIMAL(p, s)       |
| TIMESTAMP                                                            | BYTES               |
| DATE                                                                 | DATE                |
| TIME(s)                                                              | TIME(s)             |
| DATETIME(s)<br/>DATETIME2(s)<br/>DATETIMEOFFSET(s)<br/>SMALLDATETIME | TIMESTAMP(s)        |
| BOOLEAN<br/>BIT<br/>                                                 | BOOLEAN             |

## Source Options

|                      Name                      |   Type   | Required | Default |                                                                                                                                                                                                                                                                                                     Description                                                                                                                                                                                                                                                                                                      |
|------------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| username                                       | String   | Yes      | -       | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | Yes      | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                                    | List     | Yes      | -       | Table name is a combination of schema name and table name (databaseName.schemaName.tableName).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| table-names-config                             | List     | No       | -       | Table config list. for example: [{"table": "db1.schema1.table1","primaryKeys":["key1"]}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| base-url                                       | String   | Yes      | -       | URL has to be with database, like "jdbc:sqlserver://localhost:1433;databaseName=test".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| startup.mode                                   | Enum     | No       | INITIAL | Optional startup mode for SqlServer CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| startup.timestamp                              | Long     | No       | -       | Start from the specified epoch timestamp (in milliseconds).<br/> **Note, This option is required when** the **"startup.mode" option used `'timestamp'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| startup.specific-offset.file                   | String   | No       | -       | Start from the specified binlog file name. <br/>**Note, This option is required when the "startup.mode" option used `'specific'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| startup.specific-offset.pos                    | Long     | No       | -       | Start from the specified binlog file position.<br/>**Note, This option is required when the "startup.mode" option used `'specific'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| stop.mode                                      | Enum     | No       | NEVER   | Optional stop mode for SqlServer CDC consumer, valid enumerations are "never".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| stop.timestamp                                 | Long     | No       | -       | Stop from the specified epoch timestamp (in milliseconds). <br/>**Note, This option is required when the "stop.mode" option used `'timestamp'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| stop.specific-offset.file                      | String   | No       | -       | Stop from the specified binlog file name.<br/>**Note, This option is required when the "stop.mode" option used `'specific'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| stop.specific-offset.pos                       | Long     | No       | -       | Stop from the specified binlog file position.<br/>**Note, This option is required when the "stop.mode" option used `'specific'`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| incremental.parallelism                        | Integer  | No       | 1       | The number of parallel readers in the incremental phase.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| snapshot.split.size                            | Integer  | No       | 8096    | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshotof table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| snapshot.fetch.size                            | Integer  | No       | 1024    | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-time-zone                               | String   | No       | UTC     | The session time zone in database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| connect.timeout                                | Duration | No       | 30s     | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | No       | 3       | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                           | Integer  | No       | 20      | The connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100     | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05    | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                      | int      | No       | 1000    | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                          | int      | No       | 1000    | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| exactly_once                                   | Boolean  | No       | false   | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| debezium.*                                     | config   | No       | -       | Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from SqlServer server.<br/>See more about<br/>the [Debezium's SqlServer Connector properties](https://github.com/debezium/debezium/blob/1.6/documentation/modules/ROOT/pages/connectors/sqlserver.adoc#connector-properties)                                                                                                                                                                                                                                                                                    |
| format                                         | Enum     | No       | DEFAULT | Optional output format for SqlServer CDC, valid enumerations are "DEFAULT"、"COMPATIBLE_DEBEZIUM_JSON".                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| common-options                                 |          | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |

### Enable Sql Server CDC

1. Check whether the CDC Agent is enabled

> EXEC xp_servicecontrol N'querystate', N'SQLServerAGENT'; <br/>
> If the result is running, prove that it is enabled. Otherwise, you need to manually enable it

2. Enable the CDC Agent

> /opt/mssql/bin/mssql-conf setup

3. The result is as follows

> 1) Evaluation (free, no production use rights, 180-day limit)
> 2) Developer (free, no production use rights)
> 3) Express (free)
> 4) Web (PAID)
> 5) Standard (PAID)
> 6) Enterprise (PAID)
> 7) Enterprise Core (PAID)
> 8) I bought a license through a retail sales channel and have a product key to enter.

4. Set the CDC at the library level
   Set the library level below to enable CDC. At this level, all tables under the libraries of the enabled CDC automatically enable CDC

> USE TestDB; -- Replace with the actual database name <br/>
> EXEC sys.sp_cdc_enable_db;<br/>
> SELECT name, is_tracked_by_cdc  FROM sys.tables  WHERE name = 'table'; -- table Replace with the name of the table you want to check

## Task Example

### initiali read Simple

> This is a stream mode cdc initializes read table data will be read incrementally after successful read The following sql DDL is for reference only

```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  SqlServer-CDC {
    result_table_name = "customers"
    username = "sa"
    password = "Y.sa123456"
    startup.mode="initial"
    database-names = ["column_type_test"]
    table-names = ["column_type_test.dbo.full_types"]
    base-url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  }
}

transform {
}

sink {
  console {
    source_table_name = "customers"
  }
```

### increment read Simple

> This is an incremental read that reads the changed data for printing

```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  SqlServer-CDC {
   # Set up accurate one read
    exactly_once=true 
    result_table_name = "customers"
    username = "sa"
    password = "Y.sa123456"
    startup.mode="latest"
    database-names = ["column_type_test"]
    table-names = ["column_type_test.dbo.full_types"]
    base-url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
  }
}

transform {
}

sink {
  console {
    source_table_name = "customers"
  }
```

### Support custom primary key for table

```
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  SqlServer-CDC {
    base-url = "jdbc:sqlserver://localhost:1433;databaseName=column_type_test"
    username = "sa"
    password = "Y.sa123456"
    database-names = ["column_type_test"]
    
    table-names = ["column_type_test.dbo.simple_types", "column_type_test.dbo.full_types"]
    table-names-config = [
      {
        table = "column_type_test.dbo.full_types"
        primaryKeys = ["id"]
      }
    ]
  }
}

sink {
  console {
  }
```

