# PostgreSQL CDC

> PostgreSQL CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key features

- [ ] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Description

The Postgre CDC connector allows for reading snapshot data and incremental data from Postgre database. This document
describes how to set up the Postgre CDC connector to run SQL queries against Postgre databases.

## Supported DataSource Info

| Datasource |                     Supported versions                     |        Driver         |                  Url                  |                                  Maven                                   |
|------------|------------------------------------------------------------|-----------------------|---------------------------------------|--------------------------------------------------------------------------|
| PostgreSQL | Different dependency version has different driver class.   | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql) |
| PostgreSQL | If you want to manipulate the GEOMETRY type in PostgreSQL. | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | [Download](https://mvnrepository.com/artifact/net.postgis/postgis-jdbc)  |

## Using Dependency

### Install Jdbc Driver

#### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.postgresql/postgresql) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

Please download and put PostgreSQL driver in `${SEATUNNEL_HOME}/lib/` dir. For example: cp postgresql-xxx.jar `$SEATNUNNEL_HOME/lib/`

> Here are the steps to enable CDC (Change Data Capture) in PostgreSQL:

1. Ensure the wal_level is set to logical: Modify the postgresql.conf configuration file by adding "wal_level = logical",
   restart the PostgreSQL server for the changes to take effect.
   Alternatively, you can use SQL commands to modify the configuration directly:

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. Change the REPLICA policy of the specified table to FULL

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

## Data Type Mapping

|                                  PostgreSQL Data type                                   |                                                              SeaTunnel Data type                                                               |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                               | BOOLEAN                                                                                                                                        |
| _BOOL<br/>                                                                              | ARRAY&LT;BOOLEAN&GT;                                                                                                                           |
| BYTEA<br/>                                                                              | BYTES                                                                                                                                          |
| _BYTEA<br/>                                                                             | ARRAY&LT;TINYINT&GT;                                                                                                                           |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                           | INT                                                                                                                                            |
| _INT2<br/>_INT4<br/>                                                                    | ARRAY&LT;INT&GT;                                                                                                                               |
| INT8<br/>BIGSERIAL<br/>                                                                 | BIGINT                                                                                                                                         |
| _INT8<br/>                                                                              | ARRAY&LT;BIGINT&GT;                                                                                                                            |
| FLOAT4<br/>                                                                             | FLOAT                                                                                                                                          |
| _FLOAT4<br/>                                                                            | ARRAY&LT;FLOAT&GT;                                                                                                                             |
| FLOAT8<br/>                                                                             | DOUBLE                                                                                                                                         |
| _FLOAT8<br/>                                                                            | ARRAY&LT;DOUBLE&GT;                                                                                                                            |
| NUMERIC(Get the designated column's specified column size>0)                            | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)                            | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB | STRING                                                                                                                                         |
| _BPCHAR<br/>_CHARACTER<br/>_VARCHAR<br/>_TEXT                                           | ARRAY&LT;STRING&GT;                                                                                                                            |
| TIMESTAMP<br/>                                                                          | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                               | TIME                                                                                                                                           |
| DATE<br/>                                                                               | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                                        | NOT SUPPORTED YET                                                                                                                              |

## Source Options

|                      Name                      |   Type   | Required | Default  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------|----------|----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | Yes      | -        | The URL of the JDBC connection. Refer to a case: `jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| username                                       | String   | Yes      | -        | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | Yes      | -        | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | No       | -        | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                                    | List     | Yes      | -        | Table name of the database to monitor. The table name needs to include the database name, for example: `database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                             | List     | No       | -        | Table config list. for example: [{"table": "db1.schema1.table1","primaryKeys":["key1"]}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| startup.mode                                   | Enum     | No       | INITIAL  | Optional startup mode for PostgreSQL CDC consumer, valid enumerations are `initial`, `earliest` and `latest`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.                                                                                                                                                                                                                                                                                             |
| snapshot.split.size                            | Integer  | No       | 8096     | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                            | Integer  | No       | 1024     | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                      | String   | No       | -        | The name of the PostgreSQL logical decoding slot that was created for streaming changes from a particular plug-in for a particular database/schema. The server uses this slot to stream events to the connector that you are configuring. Default is seatunnel.                                                                                                                                                                                                                                                                                                                                                      |
| decoding.plugin.name                           | String   | No       | pgoutput | The name of the Postgres logical decoding plug-in installed on the server,Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,wal2json_rds_streaming and pgoutput.                                                                                                                                                                                                                                                                                                                                                                                                                          |
| server-time-zone                               | String   | No       | UTC      | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                             | Duration | No       | 30000    | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | No       | 3        | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                           | Integer  | No       | 20       | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100      | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05     | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                      | Integer  | No       | 1000     | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                          | Integer  | No       | 1000     | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| exactly_once                                   | Boolean  | No       | false    | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| format                                         | Enum     | No       | DEFAULT  | Optional output format for PostgreSQL CDC, valid enumerations are `DEFAULT`, `COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| debezium                                       | Config   | No       | -        | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) to Debezium Embedded Engine which is used to capture data changes from PostgreSQL server.                                                                                                                                                                                                                                                                                                                                |
| common-options                                 |          | no       | -        | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## Task Example

### Simple

> Support multi-table reading

```


env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  Postgres-CDC {
    result_table_name = "customers_Postgre_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.postgres_cdc_table_1,postgres_cdc.inventory.postgres_cdc_table_2"]
    base-url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
  }
}

transform {

}

sink {
  jdbc {
    source_table_name = "customers_Postgre_cdc"
    url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "postgres"

    generate_sink_sql = true
    # You need to configure both database and table
    database = postgres_cdc
    schema = "inventory"
    tablePrefix = "sink_"
    primary_keys = ["id"]
  }
}
```

### Support custom primary key for table

```
source {
  Postgres-CDC {
    result_table_name = "customers_mysql_cdc"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres_cdc"]
    schema-names = ["inventory"]
    table-names = ["postgres_cdc.inventory.full_types_no_primary_key"]
    base-url = "jdbc:postgresql://postgres_cdc_e2e:5432/postgres_cdc?loggerLevel=OFF"
    decoding.plugin.name = "decoderbufs"
    exactly_once = false
    table-names-config = [
      {
        table = "postgres_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

## Changelog

- Add PostgreSQL CDC Source Connector

### next version

