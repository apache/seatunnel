# Opengauss CDC

> Opengauss CDC source connector

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

The Opengauss CDC connector allows for reading snapshot data and incremental data from Opengauss database. This document
describes how to set up the Opengauss CDC connector to run SQL queries against Opengauss databases.

## Using steps

> Here are the steps to enable CDC (Change Data Capture) in Opengauss:

1. Ensure the wal_level is set to logical, you can use SQL commands to modify the configuration directly:

```sql
ALTER SYSTEM SET wal_level TO 'logical';
SELECT pg_reload_conf();
```

2. Change the REPLICA policy of the specified table to FULL

```sql
ALTER TABLE your_table_name REPLICA IDENTITY FULL;
```

If you have multi tables,you can use the result of this sql to change the REPLICA policy of all tables to FULL

```sql
select 'ALTER TABLE ' || schemaname || '.' || tablename || ' REPLICA IDENTITY FULL;' from pg_tables where schemaname = 'YourTableSchema'
```

## Data Type Mapping

|                                   Opengauss Data type                                   |                                                              SeaTunnel Data type                                                               |
|-----------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOL<br/>                                                                               | BOOLEAN                                                                                                                                        |
| BYTEA<br/>                                                                              | BYTES                                                                                                                                          |
| INT2<br/>SMALLSERIAL<br/>INT4<br/>SERIAL<br/>                                           | INT                                                                                                                                            |
| INT8<br/>BIGSERIAL<br/>                                                                 | BIGINT                                                                                                                                         |
| FLOAT4<br/>                                                                             | FLOAT                                                                                                                                          |
| FLOAT8<br/>                                                                             | DOUBLE                                                                                                                                         |
| NUMERIC(Get the designated column's specified column size>0)                            | DECIMAL(Get the designated column's specified column size,Gets the number of digits in the specified column to the right of the decimal point) |
| NUMERIC(Get the designated column's specified column size<0)                            | DECIMAL(38, 18)                                                                                                                                |
| BPCHAR<br/>CHARACTER<br/>VARCHAR<br/>TEXT<br/>GEOMETRY<br/>GEOGRAPHY<br/>JSON<br/>JSONB | STRING                                                                                                                                         |
| TIMESTAMP<br/>                                                                          | TIMESTAMP                                                                                                                                      |
| TIME<br/>                                                                               | TIME                                                                                                                                           |
| DATE<br/>                                                                               | DATE                                                                                                                                           |
| OTHER DATA TYPES                                                                        | NOT SUPPORTED YET                                                                                                                              |

## Source Options

|                      Name                      |   Type   | Required | Default  | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------|----------|----------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | Yes      | -        | The URL of the JDBC connection. Refer to a case: `jdbc:postgresql://localhost:5432/postgres_cdc?loggerLevel=OFF`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| username                                       | String   | Yes      | -        | Username of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| password                                       | String   | Yes      | -        | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | No       | -        | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| table-names                                    | List     | Yes      | -        | Table name of the database to monitor. The table name needs to include the database name, for example: `database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                             | List     | No       | -        | Table config list. for example: [{"table": "db1.schema1.table1","primaryKeys":["key1"]}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| startup.mode                                   | Enum     | No       | INITIAL  | Optional startup mode for Opengauss CDC consumer, valid enumerations are `initial`, `earliest`, `latest`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.                                                                                                                                                                                                                                                                                                 |
| snapshot.split.size                            | Integer  | No       | 8096     | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                            | Integer  | No       | 1024     | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| slot.name                                      | String   | No       | -        | The name of the Opengauss logical decoding slot that was created for streaming changes from a particular plug-in for a particular database/schema. The server uses this slot to stream events to the connector that you are configuring. Default is seatunnel.                                                                                                                                                                                                                                                                                                                                                       |
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
| format                                         | Enum     | No       | DEFAULT  | Optional output format for Opengauss CDC, valid enumerations are `DEFAULT`, `COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| debezium                                       | Config   | No       | -        | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/postgresql.adoc#connector-configuration-properties) to Debezium Embedded Engine which is used to capture data changes from Opengauss server.                                                                                                                                                                                                                                                                                                                                 |
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
  Opengauss-CDC {
    result_table_name = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    schema-names = ["inventory"]
    table-names = ["opengauss_cdc.inventory.opengauss_cdc_table_1","opengauss_cdc.inventory.opengauss_cdc_table_2"]
    base-url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc"
    decoding.plugin.name = "pgoutput"
  }
}

transform {

}

sink {
  jdbc {
    source_table_name = "customers_opengauss_cdc"
    url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc"
    driver = "org.postgresql.Driver"
    user = "dailai"
    password = "openGauss@123"

    compatible_mode="postgresLow"
    generate_sink_sql = true
    # You need to configure both database and table
    database = "opengauss_cdc"
    schema = "inventory"
    tablePrefix = "sink_"
    primary_keys = ["id"]
  }
}

```

### Support custom primary key for table

```
source {
  Opengauss-CDC {
    result_table_name = "customers_opengauss_cdc"
    username = "gaussdb"
    password = "openGauss@123"
    database-names = ["opengauss_cdc"]
    schema-names = ["inventory"]
    table-names = ["opengauss_cdc.inventory.full_types_no_primary_key"]
    base-url = "jdbc:postgresql://opengauss_cdc_e2e:5432/opengauss_cdc?loggerLevel=OFF"
    decoding.plugin.name = "pgoutput"
    exactly_once = true
    table-names-config = [
      {
        table = "opengauss_cdc.inventory.full_types_no_primary_key"
        primaryKeys = ["id"]
      }
    ]
  }
}
```

