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

The PostgreSQL CDC connector allows for reading snapshot data and incremental data from PostgreSQL database. This document
describes how to set up the PostgreSQL CDC connector to run SQL queries against PostgreSQL databases.

## Supported DataSource Info

| Datasource |   Supported versions   |        Driver         |                  Url                  |                                Maven                                |
|------------|------------------------|-----------------------|---------------------------------------|---------------------------------------------------------------------|
| PostgreSQL | 9.6, 10, 11, 12 and 13 | org.postgresql.Driver | jdbc:postgresql://localhost:5432/test | https://mvnrepository.com/artifact/org.postgresql/postgresql/42.6.0 |

## Database Dependency

### Install Jdbc Driver

Please download and put PostgreSQL driver in `${SEATUNNEL_HOME}/lib/` dir. For example: cp postgresql-xxx.jar `$SEATNUNNEL_HOME/lib/`

### Configuring the PostgreSQL server

configure the replication slot regardless of the decoder being used, specify the following in the `postgresql.conf` file:

```properties
# REPLICATION
wal_level = logical 
```

### Setting up permissions

Setting up a PostgreSQL server to run a Debezium connector requires a database user that can perform replications. Replication can be performed only by a database user that has appropriate permissions and only for a configured number of hosts.

Although, by default, superusers have the necessary REPLICATION and LOGIN roles, as mentioned in Security, it is best not to provide the Debezium replication user with elevated privileges. Instead, create a Debezium user that has the minimum required privileges.

Prerequisites
1. PostgreSQL administrative permissions.

Procedure
1. provide a user with replication permissions, define a PostgreSQL role that has at least the REPLICATION and LOGIN permissions, and then grant that role to the user. For example:

```properties
CREATE ROLE <name> REPLICATION LOGIN;
```

### Configuring PostgreSQL to allow replication with the Debezium connector host

To enable Debezium to replicate PostgreSQL data, you must configure the database to permit replication with the host that runs the PostgreSQL connector. To specify the clients that are permitted to replicate with the database, add entries to the PostgreSQL host-based authentication file, pg_hba.conf. For more information about the pg_hba.conf file, see the PostgreSQL documentation.

- Procedure
  Add entries to the pg_hba.conf file to specify the Debezium connector hosts that can replicate with the database host. For example,

pg_hba.conf file example:

```properties
local   replication     <youruser>                          trust   
host    replication     <youruser>  127.0.0.1/32            trust   
host    replication     <youruser>  ::1/128                 trust   
```

### Replica identity

[REPLICA IDENTITY](https://www.postgresql.org/docs/current/static/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) is a PostgreSQL-specific table-level setting that determines the amount of information that is available to the logical decoding plug-in for `UPDATE` and `DELETE` events. More specifically, the setting of `REPLICA IDENTITY` controls what (if any) information is available for the previous values of the table columns involved, whenever an `UPDATE` or `DELETE` event occurs.

ALL table need set REPLICA IDENTITY

```sql
ALTER TABLE customers REPLICA IDENTITY FULL;
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

|                      name                      |   type   | required | default value |
|------------------------------------------------|----------|----------|---------------|
| base-url                                       | String   | Yes      | -             |
| username                                       | String   | Yes      | -             |
| password                                       | String   | Yes      | -             |
| database-names                                 | List     | Yes      | -             |
| table-names                                    | List     | Yes      | -             |
| startup.mode                                   | Enum     | No       | INITIAL       |
| startup.timestamp                              | Long     | No       | -             |
| startup.specific-offset.file                   | String   | No       | -             |
| startup.specific-offset.pos                    | Long     | No       | -             |
| stop.mode                                      | Enum     | No       | NEVER         |
| stop.timestamp                                 | Long     | No       | -             |
| stop.specific-offset.file                      | String   | No       | -             |
| stop.specific-offset.pos                       | Long     | No       | -             |
| incremental.parallelism                        | Integer  | No       | 1             |
| snapshot.split.size                            | Integer  | No       | 8096          |
| snapshot.fetch.size                            | Integer  | No       | 1024          |
| server-time-zone                               | String   | No       | UTC           |
| connect.timeout.ms                             | Duration | No       | 30000         |
| connect.max-retries                            | Integer  | No       | 3             |
| connection.pool.size                           | Integer  | No       | 20            |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 1000          |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05          |
| debezium.*                                     | config   | No       | -             |
| format                                         | Enum     | No       | DEFAULT       |
| common-options                                 |          | no       | -             |

### username [String]

Name of the database to use when connecting to the database server.

### password [String]

Password to use when connecting to the database server.

### database-name [String]

Database name of the database to monitor.

### schmea-list [List]

Schmea name of the database to monitor.

### table-names [List]

Table name of the database to monitor. The table name needs to include the schema name, for example: schema_name.table_name

### startup.mode [Enum]

Optional startup mode for postgres CDC consumer, valid enumerations are "initial", "earliest", "latest" and "specific".

### startup.timestamp [Long]

Start from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "startup.mode" option used `'timestamp'`.**

### startup.specific-offset.file [String]

Start from the specified binlog file name.

**Note, This option is required when the "startup.mode" option used `'specific'`.**

### startup.specific-offset.pos [Long]

Start from the specified binlog file position.

**Note, This option is required when the "startup.mode" option used `'specific'`.**

### stop.mode [Enum]

Optional stop mode for postgres CDC consumer, valid enumerations are "never".

### stop.timestamp [Long]

Stop from the specified epoch timestamp (in milliseconds).

**Note, This option is required when the "stop.mode" option used `'timestamp'`.**

### stop.specific-offset.file [String]

Stop from the specified binlog file name.

**Note, This option is required when the "stop.mode" option used `'specific'`.**

### stop.specific-offset.pos [Long]

Stop from the specified binlog file position.

**Note, This option is required when the "stop.mode" option used `'specific'`.**

### incremental.parallelism [Integer]

The number of parallel readers in the incremental phase.

### snapshot.split.size [Integer]

The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot
of table.

### snapshot.fetch.size [Integer]

The maximum fetch size for per poll when read table snapshot.

### server-time-zone [String]

The session time zone in database server.

### connect.timeout.ms [long]

The maximum time that the connector should wait after trying to connect to the database server before timing out.

### connect.max-retries [Integer]

The max retry times that the connector should retry to build database server connection.

### connection.pool.size [Integer]

The connection pool size.

### debezium [Config]

Pass-through Debezium's properties to Debezium Embedded Engine which is used to capture data changes from postgres server.

See more about
the [Debezium's postgres Connector properties](https://debezium.io/documentation/reference/1.6/connectors/postgresql.html#postgresql-connector-properties)

### format [Enum]

Optional output format for Postgres CDC, valid enumerations are "DEFAULT"、"COMPATIBLE_DEBEZIUM_JSON".

## Task Example

### Simple

> Support multi-table reading

```
env {
  execution.parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Postgres-CDC  {
    base-url = "jdbc:postgresql://localhost:5432/postgres"
    username = "postgres"
    password = "postgres"
    database-names = ["postgres"]
    table-names = ["postgres.public.mysql_cdc_source_table"]
   }
}

transform {
}

sink {
 Console {
  }
}

```

### Support debezium-compatible format send to kafka

> Must be used with kafka connector sink, see [compatible debezium format](../formats/cdc-compatible-debezium-json.md) for details

## Changelog

- Add PostgreSQL CDC Source Connector

### next version

