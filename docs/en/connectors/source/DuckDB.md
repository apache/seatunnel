import ChangeLog from '../changelog/connector-jdbc.md';

# DuckDB

> JDBC DuckDB Source Connector

## Description

Read data from a DuckDB database file through JDBC. DuckDB is an in-process SQL OLAP database, so the connector
talks to a local database file (`jdbc:duckdb:/path/to/database.db`) or an in-memory database; there is no
remote server. The connector supports both batch and streaming modes, parallel reads via `partition_column`,
and reading multiple tables in one job through `table_list`.

## Support DuckDB Version

- 0.8.x/0.9.x/0.10.x/1.x

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Reading an attached DuckLake catalog

DuckLake tables can be read through DuckDB JDBC after every connection attaches the lake. With a DuckDB JDBC driver that supports `session_init_sql_file` (verified with 1.3.1), put the following in `/etc/duckdb/lake-init.sql` on each worker:

```sql
/* DUCKDB_CONNECTION_INIT_BELOW_MARKER */
LOAD ducklake;
LOAD sqlite_scanner;
ATTACH IF NOT EXISTS 'ducklake:sqlite:/var/lib/ducklake/catalog.sqlite' AS lake (DATA_PATH '/var/lib/ducklake/data/');
```

Use `url = "jdbc:duckdb:/var/lib/duckdb/work.db;session_init_sql_file=/etc/duckdb/lake-init.sql"` and `table_path = "lake.main.events"` in the JDBC source. The three components are the attached catalog, schema, and table. Use `ATTACH IF NOT EXISTS` in the init file because a worker can open multiple connections to the same DuckDB database; a repeated plain `ATTACH` fails. The init file and required extensions must be available to every worker.

If the job uses only attached DuckLake tables, `url = "jdbc:duckdb:;session_init_sql_file=/etc/duckdb/lake-init.sql"` can use a connection-private in-memory DuckDB instance instead (Source/Sink and reconnect verified with JDBC 1.3.1). The lake still persists in its metadata database and data path. If you use a file-backed `work.db`, keep it private to one worker JVM; do not open the same file read-write from multiple worker processes or place it on a shared volume for that purpose. See [DuckDB concurrency](https://duckdb.org/docs/stable/connect/concurrency.html).

For an **existing** DuckLake with PostgreSQL metadata, replace the SQLite lines in the init file with `LOAD postgres` and, for example:

```sql
ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=lake_catalog host=pg.example.com port=5432'
    AS lake (METADATA_SCHEMA 'lake_meta');
```

`lake_catalog` is the PostgreSQL database, `lake_meta` is the PostgreSQL schema holding DuckLake metadata, and `lake.main.events` remains the DuckLake catalog/schema/table path. Create the PostgreSQL database and metadata schema before attaching. When **creating** a lake, also provide `DATA_PATH 's3://bucket/prefix/'`; DuckLake stores that location in its metadata, so a later connection to the existing lake can omit `DATA_PATH` (verified with DuckDB JDBC 1.3.1). PostgreSQL authentication and object-store credentials still have to be available to every worker; the metadata does not supply credentials. See the [DuckLake connection parameters](https://ducklake.select/docs/stable/duckdb/usage/connecting) for credential options, and keep secrets out of job configuration and version control. This is a batch JDBC path and does not add DuckLake-specific CDC or exactly-once guarantees.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Supported DataSource Info

| Datasource | Supported versions                                       | Driver                  | Url                              | Maven                                                                 |
|------------|----------------------------------------------------------|-------------------------|----------------------------------|-----------------------------------------------------------------------|
| DuckDB     | Different dependency version has different driver class. | org.duckdb.DuckDBDriver | jdbc:duckdb:/path/to/database.db | [Download](https://mvnrepository.com/artifact/org.duckdb/duckdb_jdbc) |

## Data Type Mapping

| DuckDB Data Type                                                    | SeaTunnel Data Type |
|---------------------------------------------------------------------|---------------------|
| BOOLEAN                                                             | BOOLEAN             |
| TINYINT                                                             | TINYINT             |
| UTINYINT<br/>SMALLINT                                               | SMALLINT            |
| USMALLINT<br/>INTEGER                                               | INT                 |
| UINTEGER<br/>BIGINT                                                 | BIGINT              |
| UBIGINT                                                             | DECIMAL(20,0)       |
| HUGEINT                                                             | DECIMAL(38,0)       |
| FLOAT                                                               | FLOAT               |
| DOUBLE                                                              | DOUBLE              |
| DECIMAL(x,y)(Get the designated column's specified column size.<38) | DECIMAL(x,y)        |
| DECIMAL(x,y)(Get the designated column's specified column size.>38) | DECIMAL(38,18)      |
| VARCHAR<br/>CHAR<br/>TEXT<br/>JSON<br/>UUID<br/>INTERVAL            | STRING              |
| DATE                                                                | DATE                |
| TIME                                                                | TIME                |
| TIMESTAMP<br/>TIMESTAMP WITH TIME ZONE                              | TIMESTAMP           |
| BLOB<br/>ARRAY<br/>STRUCT<br/>MAP                                   | BYTES               |

## Source Options

| Name                         | Type       | Required | Default         | Description                                                                                                                                                                                                                                                         |
|------------------------------|------------|----------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:duckdb:/path/to/database.db                                                                                                                                                                                   |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use DuckDB the value is `org.duckdb.DuckDBDriver`.                                                                                                                                       |
| username                     | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                       |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                        |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                     |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                  |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type primary key, and only can config one column.                                                                                                                                                 |
| partition_lower_bound        | String     | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                    |
| partition_upper_bound        | String     | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                    |
| partition_num                | Int        | No       | 10              | The number of partition count, only support positive integer. default value is 10                                                                                                                                                                                   |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects, you can configure<br/> the row fetch size used in the query to improve performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| properties                   | Map        | No       | -               | Additional connection configuration parameters, when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in DuckDB, properties take precedence over the URL.                    |
| table_path                   | String     | No       | -               | The path to the full path of table, you can use this configuration instead of `query`. <br/>examples: <br/>duckdb: "main.table1" <br/>                                                                                                                              |
| table_list                   | Array      | No       | -               | The list of tables to be read, you can use this configuration instead of `table_path` example: ```[{ table_path = "main.table1"}, {table_path = "main.table2", query = "select * id, name from main.table2"}]```                                                    |
| where_condition              | String     | No       | -               | Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`                                                                                                                                                          |
| split.size                   | Int        | No       | 8096            | The split size (number of rows) of table, captured tables are split into multiple splits when read of table.                                                                                                                                                        |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                   |

## Parallel Reader

The JDBC Source connector supports parallel reading of data from tables. SeaTunnel will use certain rules to split the data in the table, which will be handed over to readers for reading. The number of readers is determined by the `parallelism` option.

**Split Key Rules:**

1. If `partition_column` is not null, It will be used to calculate split. The column must in **Supported split data type**.
2. If `partition_column` is null, seatunnel will read the schema from table and get the Primary Key and Unique Index. If there are more than one column in Primary Key and Unique Index, The first column which in the **supported split data type** will be used to split data. For example, the table have Primary Key(nn guid, name varchar), because `guid` id not in **supported split data type**, so the column `name` will be used to split data.

**Supported split data type:**
* String
* Number(int, bigint, decimal, ...)
* Date

### Options Related To Split

#### split.size

How many rows in one split, captured tables are split into multiple splits when read of table.

#### partition_column [string]

The column name for split data.

#### partition_upper_bound [string]

The partition_column max value for scan, if not set SeaTunnel will query database get max value.

#### partition_lower_bound [string]

The partition_column min value for scan, if not set SeaTunnel will query database get min value.

#### partition_num [int]

> Not recommended for use, The correct approach is to control the number of split through `split.size`

How many splits do we need to split into, only support positive integer. default value is 10.

## Tips

> If the table can not be split (for example, the table has no Primary Key or Unique Index, and `partition_column` is not set), it will run in single concurrency.
>
> Use `table_path` to replace `query` for single table reading. If you need to read multiple tables, use `table_list`.

## Task Example

### Simple

> This example queries 'user_events' table in your test database in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.

```hocon
# Defining the runtime environment
env {
  parallelism = 4
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        query = "select * from user_events limit 16"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
    Console {}
}
```

### parallel by partition_column

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        query = "select * from user_events"
        partition_column = "id"
        split.size = 10000
        # Read start boundary
        #partition_lower_bound = ...
        # Read end boundary
        #partition_upper_bound = ...
    }
}

sink {
  Console {}
}
```

### parallel by Primary Key or Unique Index

> Configuring `table_path` will turn on auto split, you can configure `split.*` to adjust the split strategy

```hocon
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = ""
        password = ""
        table_path = "main.user_events"
        query = "select * from main.user_events"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### Parallel Boundary

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```hocon
source {
    Jdbc {
        url = "jdbc:duckdb:/tmp/test.db"
        driver = "org.duckdb.DuckDBDriver"
        connection_check_timeout_sec = 100
        username = "duckdb"
        password = ""
        # Define query logic as required
        query = "select * from user_events"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
        properties {
         threads=4
         memory_limit="4GB"
        }
    }
}
```

### Multiple table read

***Configuring `table_list` will turn on auto split, you can configure `split.*` to adjust the split strategy***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}
source {
  Jdbc {
    url = "jdbc:duckdb:/tmp/test.db"
    driver = "org.duckdb.DuckDBDriver"
    connection_check_timeout_sec = 100
    username = "duckdb"
    password = ""

    table_list = [
      {
        table_path = "main.table1"
      },
      {
        table_path = "main.table2"
        # Use query filetr rows & columns
        query = "select id, name from main.table2 where id > 100"
      }
    ]
    #where_condition= "where id > 100"
    #split.size = 8096
  }
}

sink {
  Console {}
}
```
### DuckLake snapshot consistency

JDBC source splits use separate reads; the connector does not automatically pin a DuckLake snapshot for the entire job. For a stable batch extraction, resolve a retained snapshot ID once and use the same `SNAPSHOT_VERSION` in the Source initialization script on every Worker:

```sql
ATTACH IF NOT EXISTS 'ducklake:postgres:dbname=ducklake_catalog host=metadata-host user=reader'
AS lake (METADATA_SCHEMA 'lake_meta', SNAPSHOT_VERSION 2);
```

Replace `2` with an existing snapshot ID from `SELECT * FROM lake.snapshots()`. Keep that snapshot available until the job and any retries complete. Use a separate initialization script for a writable Sink; the snapshot-pinned catalog is for historical reads. See [DuckLake time travel](https://ducklake.select/docs/stable/duckdb/usage/time_travel).

## Change Log

<ChangeLog />

Catalog discovery with `table_pattern` or a regular-expression `table_path` searches only the current DuckDB catalog. List attached-catalog tables explicitly with three-part `table_path` or `table_names` values, such as `lake.main.events`. Catalog matching is case-insensitive. `main` and `default` are reserved aliases for the current catalog; choose a different alias when attaching a lake.
