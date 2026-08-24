import ChangeLog from '../changelog/connector-cdc-db2.md';

# DB2 CDC

> DB2 CDC source connector

## Support DB2 Version

- DB2 LUW 11.5 or later versions supported by Debezium DB2 connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

The DB2 CDC connector reads snapshot data and incremental data from DB2 tables that have been put
into capture mode. It uses Debezium DB2 internally and can continue streaming committed INSERT,
UPDATE and DELETE changes after the initial snapshot is finished.

## Supported DataSource Info

| Datasource | Supported versions | Driver | Url | Maven |
|------------|--------------------|--------|-----|-------|
| DB2 | DB2 LUW 11.5 or later versions supported by Debezium DB2 connector | com.ibm.db2.jcc.DB2Driver | jdbc:db2://127.0.0.1:50000/testdb | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc |

## Using Dependency

### Install Jdbc Driver

#### For Spark/Flink Engine

> 1. You need to ensure that the [DB2 JDBC driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [DB2 JDBC driver jar package](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Data Type Mapping

| DB2 Data Type | SeaTunnel Data Type |
|---------------|---------------------|
| BOOLEAN | BOOLEAN |
| SMALLINT | SHORT |
| INT<br/>INTEGER | INT |
| BIGINT | BIGINT |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM | DECIMAL |
| REAL | FLOAT |
| DOUBLE<br/>DECFLOAT | DOUBLE |
| CHAR<br/>CHARACTER<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>DBCLOB<br/>XML | STRING |
| BINARY<br/>VARBINARY<br/>BLOB | BYTES |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |

## Source Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| username | String | Yes | - | User name used to connect to DB2. |
| password | String | Yes | - | Password used to connect to DB2. |
| url | String | Yes | - | DB2 JDBC URL. The URL must include a database name, for example `jdbc:db2://127.0.0.1:50000/testdb`. |
| database-names | List | No | The database parsed from `url` | Database name to monitor. DB2 CDC captures one database in one source. |
| table-names | List | Yes when `table-pattern` is not set | - | Table names to monitor, using `databaseName.schemaName.tableName`, for example `testdb.DB2INST1.CUSTOMERS`. |
| table-pattern | String | Yes when `table-names` is not set | - | Regular expression used to discover captured tables. |
| table-names-config | List | No | - | Table config list. For example: `[{"table": "testdb.DB2INST1.CUSTOMERS","primaryKeys": ["ID"],"snapshotSplitColumn": "ID"}]`. |
| startup.mode | Enum | No | INITIAL | Optional startup mode for DB2 CDC source. Valid values are `initial`, `earliest` and `latest`. |
| stop.mode | Enum | No | NEVER | Optional stop mode for DB2 CDC source. Valid value is `never`. |
| incremental.parallelism | Integer | No | 1 | The number of parallel readers in the incremental phase. |
| snapshot.split.size | Integer | No | 8096 | The split size of table snapshot. |
| snapshot.fetch.size | Integer | No | 1024 | The maximum fetch size for each poll when reading table snapshot. |
| server-time-zone | String | No | UTC | The session time zone in database server. |
| connect.timeout.ms | Duration | No | 30s | The maximum time that the connector should wait after trying to connect to the database server before timing out. |
| connect.max-retries | Integer | No | 3 | The maximum retry times to build database server connection. |
| connection.pool.size | Integer | No | 20 | The connection pool size. |
| chunk-key.even-distribution.factor.upper-bound | Double | No | 100 | The upper bound used to decide whether a split key is evenly distributed. |
| chunk-key.even-distribution.factor.lower-bound | Double | No | 0.05 | The lower bound used to decide whether a split key is evenly distributed. |
| sample-sharding.threshold | int | No | 1000 | The estimated shard count threshold that triggers sample-based sharding for unevenly distributed split keys. |
| inverse-sampling.rate | int | No | 1000 | The inverse sampling rate used by sample-based sharding. |
| exactly_once | Boolean | No | false | Enable exactly-once semantics for initial snapshot handoff. |
| debezium.* | config | No | - | Pass-through Debezium DB2 connector properties. |
| format | Enum | No | DEFAULT | Optional output format. Valid values are `DEFAULT` and `COMPATIBLE_DEBEZIUM_JSON`. |
| common-options |  | No | - | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details. |

### Enable DB2 CDC

DB2 CDC depends on DB2 SQL replication and ASN capture tables. Verify the required IBM replication
license for your environment before enabling capture. The database administrator must put every
source table into capture mode before running SeaTunnel. You can use DB2 control commands or
Debezium's management UDFs. The following commands show the common UDF workflow:

```sql
VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');
VALUES ASNCDC.ASNCDCSERVICES('start','asncdc');
CALL ASNCDC.ADDTABLE('DB2INST1', 'CUSTOMERS');
VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
```

For complete DB2 server setup, permissions and ASN capture agent configuration, refer to the
[Debezium DB2 connector setup guide](https://debezium.io/documentation/reference/1.9/connectors/db2.html).

## Task Example

### Initial Read Simple

> This example reads an initial snapshot and then continues to read incremental changes.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "initial"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### Incremental Read Simple

> This example starts from the latest DB2 LSN and prints newly changed data.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "latest"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}

sink {
  console {
    plugin_input = "customers"
  }
}
```

### Support Custom Primary Key For Table

```hocon
source {
  DB2-CDC {
    plugin_output = "customers"
    username = "db2inst1"
    password = "db2inst1"
    startup.mode = "initial"
    database-names = ["testdb"]
    table-names = ["testdb.DB2INST1.CUSTOMERS"]
    table-names-config = [
      {
        table = "testdb.DB2INST1.CUSTOMERS"
        primaryKeys = ["ID"]
        snapshotSplitColumn = "ID"
      }
    ]
    url = "jdbc:db2://127.0.0.1:50000/testdb"
  }
}
```

## Changelog

<ChangeLog />
