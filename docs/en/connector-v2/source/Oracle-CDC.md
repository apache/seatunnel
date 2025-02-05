# Oracle CDC

> Oracle CDC source connector

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

The Oracle CDC connector allows for reading snapshot data and incremental data from Oracle database. This document
describes how to set up the Oracle CDC connector to run SQL queries against Oracle databases.

## Notice

The Debezium Oracle connector does not rely on the continuous mining option.  The connector is responsible for detecting log switches and adjusting the logs that are mined automatically, which the continuous mining option did for you automatically.
So, you can not set this property named `log.mining.continuous.mine` in the debezium.

## Supported DataSource Info

| Datasource |                    Supported versions                    |          Driver          |                  Url                   |                               Maven                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | Different dependency version has different driver class. | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## Database Dependency

### Install Jdbc Driver

#### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.
> 2. To support the i18n character set, copy the `orai18n.jar` to the `$SEATUNNEL_HOME/plugins/` directory.

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/lib/`.
> 2. To support the i18n character set, copy the `orai18n.jar` to the `$SEATUNNEL_HOME/lib/` directory.

### Enable Oracle Logminer

> To enable Oracle CDC (Change Data Capture) using Logminer in Seatunnel, which is a built-in tool provided by Oracle, follow the steps below:

#### Enabling Logminer without CDB (Container Database) mode.

1. The operating system creates an empty file directory to store Oracle archived logs and user tablespaces.

```shell
mkdir -p /opt/oracle/oradata/recovery_area
mkdir -p /opt/oracle/oradata/ORCLCDB
chown -R oracle /opt/oracle/***
```

2. Login as admin and enable Oracle archived logs.

```sql
sqlplus /nolog;
connect sys as sysdba;
alter system set db_recovery_file_dest_size = 10G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate;
startup mount;
alter database archivelog;
alter database open;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
archive log list;
```

3. Login as admin and create an account called logminer_user with the password "oracle", and grant it privileges to read tables and logs.

```sql
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
CREATE USER logminer_user IDENTIFIED BY oracle DEFAULT TABLESPACE logminer_tbs QUOTA UNLIMITED ON logminer_tbs;

GRANT CREATE SESSION TO logminer_user;
GRANT SELECT ON V_$DATABASE to logminer_user;
GRANT SELECT ON V_$LOG TO logminer_user;
GRANT SELECT ON V_$LOGFILE TO logminer_user;
GRANT SELECT ON V_$LOGMNR_LOGS TO logminer_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO logminer_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO logminer_user;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO logminer_user;
GRANT EXECUTE ON DBMS_LOGMNR TO logminer_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO logminer_user;
GRANT SELECT ANY TRANSACTION TO logminer_user;
GRANT SELECT ON V_$TRANSACTION TO logminer_user;
```

##### Oracle 11g is not supported

```sql
GRANT LOGMINING TO logminer_user;
```

##### Grant privileges only to the tables that need to be collected

```sql
GRANT SELECT ANY TABLE TO logminer_user;
GRANT ANALYZE ANY TO logminer_user;
```

#### To enable Logminer in Oracle with CDB (Container Database) + PDB (Pluggable Database) mode, follow the steps below:

1. The operating system creates an empty file directory to store Oracle archived logs and user tablespaces.

```shell
mkdir -p /opt/oracle/oradata/recovery_area
mkdir -p /opt/oracle/oradata/ORCLCDB
mkdir -p /opt/oracle/oradata/ORCLCDB/ORCLPDB1
chown -R oracle /opt/oracle/***
```

2. Login as admin and enable logging

```sql
sqlplus /nolog
connect sys as sysdba; # Password: oracle
alter system set db_recovery_file_dest_size = 10G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate
startup mount
alter database archivelog;
alter database open;
archive log list;
```

3. Executing in CDB

```sql
ALTER TABLE TEST.* ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE TEST.T2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

4. Creating debeziume account

> Operating in CDB

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLCDB as sysdba
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf'
 SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
```

> Operating in PDB

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLPDB1 as sysdba
 CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf'
   SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
 exit;
```

5. Operating in CDB

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLCDB as sysdba

CREATE USER c##dbzuser IDENTIFIED BY dbz
DEFAULT TABLESPACE logminer_tbs
QUOTA UNLIMITED ON logminer_tbs
CONTAINER=ALL;

GRANT CREATE SESSION TO c##dbzuser CONTAINER=ALL;
GRANT SET CONTAINER TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$DATABASE to c##dbzuser CONTAINER=ALL;
GRANT FLASHBACK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE_CATALOG_ROLE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ANY TRANSACTION TO c##dbzuser CONTAINER=ALL;
GRANT LOGMINING TO c##dbzuser CONTAINER=ALL;

GRANT CREATE TABLE TO c##dbzuser CONTAINER=ALL;
GRANT LOCK ANY TABLE TO c##dbzuser CONTAINER=ALL;
GRANT CREATE SEQUENCE TO c##dbzuser CONTAINER=ALL;

GRANT EXECUTE ON DBMS_LOGMNR TO c##dbzuser CONTAINER=ALL;
GRANT EXECUTE ON DBMS_LOGMNR_D TO c##dbzuser CONTAINER=ALL;

GRANT SELECT ON V_$LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOG_HISTORY TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_LOGS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$LOGFILE TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVED_LOG TO c##dbzuser CONTAINER=ALL;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO c##dbzuser CONTAINER=ALL;
GRANT analyze any TO debeziume_1 CONTAINER=ALL;

exit;
```

## Data Type Mapping

|                                   Oracle Data type                                   | SeaTunnel Data type |
|--------------------------------------------------------------------------------------|---------------------|
| INTEGER                                                                              | INT                 |
| FLOAT                                                                                | DECIMAL(38, 18)     |
| NUMBER(precision <= 9, scale == 0)                                                   | INT                 |
| NUMBER(9 < precision <= 18, scale == 0)                                              | BIGINT              |
| NUMBER(18 < precision, scale == 0)                                                   | DECIMAL(38, 0)      |
| NUMBER(precision == 0, scale == 0)                                                   | DECIMAL(38, 18)     |
| NUMBER(scale != 0)                                                                   | DECIMAL(38, 18)     |
| BINARY_DOUBLE                                                                        | DOUBLE              |
| BINARY_FLOAT<br/>REAL                                                                | FLOAT               |
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/> | STRING              |
| DATE                                                                                 | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                         | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                  | BYTES               |

## Source Options

|                      Name                      |   Type   | Required | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|------------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| base-url                                       | String   | Yes      | -       | The URL of the JDBC connection. Refer to a case: `idbc:oracle:thin:datasource01:1523:xe`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| username                                       | String   | Yes      | -       | Name of the database to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                       | String   | Yes      | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                                 | List     | No       | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| schema-names                                   | List     | No       | -       | Schema name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| table-names                                    | List     | Yes      | -       | Table name of the database to monitor. The table name needs to include the database name, for example: `database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                             | List     | No       | -       | Table config list. for example: [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                                   | Enum     | No       | INITIAL | Optional startup mode for Oracle CDC consumer, valid enumerations are `initial`, `earliest`, `latest` and `specific`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `earliest`: Startup from the earliest offset possible.<br/> `latest`: Startup from the latest offset.<br/> `specific`: Startup from user-supplied specific offsets.                                                                                                                                                                                                                       |
| startup.specific-offset.file                   | String   | No       | -       | Start from the specified binlog file name. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.pos                    | Long     | No       | -       | Start from the specified binlog file position. **Note, This option is required when the `startup.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| stop.mode                                      | Enum     | No       | NEVER   | Optional stop mode for Oracle CDC consumer, valid enumerations are `never`, `latest` or `specific`. <br/> `never`: Real-time job don't stop the source.<br/> `latest`: Stop from the latest offset.<br/> `specific`: Stop from user-supplied specific offset.                                                                                                                                                                                                                                                                                                                                                        |
| stop.specific-offset.file                      | String   | No       | -       | Stop from the specified binlog file name. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.specific-offset.pos                       | Long     | No       | -       | Stop from the specified binlog file position. **Note, This option is required when the `stop.mode` option used `specific`.**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| snapshot.split.size                            | Integer  | No       | 8096    | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                            | Integer  | No       | 1024    | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-time-zone                               | String   | No       | UTC     | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                             | Duration | No       | 30000   | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                            | Integer  | No       | 3       | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                           | Integer  | No       | 20      | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | No       | 100     | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No       | 0.05    | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                      | Integer  | No       | 1000    | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                          | Integer  | No       | 1000    | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| exactly_once                                   | Boolean  | No       | false   | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| use_select_count                               | Boolean  | No       | false   | Use select count for table count rather then other methods in full stage.In this scenario, select count directly is used when it is faster to update statistics using sql from analysis table                                                                                                                                                                                                                                                                                                                                                                                                                        |
| skip_analyze                                   | Boolean  | No       | false   | Skip the analysis of table count in full stage.In this scenario, you schedule analysis table sql to update related table statistics periodically or your table data does not change frequently                                                                                                                                                                                                                                                                                                                                                                                                                       |
| format                                         | Enum     | No       | DEFAULT | Optional output format for Oracle CDC, valid enumerations are `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| schema-changes.enabled                         | Boolean  | No       | false   | Schema evolution is disabled by default. Now we only support `add column`、`drop column`、`rename column` and `modify column`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| debezium                                       | Config   | No       | -       | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#connector-properties) to Debezium Embedded Engine which is used to capture data changes from Oracle server.                                                                                                                                                                                                                                                                                                                                                      |
| common-options                                 |          | no       | -       | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| decimal_type_narrowing                     | Boolean | No       | true            | Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now. Please refer to `decimal_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                                  |


### decimal_type_narrowing

Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now.

eg:

decimal_type_narrowing = true

| Oracle        | SeaTunnel |
|---------------|-----------|
| NUMBER(1, 0)  | Boolean   |
| NUMBER(6, 0)  | INT       |
| NUMBER(10, 0) | BIGINT    |

decimal_type_narrowing = false

| Oracle        | SeaTunnel      |
|---------------|----------------|
| NUMBER(1, 0)  | Decimal(1, 0)  |
| NUMBER(6, 0)  | Decimal(6, 0)  |
| NUMBER(10, 0) | Decimal(10, 0) |

## Task Example

### Simple

> Support multi-table reading

```conf
source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "system"
    password = "oracle"
    database-names = ["XE"]
    schema-names = ["DEBEZIUM"]
    table-names = ["XE.DEBEZIUM.FULL_TYPES", "XE.DEBEZIUM.FULL_TYPES2"]
    base-url = "jdbc:oracle:thin:@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

> Use the select count(*) instead of analysis table for count table rows in full stage
```conf
source {
# This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    use_select_count = true 
    username = "system"
    password = "oracle"
    database-names = ["XE"]
    schema-names = ["DEBEZIUM"]
    table-names = ["XE.DEBEZIUM.FULL_TYPES"]
    base-url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

> Use the select NUM_ROWS from all_tables for the table rows but skip the analyze table.

```conf
source {
# This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    skip_analyze = true 
    username = "system"
    password = "oracle"
    database-names = ["XE"]
    schema-names = ["DEBEZIUM"]
    table-names = ["XE.DEBEZIUM.FULL_TYPES"]
    base-url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

### Support custom primary key for table

```conf
source {
  Oracle-CDC {
    plugin_output = "customers"
    base-url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
    username = "system"
    password = "oracle"
    database-names = ["XE"]
    schema-names = ["DEBEZIUM"]
    table-names = ["XE.DEBEZIUM.FULL_TYPES"]
    table-names-config = [
      {
        table = "XE.DEBEZIUM.FULL_TYPES"
        primaryKeys = ["ID"]
      }
    ]
  }
}
```

### Support debezium-compatible format send to kafka

> Must be used with kafka connector sink, see [compatible debezium format](../formats/cdc-compatible-debezium-json.md) for details

## Changelog

- Add Oracle CDC Source Connector

### next version

