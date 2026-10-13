import ChangeLog from '../changelog/connector-cdc-oracle.md';

# Oracle CDC

> Oracle CDC source connector

## Support Those Engines

> SeaTunnel Zeta<br/>
> Flink <br/>

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

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
> 3. To capture `XMLTYPE` columns, also copy `xdb` and `xmlparserv2` (the same Oracle version as `ojdbc8`, for example 19.18) into `${SEATUNNEL_HOME}/plugins/`. See [XMLTYPE columns](#xmltype-columns).

#### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/lib/`.
> 2. To support the i18n character set, copy the `orai18n.jar` to the `$SEATUNNEL_HOME/lib/` directory.
> 3. To capture `XMLTYPE` columns, also copy `xdb` and `xmlparserv2` (the same Oracle version as `ojdbc8`, for example 19.18) into `${SEATUNNEL_HOME}/lib/`. See [XMLTYPE columns](#xmltype-columns).

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
-- Required when lob.reselect.enabled = true, so AS OF SCN can read the LOB
GRANT FLASHBACK ANY TABLE TO logminer_user;
GRANT ANALYZE ANY TO logminer_user;
```

#### To enable Logminer in Oracle with CDB (Container Database) + PDB (Pluggable Database) mode

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
| CHAR<br/>NCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/>XMLTYPE<br/> | STRING              |
| DATE                                                                                 | DATE                |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                         | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                  | BYTES               |

## Source Options

|                      Name                 |   Type   | Required  | Default | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|-----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | Yes       | -       | The URL of the JDBC connection. For example: `jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| username                                  | String   | Yes       | -       | Username used to connect to the Oracle database.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| password                                  | String   | Yes       | -       | Password to use when connecting to the database server.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | No        | -       | Database name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| schema-names                              | List     | No        | -       | Schema name of the database to monitor.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| table-names                               | List     | Conditionally required | -       | Table names to monitor. Each value should use `database.schema.table`, for example: `ORCLCDB.DEBEZIUM.FULL_TYPES`. Configure either `table-names` or `table-pattern`.                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| table-pattern                             | String   | Conditionally required | -       | Regular expression for table names to capture. Configure either `table-names` or `table-pattern`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| table-names-config                        | List     | No        | -       | Per-table config list. For example: `[{"table": "ORCLCDB.DEBEZIUM.FULL_TYPES","primaryKeys": ["ID"],"snapshotSplitColumn": "ID"}]`. Use this when the table has no primary key, needs a custom primary key, or needs an explicit snapshot split column.                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                              | Enum     | No        | INITIAL | Optional startup mode for Oracle CDC consumer, valid enumerations are `initial`, `latest`, `timestamp` and `specific`. <br/> `initial`: Synchronize historical data at startup, and then synchronize incremental data.<br/> `latest`: Start from the latest offset and skip the initial snapshot.<br/> `timestamp`: Start from the SCN resolved from `startup.timestamp`.<br/> `specific`: Start from user-supplied SCN.                                                                                                                                                                                           |
| startup.timestamp                         | Long     | No        | -       | Start from the specified timestamp (milliseconds since Unix epoch). This timestamp is converted with `server-time-zone` when `startup.mode = timestamp`. **Note, This option is required when the `startup.mode` option used `timestamp`.**                                                                                                                                                                                                                                                                                                                                                                        |
| startup.specific-offset.scn               | Long     | No        | -       | Start from the specified Oracle SCN. **Note, This option is required when the `startup.mode` option uses `specific`. The SCN must still be available to the selected Oracle log mining backend.**                                                                                                                                                                                                                                                                                                                                                                                                                    |
| stop.mode                                 | Enum     | No        | NEVER   | Optional stop mode for Oracle CDC consumer. The only valid value is `never`, so a streaming Oracle CDC source keeps running until the job is stopped.                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| snapshot.split.size                       | Integer  | No        | 8096    | The split size (number of rows) of table snapshot, captured tables are split into multiple splits when read the snapshot of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | No        | 1024    | The maximum fetch size for per poll when read table snapshot.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-time-zone                          | String   | No        | -       | The session time zone in database server. If not set, then ZoneId.systemDefault() is used to determine the server time zone. This value is also used when converting `startup.timestamp` to SCN. Set it explicitly when database time zone and JVM time zone are different.                                                                                                                                                                                                                                                                                                                                     |
| connect.timeout.ms                        | Long     | No        | 30000   | The maximum time that the connector should wait after trying to connect to the database server before timing out.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | No        | 3       | The max retry times that the connector should retry to build database server connection.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | No        | 20      | The jdbc connection pool size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| incremental.parallelism                   | Integer  | No        | 1       | Number of parallel readers used after the snapshot phase enters incremental log reading.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| chunk-key.even-distribution.factor.upper-bound | Double   | No        | 100     | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| chunk-key.even-distribution.factor.lower-bound | Double   | No        | 0.05    | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| sample-sharding.threshold                 | Integer  | No        | 1000    | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| inverse-sampling.rate                     | Integer  | No        | 1000    | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| split.allow-sampling                    | Boolean  | No        | true    | When set to false, the system should fall back to unevenly-sized chunk splitting (iterative query approach) regardless of the shard count.                                                                                                                                                              |
| enable_concurrent_read                  | Boolean  | No        | true    | Whether to enable concurrent read with split during the snapshot phase. When set to false, the source skips split analysis and reads the table as a single split, which is useful for tables without indexes. The default value is true. |
| exactly_once                              | Boolean  | No        | false   | Enable exactly once semantic.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| use_select_count                          | Boolean  | No        | false   | Use select count for table count rather then other methods in full stage.In this scenario, select count directly is used when it is faster to update statistics using sql from analysis table                                                                                                                                                                                                                                                                                                                                                                                                                        |
| skip_analyze                              | Boolean  | No        | false   | Skip the analysis of table count in full stage.In this scenario, you schedule analysis table sql to update related table statistics periodically or your table data does not change frequently                                                                                                                                                                                                                                                                                                                                                                                                                       |
| format                                    | Enum     | No        | DEFAULT | Optional output format for Oracle CDC, valid enumerations are `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| schema-changes.enabled                    | Boolean  | No        | false   | Schema evolution is disabled by default. Now we only support `add column`、`drop column`、`rename column` and `modify column`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| schema-changes.include                     | List     | No        | -       | Only the listed schema change event types are sent downstream (when `schema-changes.enabled = true`). Empty means all are eligible. See [Schema change event filtering](#schema-change-event-filtering).                                                                                                                                                                                                                                                                                                                                                                                                             |
| schema-changes.exclude                     | List     | No        | -       | Schema change event types listed here are NOT sent downstream. Applied after `schema-changes.include`; exclude wins on conflict. See [Schema change event filtering](#schema-change-event-filtering).                                                                                                                                                                                                                                                                                                                                                                                                                 |
| lob.enabled                               | Boolean  | No        | false   | Passed through to Debezium `lob.enabled`. `true` mines CLOB, NCLOB, BLOB, and XMLTYPE redo. When omitted, `debezium.lob.enabled` is left unchanged. An explicit value overrides it. See [LOB columns](#lob-columns). |
| lob.reselect.enabled                      | Boolean  | No        | false   | With `lob.enabled = true` and this option left false, a LOB column an UPDATE did not change, and LOB columns on DELETE and UPDATE_BEFORE, are the placeholder `__debezium_unavailable_value`. A sink that writes the whole row replaces the stored value with that placeholder. `true` re-selects those CLOB, NCLOB, BLOB, and XMLTYPE columns on INSERT and UPDATE_AFTER by primary key (`AS OF SCN` when `commit_scn` is present) and writes the Oracle value instead. DELETE and UPDATE_BEFORE placeholders become null. Requires a primary key. Rejected unless `lob.enabled` is true. Grants: [What Oracle permissions are required for CDC?](#what-oracle-permissions-are-required-for-cdc). |
| lob.unavailable-value.handling            | Enum     | No        | warn_and_keep | The placeholder remains when re-select is off, the table has no primary key, or the lookup fails, and a full-row sink write would store it. `warn_and_keep` (default) logs once per table and keeps the placeholder. `null` replaces it with null so the sink does not store the placeholder text. `fail` stops the task. `null` and `fail` are rejected unless `lob.enabled` is true. |
| debezium                                  | Config   | No        | -       | Pass-through [Debezium's properties](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#connector-properties) to Debezium Embedded Engine which is used to capture data changes from Oracle server.                                                                                                                                                                                                                                                                                                                                                      |
| common-options                            |          | no        | -       | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

## LOB columns

What you see: large values live outside the row. With `lob.enabled` unset, a streaming LOB insert is often null, and an update that only changes the LOB can be missed. Snapshot does not depend on `lob.enabled`: the split query is a normal JDBC `SELECT`, and CLOB, NCLOB, and BLOB columns are materialized from the JDBC locator on that row (no extra query per column).

Downstream SeaTunnel types are unchanged (STRING for character LOBs, BYTES for BLOB). Use target columns that can hold large values (for example CLOB or LONGTEXT, not a short VARCHAR). Snapshot and re-select materialize LOB values in memory; size the reader heap for your largest columns and split parallelism.

`lob.enabled = true` mines that redo and passes the value through to Debezium. If you omit it, `debezium.lob.enabled` still applies; an explicit `lob.enabled` overrides it. An `EMPTY_CLOB()` / `EMPTY_BLOB()` with no later write is an empty string or empty bytes. SQL `NULL` stays null. Events for the same row in one transaction are merged, and a later SQL `NULL` replaces the earlier value. The unavailable placeholder still means the column was absent from that redo record and is left unchanged. Setting `lob.reselect.enabled = true`, or `lob.unavailable-value.handling` to `null` or `fail`, while mining is off is rejected.

What you see next: with mining on and `lob.reselect.enabled` left false, a LOB column the UPDATE did not change, and LOB columns on DELETE and UPDATE_BEFORE, stay the placeholder `__debezium_unavailable_value`. A sink that writes the whole row replaces the stored value with that placeholder.

`lob.reselect.enabled = true` fills that value by primary key (`AS OF SCN` when `commit_scn` is present). If flashback fails (`ORA-01555`, `ORA-01466`, `ORA-08181`, `ORA-01031`), the current row is read. DELETE and UPDATE_BEFORE placeholders become null. A primary key is required, or a key in `table-names-config`. Grants are in [What Oracle permissions are required for CDC?](#what-oracle-permissions-are-required-for-cdc).

When the placeholder is still there (re-select is off, there is no primary key, or the lookup fails), `lob.unavailable-value.handling` decides what the sink receives. `warn_and_keep` (default) keeps the placeholder, `null` replaces it with null, and `fail` stops the task. The defaults `false` and `warn_and_keep` do not query Oracle and do not change the row.

`format = COMPATIBLE_DEBEZIUM_JSON` keeps the raw envelope, including the placeholder. A full job is in [Capture LOB columns](#capture-lob-columns).

## XMLTYPE columns

What you see: an `XMLTYPE` column on a relational table is stored like a LOB. Without `lob.enabled`, streaming events after the snapshot have no XML document, so the column is empty or null. With mining on, an XML column the statement did not change is the same placeholder, and a full-row sink write replaces the stored XML with it.

`lob.enabled = true` captures the column as STRING. Put `xdb` and `xmlparserv2` (same version as `ojdbc8`) next to the driver. See [Install Jdbc Driver](#install-jdbc-driver). Placeholders use the same re-select and handling as CLOB. If XML parsing fails after those jars are added, start the engine with `-Djavax.xml.parsers.SAXParserFactory=com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl`.

Not supported: a table whose row type is `XMLTYPE`, `XMLTYPE STORE AS CLOB`, and DDL that adds an `XMLTYPE` column. A snapshot still sees `XMLTYPE` columns that already exist.

## Task Example

### Simple

> Support multi-table reading

```conf
source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES", "ORCLCDB.DEBEZIUM.FULL_TYPES2"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    connection.pool.size = 1
    debezium {
      database.oracle.jdbc.timezoneAsRegion = "false"
    }
  }
}
```

### Capture LOB columns

`lob.enabled = true` mines LOB redo so a streaming insert or a LOB-only update is not lost. `lob.reselect.enabled = true` replaces the placeholder on columns the statement did not change, so an UPDATE or DELETE does not overwrite the sink value with `__debezium_unavailable_value`. `lob.unavailable-value.handling = "null"` keeps that placeholder out of the sink when the lookup cannot recover the value.

```sql
CREATE TABLE DEBEZIUM.LOB_TYPES (
  ID NUMBER(9) NOT NULL PRIMARY KEY,
  VAL_CLOB CLOB,
  VAL_BLOB BLOB,
  VAL_NCLOB NCLOB
);
ALTER TABLE DEBEZIUM.LOB_TYPES ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

```conf
source {
  Oracle-CDC {
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    username = "c##dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.LOB_TYPES"]
    lob.enabled = true
    lob.reselect.enabled = true
    lob.unavailable-value.handling = "null"
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
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
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
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
  }
}
```

### Support custom primary key for table

```conf
source {
  Oracle-CDC {
    plugin_output = "customers"
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    table-names-config = [
      {
        table = "ORCLCDB.DEBEZIUM.FULL_TYPES"
        primaryKeys = ["ID"]
      }
    ]
  }
}
```

### Enable exactly-once CDC

`exactly_once = true` is used with the default `startup.mode = "initial"` path. Use it when the downstream sink is also configured for exactly-once delivery, for example a JDBC sink with XA enabled.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Oracle-CDC {
    plugin_output = "customers"
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    exactly_once = true
    connection.pool.size = 1
    debezium {
      database.oracle.jdbc.timezoneAsRegion = "false"
    }
  }
}
```

### Start From Timestamp

Use `startup.mode = "timestamp"` to start from the Oracle SCN resolved from a Unix timestamp in milliseconds.

```hocon
source {
  Oracle-CDC {
    plugin_output = "customers"
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    startup.mode = "timestamp"
    startup.timestamp = 1700000000000
    server-time-zone = "UTC"
    debezium {
      database.oracle.jdbc.timezoneAsRegion = "false"
    }
  }
}
```

### Configure Debezium heartbeat

For low-traffic tables, the Oracle LogMiner SCN only advances when redo log changes occur. Use a Debezium heartbeat to keep the SCN moving so checkpoint offsets are recorded regularly and replication lag stays observable. The heartbeat table must exist on the Oracle server before the job starts.

```hocon
source {
  Oracle-CDC {
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    debezium {
      database.oracle.jdbc.timezoneAsRegion = "false"
      heartbeat.interval.ms = 100
      heartbeat.action.query = "INSERT INTO DEBEZIUM.heartbeat (ts) VALUES (SYSTIMESTAMP)"
    }
  }
}
```

### Read tables without a primary key

Pick the path that matches what the source table guarantees:

- **Append-only workload** (no UPDATE/DELETE will ever be produced downstream): keep
  `exactly_once = false` and do not declare a primary key. The source falls back to a best-effort
  row identity. Without a usable key, the connector cannot apply UPDATE/DELETE events safely.
- **Unique non-primary column is available**: declare it via `table-names-config.primaryKeys` and
  set `exactly_once = true` so the snapshot and redo-log phases both use the configured key for
  consistent row identity.

```hocon
source {
  Oracle-CDC {
    username = "system"
    password = "top_secret"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    url = "jdbc:oracle:thin:@//oracle-host:1521/ORCLCDB"
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES_NO_PRIMARY_KEY"]
    table-names-config = [
      {
        table = "ORCLCDB.DEBEZIUM.FULL_TYPES_NO_PRIMARY_KEY"
        primaryKeys = ["ID"]
      }
    ]
    exactly_once = true
  }
}
```

Without a usable primary key, the connector cannot safely apply UPDATE/DELETE events. Use this mode only for append-only workloads.

### Schema change event filtering

When `schema-changes.enabled = true`, you can further control which schema change event types are
propagated downstream using `schema-changes.include` / `schema-changes.exclude`.

Use these SeaTunnel-owned canonical names:

| Canonical name   | Operation                                            |
|------------------|------------------------------------------------------|
| `add.column`     | add a column                                         |
| `drop.column`    | drop a column                                        |
| `modify.column`  | change a column's type/attributes, name unchanged    |
| `change.column`  | rename a column, optionally re-type                  |
| `update.columns` | group alias for all four column-level changes above  |

Precedence is deterministic:

1. if `schema-changes.include` is set, only included event types are eligible;
2. `schema-changes.exclude` is then applied;
3. **exclude wins** when a type appears in both lists.

```hocon
source {
  Oracle-CDC {
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

### Support debezium-compatible format send to kafka

> Must be used with kafka connector sink, see [compatible debezium format](../formats/cdc-compatible-debezium-json.md) for details

## FAQ

### What Oracle permissions are required for CDC?

The LogMiner user needs the following privileges:

```sql
GRANT CREATE SESSION TO logminer_user;
GRANT SET CONTAINER TO logminer_user;
GRANT SELECT ON V_$DATABASE TO logminer_user;
GRANT FLASHBACK ANY TABLE TO logminer_user;
GRANT SELECT ANY TABLE TO logminer_user;
GRANT SELECT_CATALOG_ROLE TO logminer_user;
GRANT EXECUTE_CATALOG_ROLE TO logminer_user;
GRANT SELECT ANY TRANSACTION TO logminer_user;
GRANT LOGMINING TO logminer_user;
GRANT CREATE TABLE TO logminer_user;
GRANT LOCK ANY TABLE TO logminer_user;
GRANT CREATE SEQUENCE TO logminer_user;
GRANT EXECUTE ON DBMS_LOGMNR TO logminer_user;
GRANT EXECUTE ON DBMS_LOGMNR_D TO logminer_user;
GRANT SELECT ON V_$LOG TO logminer_user;
GRANT SELECT ON V_$LOG_HISTORY TO logminer_user;
GRANT SELECT ON V_$LOGMNR_LOGS TO logminer_user;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO logminer_user;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO logminer_user;
GRANT SELECT ON V_$LOGFILE TO logminer_user;
GRANT SELECT ON V_$ARCHIVED_LOG TO logminer_user;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO logminer_user;
GRANT SELECT ON V_$TRANSACTION TO logminer_user;
```

Also enable supplemental logging at the database and table level:

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
ALTER TABLE schema_name.table_name ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

### Does Oracle CDC support multi-tenant (CDB/PDB) databases?

Yes. Set `database-names` to the CDB name and configure the JDBC URL to point to the CDB root. The user must be a common user (prefixed with `C##`) with the above privileges granted in all containers (`CONTAINER = ALL`).

### Does Oracle CDC support tables without primary keys?

By default, Oracle CDC requires primary keys. You can specify a custom primary key column via `table-names-config` with the `primaryKeys` field if the table has a suitable unique column.

### How do I use a custom snapshot query?

Use Debezium's `snapshot.select.statement.overrides` properties inside the `debezium` block. The query is applied before SeaTunnel adds the snapshot-split boundaries, so it must include every column needed by the configured table schema and its split key.

```hocon
debezium {
  snapshot.select.statement.overrides = "DEBEZIUM.FULL_TYPES"
  snapshot.select.statement.overrides.DEBEZIUM.FULL_TYPES = "SELECT * FROM DEBEZIUM.FULL_TYPES WHERE ACTIVE = 1"
}
```

### How do I improve LogMiner performance?

Treat this primarily as a database and redo-log tuning topic. Reuse the LogMiner setup and
supplemental logging sections above first, enable logging only for the required tables, and add
Debezium passthrough tuning only after validating that those properties are supported in the exact
Oracle CDC runtime you are deploying.

### How are XMLTYPE columns captured?

See [XMLTYPE columns](#xmltype-columns). Without `lob.enabled = true`, streaming events after the snapshot have no XML document. With mining on, an unchanged XML column is the placeholder, and a full-row sink write replaces the stored XML with it.

### How are CLOB, NCLOB, and BLOB columns captured?

See [LOB columns](#lob-columns) and [Capture LOB columns](#capture-lob-columns). Without `lob.enabled`, a streaming LOB insert is often null. With mining on and `lob.reselect.enabled` left false, UPDATE and DELETE replace the sink's existing LOB with `__debezium_unavailable_value`.

### Which Oracle versions are supported?

Oracle CDC is supported on Oracle Database 11g, 12c, 19c, and 21c. For 12c and later multi-tenant configurations, use the CDB root connection with a common user.

## See Also

For a production-grade end-to-end guide covering full + incremental synchronization lifecycle,
2PC sink configuration, schema evolution, and troubleshooting, see
[CDC Production Cookbook](../cdc-production-cookbook.md).

## Changelog

<ChangeLog />
