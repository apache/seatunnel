import ChangeLog from '../changelog/connector-cdc-oracle.md';

# Oracle CDC

> Oracle CDC 源连接器

## 支持这些引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 关键特性

- [ ] [批](../../concept/connector-v2-features.md)
- [x] [流](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户自定义split](../../concept/connector-v2-features.md)

## 描述

Oracle CDC 连接器允许从 Oracle 数据库读取快照数据和增量数据。本文档描述了如何设置 Oracle CDC 连接器以针对 Oracle 数据库运行 SQL 查询。

## 注意

Debezium Oracle 连接器不依赖于连续挖掘选项。连接器负责检测日志切换并自动调整要挖掘的日志，这是连续挖掘选项为您自动执行的操作。
因此，您不能在 debezium 中设置名为 `log.mining.continuous.mine` 的此属性。

## 支持的数据源信息

| 数据源 | 支持的版本 | 驱动程序 | URL | Maven |
|--------|-----------|---------|-----|-------|
| Oracle | 不同的依赖版本有不同的驱动程序类。 | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## 数据库依赖

### 安装 JDBC 驱动程序

#### 对于 Spark/Flink 引擎

> 1. 您需要确保 [JDBC 驱动程序 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。
> 2. 为了支持 i18n 字符集，将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/plugins/` 目录。

#### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [JDBC 驱动程序 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。
> 2. 为了支持 i18n 字符集，将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/lib/` 目录。

### 启用 Oracle Logminer

> 要在 Seatunnel 中启用 Oracle CDC（变更数据捕获）使用 Logminer（这是 Oracle 提供的内置工具），请按照以下步骤操作：

#### 在没有 CDB（容器数据库）模式的情况下启用 Logminer。

1. 操作系统创建一个空文件目录来存储 Oracle 归档日志和用户表空间。

```shell
mkdir -p /opt/oracle/oradata/recovery_area
mkdir -p /opt/oracle/oradata/ORCLCDB
chown -R oracle /opt/oracle/***
```

2. 以管理员身份登录并启用 Oracle 归档日志。

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

3. 以管理员身份登录并创建一个名为 logminer_user 的帐户，密码为 "oracle"，并授予其读取表和日志的权限。

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

##### 不支持 Oracle 11g。

```sql
GRANT LOGMINING TO logminer_user;
```

##### 仅向需要采集的表授予权限。
```sql
GRANT SELECT ANY TABLE TO logminer_user;
GRANT ANALYZE ANY TO logminer_user;
```

#### 在 Oracle 的 CDB（容器数据库）+ PDB（可插拔数据库）模式下启用 LogMiner

1. 操作系统创建一个空的文件目录，用于存储 Oracle 归档日志和用户表空间。

```shell
mkdir -p /opt/oracle/oradata/recovery_area
mkdir -p /opt/oracle/oradata/ORCLCDB
mkdir -p /opt/oracle/oradata/ORCLCDB/ORCLPDB1
chown -R oracle /opt/oracle/***
```

2. 以管理员身份登录并启用日志功能。
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

3. 在 CDB 中执行。

```sql
ALTER TABLE TEST.* ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE TEST.T2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

4. 创建 debezium 账户。

> 在 CDB 中操作。

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLCDB as sysdba
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf'
 SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
```

> 在 PDB 中操作。

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLPDB1 as sysdba
 CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf'
   SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
 exit;
```

5. 在 CDB 中操作。

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

## 数据类型映射

|                                   Oracle 数据类型                                   | SeaTunnel数据类型 |
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

## 源选项
|                      名称                 |   类型   | 必需 | 默认 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|----------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | Yes      | -       | JDBC 连接的 URL。示例：`jdbc:oracle:thin:@datasource01:1523:xe`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| username                                  | String   | 是      | -       | 连接到数据库服务器时使用的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| password                                  | String   | 是      | -       | 连接到数据库服务器时使用的密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | 否       | -       | 需要监控的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| schema-names                              | List     | 否       | -       | 要监控的数据库 Schema 名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| table-names                               | List     | 是      | -       | 需要监控的数据库表名称。表名称需要包含数据库名称，例如：`database_name.table_name`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                        | List     | 否   | -        | 表配置列表。例如： [{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]。 snapshotSplitColumn 选项必须配置为唯一键(主键或唯一索引)。 如果指定了非唯一列，该配置将被忽略，SeaTunnel 会在内部自动选择合适的拆分列。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                              | Enum     | 否       | INITIAL | Oracle CDC 消费者的可选启动模式，支持的枚举值包括 `initial`、`earliest`、`latest` 和 `specific`。<br/> `initial`：启动时同步历史数据，随后同步增量数据。<br/> `earliest`：从可用的最早偏移量开始启动。<br/> `latest`：从最新偏移量开始启动。<br/> `specific`：从用户指定的特定偏移量开始启动。                                                                                                                                                                                                                  |
| startup.specific-offset.file              | String   | 否       | -       | 从指定的 binlog 文件名开始。**注意：当 `startup.mode` 设置为 `specific` 时，此选项为必填。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.pos               | Long     | 否       | -       | 从指定的 binlog 文件位置开始。**注意：当 `startup.mode` 设置为 `specific` 时，此选项为必填。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| stop.mode                                 | Enum     | 否       | NEVER   | Oracle CDC 消费者的可选停止模式，支持的枚举值包括 `never`、`latest` 和 `specific`。<br/> `never`：实时任务不会停止源端读取。<br/> `latest`：在最新偏移量处停止。<br/> `specific`：在用户指定的特定偏移量处停止。                                                                                                                                                                                                                                                                                                                                                        |
| stop.specific-offset.file                 | String   | 否       | -       | 在指定的 binlog 文件名处停止。**注意：当 `stop.mode` 设置为 `specific` 时，此选项为必填。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.specific-offset.pos                  | Long     | No       | -       | 在指定的 binlog 文件位置处停止。**注意：当 `stop.mode` 设置为 `specific` 时，此选项为必填。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| snapshot.split.size                       | Integer  | 否       | 8096    | 表快照的拆分大小（行数），捕获的表在读取表快照时被拆分成多个拆分                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | 否       | 1024    | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-time-zone                          | String   | No       | UTC     | 数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| connect.timeout.ms                        | Duration | No       | 30000   | 连接器在尝试连接到数据库服务器后应等待的最大时间，以防超时。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | 否       | 3       | 连接器重试建立数据库服务器连接的最大次数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | 否       | 20      | JDBC 连接池大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否       | 100     | 块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更大，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 100.0。 |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否       | 0.05    | 块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则将优化表块以实现均匀分布。否则，如果分布因子更小，则将认为该表分布不均匀，并且如果估计的分片数量超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 0.05。  |
| sample-sharding.threshold                 | Integer  | 否       | 1000    | 此配置指定触发采样分片策略的估计分片数量阈值。当分布因子超出由 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，且估计的分片数量（计算为近似行数 / 块大小）超过此阈值时，将使用采样分片策略。这可以帮助更有效地处理大数据集。默认值为 1000 个分片。                                                                                   |
| inverse-sampling.rate                     | Integer  | 否       | 1000    | 在采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理非常大数据集时，较低的采样率尤为有用。默认值为 1000。                                                                                                                                                              |
| exactly_once                              | Boolean  | 否       | false   | 启用精确一次语义。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| use_select_count                          | Boolean  | 否       | false   | 在全量阶段使用 `select count` 进行表行数统计，而不是使用其他方法。在此场景下，当直接执行 `select count` 比通过分析表（analysis table）使用 SQL 更新统计信息更快时，将采用该方式。                                                                                                                                                                                                                                                                                                                                                                                                                        |
| skip_analyze                              | Boolean  | 否       | false   | 在全量阶段跳过表行数统计的分析操作。在此场景下，你可以定期调度分析表的 SQL 来更新相关表的统计信息，或者表数据变更不频繁。                                                                                                                                                                                                                                                                                                                                                                                                                       |
| format                                    | Enum     | 否       | DEFAULT | Oracle CDC 的可选输出格式, 有效的枚举值为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| schema-changes.enabled                    | Boolean  | 否       | false   | 模式演进默认是禁用的. 当前我们只支持 `add column`、`drop column`、`rename column` 和 `modify column`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| debezium                                  | Config   | 否       | -       | 传递 [Debezium 的属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#connector-properties) 给Debezium嵌入式引擎, 该引擎用于捕获 Oracle 服务的数据变。                                                                                                                                                                                                                                                                                                                                                      |
| common-options                            |          | 否       | -       | Source插件通用参数, 详见 [Source Common Options](../source-common-options.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| decimal_type_narrowing                    | Boolean | 否       | true            | Decimal 类型收窄：如果设置为 true，在不损失精度的情况下，decimal 类型将被收窄为 int 或 long 类型。目前仅支持 Oracle。请参考下方的 `decimal_type_narrowing` 配置。                                                                                                                                                                                                                                                                                                                                                                                                                                                  |


### decimal 类型收窄

Decimal 类型收窄：如果设置为 true，在不损失精度的情况下，decimal 类型将被收窄为 int 或 long 类型。目前仅支持 Oracle。

例如:

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

## 任务示例

### 简单的示例

> 支持多表读取

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
    url = "jdbc:oracle:thin:@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

> 在全量阶段使用 select count(*) 代替分析表来统计表行数
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
    url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

> 使用 all_tables 中的 NUM_ROWS 作为表行数统计结果，并跳过对表的 analyze 操作

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
    url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
    source.reader.close.timeout = 120000
  }
}
```

### 支持表的自定义主键

```conf
source {
  Oracle-CDC {
    plugin_output = "customers"
    url = "jdbc:oracle:thin:system/oracle@oracle-host:1521:xe"
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

### 支持向Kafka发送与Debezium兼容的格式

> 一定是使用kafka作为sink, 详见 [compatible debezium format](../formats/cdc-compatible-debezium-json.md)

## 变更日志

<ChangeLog />