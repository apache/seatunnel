import ChangeLog from '../changelog/connector-cdc-oracle.md';

# Oracle CDC

> Oracle CDC 数据源连接器

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink <br/>

## 关键特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)

## 描述

Oracle CDC 连接器允许从 Oracle 数据库读取快照数据和增量数据。本文档描述了如何设置 Oracle CDC 连接器以针对 Oracle 数据库运行 SQL 查询。

## 注意

Debezium Oracle 连接器不依赖于连续挖掘（continuous mining）选项。该连接器负责检测日志切换并自动调整正在挖掘的日志，这正是连续挖掘选项自动为您完成的工作。
因此，您不能在 debezium 中设置名为 `log.mining.continuous.mine` 的属性。

## 支持的数据源信息

| 数据源 |                    支持的版本                    |          驱动类          |                  Url                   |                               Maven                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | 不同的依赖版本有不同的驱动类。 | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## 数据库依赖

### 安装 Jdbc 驱动

#### 适用于 Spark/Flink 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在 `${SEATUNNEL_HOME}/plugins/` 目录下。
> 2. 为了支持 i18n 字符集，请将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/plugins/` 目录。

#### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [jdbc 驱动 jar 包](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) 已放置在 `${SEATUNNEL_HOME}/lib/` 目录下。
> 2. 为了支持 i18n 字符集，请将 `orai18n.jar` 复制到 `$SEATUNNEL_HOME/lib/` 目录。

### 启用 Oracle Logminer

> 要在 Seatunnel 中使用 Logminer（Oracle 提供的内置工具）启用 Oracle CDC（变更数据捕获），请按照以下步骤操作：

#### 在非 CDB（容器数据库）模式下启用 Logminer。

1. 操作系统创建一个空的目录来存储 Oracle 归档日志和用户表空间。

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

3. 以管理员身份登录并创建一个名为 logminer_user 的账户，密码为 "oracle"，并授予其读取表和日志的权限。

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

##### 注意：Oracle 11g 不支持以下命令

```sql
GRANT LOGMINING TO logminer_user;
```

##### 仅授予需要采集的表的权限

```sql
GRANT SELECT ANY TABLE TO logminer_user;
GRANT ANALYZE ANY TO logminer_user;
```

#### 在 Oracle CDB (容器数据库) + PDB (可插拔数据库) 模式下启用 Logminer

1. 操作系统创建一个空的目录来存储 Oracle 归档日志和用户表空间。

```shell
mkdir -p /opt/oracle/oradata/recovery_area
mkdir -p /opt/oracle/oradata/ORCLCDB
mkdir -p /opt/oracle/oradata/ORCLCDB/ORCLPDB1
chown -R oracle /opt/oracle/***
```

2. 以管理员身份登录并启用日志记录

```sql
sqlplus /nolog
connect sys as sysdba; # 密码: oracle
alter system set db_recovery_file_dest_size = 10G;
alter system set db_recovery_file_dest = '/opt/oracle/oradata/recovery_area' scope=spfile;
shutdown immediate
startup mount
alter database archivelog;
alter database open;
archive log list;
```

3. 在 CDB 中执行

```sql
ALTER TABLE TEST.* ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE TEST.T2 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

4. 创建 debeziume 账户

> 在 CDB 中操作

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLCDB as sysdba
CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/logminer_tbs.dbf'
 SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
exit;
```

> 在 PDB 中操作

```sql
sqlplus sys/top_secret@//localhost:1521/ORCLPDB1 as sysdba
 CREATE TABLESPACE logminer_tbs DATAFILE '/opt/oracle/oradata/ORCLCDB/ORCLPDB1/logminer_tbs.dbf'
   SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
 exit;
```

5. 在 CDB 中操作

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

|                                   Oracle 数据类型                                   | SeaTunnel 数据类型 |
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

## 源端选项

|                      参数名称                 |   类型   | 是否必选   | 默认值 | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-------------------------------------------|----------|--------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                       | String   | 是      | -       | JDBC 连接的 URL。例如：`jdbc:oracle:thin:datasource01:1523:xe`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| username                                  | String   | 是      | -       | 连接数据库服务器时使用的数据库用户名。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| password                                  | String   | 是      | -       | 连接数据库服务器时使用的数据库密码。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| database-names                            | List     | 否      | -       | 要监控的数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| schema-names                              | List     | 否      | -       | 要监控的数据库 Schema 名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| table-names                               | List     | 是      | -       | 要监控的数据库表名。表名需要包含数据库名，例如：`database_name.table_name`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| table-names-config                        | List     | 否      | -       | 表配置列表。例如：`[{"table": "db1.schema1.table1","primaryKeys": ["key1"],"snapshotSplitColumn": "key2"}]`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| startup.mode                              | Enum     | 否      | INITIAL | Oracle CDC 使用者的可选启动模式，有效枚举值为 `initial`、`earliest`、`latest`、`timestamp` 和 `specific`。<br/> `initial`：启动时同步历史数据，然后同步增量数据。<br/> `earliest`：从尽可能早的偏移量启动。<br/> `latest`：从最新的偏移量启动。<br/> `specific`：从用户提供的特定偏移量启动。                                                                                                                                                                                                          |
| startup.timestamp                         | Long     | 否      | -       | 从指定的时间戳（自 Unix 纪元以来的毫秒数）启动。当 `startup.mode = timestamp` 时，该时间戳会按 `server-time-zone` 转换。**注意，当 `startup.mode` 选项使用 `timestamp` 时，此选项是必需的。**                                                                                                                                                                                                                                                                                                                                                                                                      |
| startup.specific-offset.file              | String   | 否      | -       | 从指定的 binlog 文件名启动。**注意，当 `startup.mode` 选项使用 `specific` 时，此选项是必需的。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| startup.specific-offset.pos               | Long     | 否      | -       | 从指定的 binlog 文件位置启动。**注意，当 `startup.mode` 选项使用 `specific` 时，此选项是必需的。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| stop.mode                                 | Enum     | 否      | NEVER   | Oracle CDC 使用者的可选停止模式，有效枚举值为 `never`、`latest` 或 `specific`。<br/> `never`：实时任务不停止源。<br/> `latest`：从最新的偏移量停止。<br/> `specific`：从用户提供的特定偏移量停止。                                                                                                                                                                                                                                                                                                                                                                                                                      |
| stop.specific-offset.file                 | String   | 否      | -       | 从指定的 binlog 文件名停止。**注意，当 `stop.mode` 选项使用 `specific` 时，此选项是必需的。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| stop.specific-offset.pos                  | Long     | 否      | -       | 从指定的 binlog 文件位置停止。**注意，当 `stop.mode` 选项使用 `specific` 时，此选项是必需的。**                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| snapshot.split.size                       | Integer  | 否      | 8096    | 表快照的拆分大小（行数），在读取表快照时，捕获的表将被拆分为多个拆分块。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| snapshot.fetch.size                       | Integer  | 否      | 1024    | 读取表快照时每次轮询的最大获取大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| server-time-zone                          | String   | 否      | UTC     | 数据库服务器中的会话时区。如果未设置，则使用 ZoneId.systemDefault() 来确定服务器时区。该参数也用于将 `startup.timestamp` 转换为 SCN。若数据库时区与 JVM 时区不同，建议显式配置。                                                                                                                                                                                                                                                                                                                                                                                                                  |
| connect.timeout.ms                        | Duration | 否      | 30000   | 连接器在尝试连接数据库服务器后超时的最大等待时间。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| connect.max-retries                       | Integer  | 否      | 3       | 连接器尝试建立数据库服务器连接的最大重试次数。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| connection.pool.size                      | Integer  | 否      | 20      | JDBC 连接池大小。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| chunk-key.even-distribution.factor.upper-bound | Double   | 否      | 100     | 分块键分布因子的上限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子小于或等于此上限（即 (MAX(id) - MIN(id) + 1) / 行数），则表分块将针对均匀分布进行优化。否则，如果分布因子较大，则表将被视为分布不均，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 100.0。 |
| chunk-key.even-distribution.factor.lower-bound | Double   | 否      | 0.05    | 分块键分布因子的下限。此因子用于确定表数据是否均匀分布。如果计算出的分布因子大于或等于此下限（即 (MAX(id) - MIN(id) + 1) / 行数），则表分块将针对均匀分布进行优化。否则，如果分布因子较小，则表将被视为分布不均，如果估计的分片数超过 `sample-sharding.threshold` 指定的值，则将使用基于采样的分片策略。默认值为 0.05。  |
| sample-sharding.threshold                 | Integer  | 否      | 1000    | 此配置指定触发采样分片策略的预估分片数阈值。当分布因子超出 `chunk-key.even-distribution.factor.upper-bound` 和 `chunk-key.even-distribution.factor.lower-bound` 指定的范围，并且预估的分片数（计算为近似行数 / 分块大小）超过此阈值时，将使用采样分片策略。这有助于更有效地处理大型数据集。默认值为 1000 个分片。                                                                                   |
| inverse-sampling.rate                     | Integer  | 否      | 1000    | 采样分片策略中使用的采样率的倒数。例如，如果此值设置为 1000，则意味着在采样过程中应用 1/1000 的采样率。此选项提供了控制采样粒度的灵活性，从而影响最终的分片数量。在处理首选较低采样率的极大型数据集时，它特别有用。默认值为 1000。                                                                                                                                                              |
| split.allow-sampling                    | Boolean  | 否      | true    | 是否启用基于采样的分片策略。当设置为 false 时，无论预估分片数是否超过阈值，系统都将回退到非均匀分片方式（迭代查询方式）。                                                                                                                                                                                    |
| exactly_once                              | Boolean  | 否      | false   | 启用精确一次语义。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| use_select_count                          | Boolean  | 否      | false   | 使用 `select count` 统计表行数，而不是在全量阶段使用其他方法。在这种情况下，当通过分析表使用 SQL 更新统计信息更快时，直接使用 `select count`。                                                                                                                                                                                                                                                                                                                                                                                                                        |
| skip_analyze                              | Boolean  | 否      | false   | 在全量阶段跳过表行数的分析。在这种情况下，您需要定期调度分析表 SQL 以更新相关表统计信息，或者您的表数据更改不频繁。                                                                                                                                                                                                                                                                                                                                                                                                                       |
| format                                    | Enum     | 否      | DEFAULT | Oracle CDC 的可选输出格式，有效枚举值为 `DEFAULT`、`COMPATIBLE_DEBEZIUM_JSON`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| schema-changes.enabled                    | Boolean  | 否      | false   | Schema 演进默认禁用。目前我们仅支持 `add column`、`drop column`、`rename column` 和 `modify column`。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| debezium                                  | Config   | 否      | -       | 透传 [Debezium 属性](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#connector-properties) 给 Debezium Embedded Engine，该引擎用于捕获 Oracle 服务器的数据更改。                                                                                                                                                                                                                                                                                                                                                      |
| common-options                            |          | 否      | -       | 源端插件常用参数，详情请参阅 [源端常用选项](../common-options/source-common-options.md)。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| decimal_type_narrowing                    | Boolean | 否      | true            | 数值类型收缩，如果为 true，则在不损失精度的情况下，将 decimal 类型收缩为 int 或 long 类型。目前仅支持 Oracle。请参阅下文的 `decimal_type_narrowing`。                                                                                                                                                                                                                                                                                                                                                                                                              |


### decimal_type_narrowing

数值类型收缩，如果为 true，则在不损失精度的情况下，将 decimal 类型收缩为 int 或 long 类型。目前仅支持 Oracle。

例如：

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

### 简单示例

> 支持多表读取

```conf
source {
  # 这是一个示例源端插件，**仅用于测试和演示源端插件功能**
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

> 在全量阶段使用 select count(*) 代替 analysis table 来统计表行数
```conf
source {
# 这是一个示例源端插件，**仅用于测试和演示源端插件功能**
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

> 使用 select NUM_ROWS from all_tables 获取表行数，但跳过 analyze table 操作。

```conf
source {
# 这是一个示例源端插件，**仅用于测试和演示源端插件功能**
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

### 支持以兼容 debezium 的格式发送到 kafka

> 必须与 kafka 连接器 sink 配合使用，详情请参阅 [兼容 debezium 格式](../formats/cdc-compatible-debezium-json.md)

## 更新日志

<ChangeLog />
