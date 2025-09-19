import ChangeLog from '../changelog/connector-jdbc.md';

# JDBC

> JDBC source connector

## Description

Read external data source data through JDBC.

:::tip

Warn: for license compliance, you have to provide database driver yourself, copy to `$SEATUNNEL_HOME/lib/` directory in order to make them work.

e.g. If you use MySQL, should download and copy `mysql-connector-java-xxx.jar` to `$SEATUNNEL_HOME/lib/`. For Spark/Flink, you should also copy it to `$SPARK_HOME/jars/` or `$FLINK_HOME/lib/`.

:::

## Using Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) has been placed in directory `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)
- [x] [support multiple table read](../../concept/connector-v2-features.md)

## Options

| name                                       | type    | required | default value   | description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------|---------|----------|-----------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String  | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| driver                                     | String  | Yes      | -               | The jdbc class name used to connect to the remote data source, if you use MySQL the value is `com.mysql.cj.jdbc.Driver`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| username                                       | String  | No       | -               | userName                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| password                                   | String  | No       | -               | password                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| query                                      | String  | No       | -               | Query statement                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| compatible_mode                            | String  | No       | -               | The compatible mode of database, required when the database supports multiple compatible modes.<br/> For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'. <br/> when using starrocks, you need set it to `starrocks`                                                                                                                                                                                                                                                                                                                                                                                             |
| dialect                                    | String  | No       | -               | The appointed dialect, if it does not exist, is still obtained according to the url, and the priority is higher than the url. <br/> For example,when using starrocks, you need set it to `starrocks`                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| connection_check_timeout_sec               | Int     | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| partition_column                           | String  | No       | -               | The column name for split data.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| partition_upper_bound                      | Long    | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_lower_bound                      | Long    | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| partition_num                              | Int     | No       | job parallelism | Not recommended for use, The correct approach is to control the number of split through `split.size`<br/> **Note:** This parameter takes effect only when using the `query` parameter. It does not take effect when using the `table_path` parameter.                                                                                                                                                                                                                                                                                                                                                                                              |
| decimal_type_narrowing                     | Boolean | No       | true            | Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now. Please refer to `decimal_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| int_type_narrowing                         | Boolean | No       | true            | Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now. Please refer to `int_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| handle_blob_as_string                      | Boolean | No       | false           | If true, BLOB type will be converted to STRING type. **Only supported for Oracle database**. This is useful for handling large BLOB fields in Oracle that exceed the default size limit. When transmitting Oracle's BLOB fields to systems like Doris, setting this to true can make the data transfer more efficient.                                                                                                                                                                                                                                                                                                                             |
| use_select_count                           | Boolean | No       | false           | Use select count for table count rather then other methods in dynamic chunk split stage. This is currently only available for jdbc-oracle.In this scenario, select count directly is used when it is faster to update statistics using sql from analysis table                                                                                                                                                                                                                                                                                                                                                                                     |
| skip_analyze                               | Boolean | No       | false           | Skip the analysis of table count in dynamic chunk split stage. This is currently only available for jdbc-oracle.In this scenario, you schedule analysis table sql to update related table statistics periodically or your table data does not change frequently                                                                                                                                                                                                                                                                                                                                                                                    |
| use_regex                                  | Boolean | No       | false           | Control regular expression matching for table_path. When set to `true`, the table_path will be treated as a regular expression pattern. When set to `false` or not specified, the table_path will be treated as an exact path (no regex matching). |
| fetch_size                                 | Int     | No       | 0               | For queries that return a large number of objects, you can configure the row fetch size used in the query to improve performance by reducing the number database hits required to satisfy the selection criteria. Zero means use jdbc default value.                                                                                                                                                                                                                                                                                                                                                                                               |
| properties                                 | Map     | No       | -               | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in MySQL, properties take precedence over the URL.                                                                                                                                                                                                                                                                                                                                                                                                     |
| table_path                                 | String  | No       | -               | The path to the full path of table, you can use this configuration instead of `query`. <br/>examples: <br/>`- mysql: "testdb.table1" `<br/>`- oracle: "test_schema.table1" `<br/>`- sqlserver: "testdb.test_schema.table1"` <br/>`- postgresql: "testdb.test_schema.table1"`  <br/>`- iris: "test_schema.table1"`                                                                                                                                                                                                                                                                                                                                  |
| table_list                                 | Array   | No       | -               | The list of tables to be read, you can use this configuration instead of `table_path`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| where_condition                            | String  | No       | -               | Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.size                                 | Int     | No       | 8096            | How many rows in one split, captured tables are split into multiple splits when read of table. **Note**: This parameter takes effect only when using the `table_path` parameter. It does not take effect when using the `query` parameter.                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.even-distribution.factor.lower-bound | Double  | No       | 0.05            | Not recommended for use.<br/> The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| split.even-distribution.factor.upper-bound | Double  | No       | 100             | Not recommended for use.<br/> The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| split.sample-sharding.threshold            | Int     | No       | 1000            | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                                                 |
| split.inverse-sampling.rate                | Int     | No       | 1000            | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                                                            |
| common-options                             |         | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| split.string_split_mode                    | String  | No       | sample          | Supports different string splitting algorithms. By default, `sample` is used to determine the split by sampling the string value. You can switch to `charset_based` to enable charset-based string splitting algorithm. When set to `charset_based`, the algorithm assumes characters of partition_column are within ASCII range 32-126, which covers most character-based splitting scenarios.                                                                                                                                                                                                                                                    |
| split.string_split_mode_collate            | String  | No       | -               | Specifies the collation to use when string_split_mode is set to `charset_based` and the table has a special collation. If not specified, the database's default collation will be used.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

### Table Matching

The JDBC Source connector supports two ways to specify tables:

1. **Exact Table Path**: Use `table_path` to specify a single table with its full path.
   ```hocon
   table_path = "testdb.table1"
   ```

2. **Regular Expression**: Use `table_path` with a regex pattern to match multiple tables.
   ```hocon
   table_path = "testdb.table\\d+"  # Matches table1, table2, table3, etc.
   use_regex = true
   ```

#### Regular Expression Support for Table Names

The JDBC connector supports using regular expressions to match multiple tables. This feature allows you to process multiple tables with a single source configuration.

#### Configuration

To use regular expression matching for table paths:

1. Set `use_regex = true` to enable regex matching
2. If `use_regex` is not set or set to `false`, the connector will treat the table_path as an exact path (no regex matching)

#### Regular Expression Syntax Notes

- **Path Separator**: The dot (`.`) is treated as a separator between database, schema, and table names.
- **Escaped Dots**: If you need to use a dot (`.`) as a wildcard character in your regular expression to match any character, you must escape it with a backslash (`\.`).
- **Path Format**: For paths like `database.table` or `database.schema.table`, the last unescaped dot separates the table pattern from the database/schema pattern.
- **Pattern Examples**:
  - `test.table\\d+` - Matches tables like `table1`, `table2`, etc. in the `test` database
  - `test.*` - Matches all tables in the `test` database (for whole database synchronization)
  - `postgres.public.test_db_\.*` - Matches all tables that start with `test_db_` in the `public` schema of the `postgres` database

#### Example

```hocon
source {
  Jdbc {
    url = "jdbc:mysql://localhost:3306/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "password"
    
    table_list = [
      {
        # Regex matching - match any table in test database
        table_path = "test.*"
        use_regex = true
      },
      {
        # Regex matching - match tables with "user" followed by digits
        table_path = "test.user\\d+"
        use_regex = true
      },
      {
        # Exact matching - simple table name
        table_path = "test.config"
        # use_regex not specified, defaults to false
      },
    ]
  }
}
```

#### Multi-table Synchronization

When using either regular expressions, the connector will read data from all matching tables. Each table will be processed independently, and the data will be combined in the output.

Example configuration for multi-table synchronization:
```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    # Using regular expression with explicit configuration
    table_list = [
      {
        table_path = "testdb.table\\d+"
        use_regex = true
      }
    ]
}
```

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

### int_type_narrowing

Int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now.

eg:

int_type_narrowing = true

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | Boolean   |

int_type_narrowing = false

| MySQL      | SeaTunnel |
|------------|-----------|
| TINYINT(1) | TINYINT   |

### dialect [string]

The appointed dialect, if it does not exist, is still obtained according to the url, and the priority is higher than the url. For example,when using starrocks, you need set it to `starrocks`. Similarly, when using mysql, you need to set its value to `mysql`.

If one dialect not supported by SeaTunnel, it will use the default dialect `GenericDialect`. Just make sure the driver you provided support the database you want to connect.

#### dialect list

|           | Dialect Name |          |
|-----------|--------------|----------|
| Greenplum | DB2          | Dameng   |
| Gbase8a   | HIVE         | KingBase |
| MySQL     | StarRocks    | Oracle   |
| Phoenix   | Postgres     | Redshift |
| SapHana   | Snowflake    | Sqlite   |
| SqlServer | Tablestore   | Teradata |
| Vertica   | OceanBase    | XUGU     |
| IRIS      | Inceptor     | Highgo   |


## Parallel Reader

The JDBC Source connector supports parallel reading of data from tables. SeaTunnel will use certain rules to split the data in the table, which will be handed over to readers for reading. The number of readers is determined by the `parallelism` option.

**Split Key Rules:**

1. If `partition_column` is not null, It will be used to calculate split. The column must in **Supported split data type**.
2. If `partition_column` is null, seatunnel will read the schema from table and get the Primary Key and Unique Index. If there are more than one column in Primary Key and Unique Index, The first column which in the **supported split data type** will be used to split data. For example, the table have Primary Key(nn guid, name varchar), because `guid` id not in **supported split data type**, so the column `name` will be used to split data.

**Supported split data type:**
* String
* Number(int, bigint, decimal, ...)
* Date

## tips

> If the table can not be split(for example, table have no Primary Key or Unique Index, and `partition_column` is not set), it will run in single concurrency.
>
> Use `table_path` to replace `query` for single table reading. If you need to read multiple tables, use `table_list`.

## appendix

there are some reference value for params above.

| datasource        | driver                                              | url                                                                    | maven                                                                                                                         |
|-------------------|-----------------------------------------------------|------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|
| mysql             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| postgresql        | org.postgresql.Driver                               | jdbc:postgresql://localhost:5432/postgres                              | https://mvnrepository.com/artifact/org.postgresql/postgresql                                                                  |
| dm                | dm.jdbc.driver.DmDriver                             | jdbc:dm://localhost:5236                                               | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18                                                                  |
| phoenix           | org.apache.phoenix.queryserver.client.Driver        | jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF     | https://mvnrepository.com/artifact/com.aliyun.phoenix/ali-phoenix-shaded-thin-client                                          |
| sqlserver         | com.microsoft.sqlserver.jdbc.SQLServerDriver        | jdbc:sqlserver://localhost:1433                                        | https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc                                                         |
| oracle            | oracle.jdbc.OracleDriver                            | jdbc:oracle:thin:@localhost:1521/xepdb1                                | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8                                                            |
| sqlite            | org.sqlite.JDBC                                     | jdbc:sqlite:test.db                                                    | https://mvnrepository.com/artifact/org.xerial/sqlite-jdbc                                                                     |
| gbase8a           | com.gbase.jdbc.Driver                               | jdbc:gbase://e2e_gbase8aDb:5258/test                                   | https://cdn.gbase.cn/products/30/p5CiVwXBKQYIUGN8ecHvk/gbase-connector-java-9.5.0.7-build1-bin.jar                            |
| starrocks         | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| db2               | com.ibm.db2.jcc.DB2Driver                           | jdbc:db2://localhost:50000/testdb                                      | https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc/db2jcc4                                                             |
| tablestore        | com.alicloud.openservices.tablestore.jdbc.OTSDriver | "jdbc:ots:http s://myinstance.cn-hangzhou.ots.aliyuncs.com/myinstance" | https://mvnrepository.com/artifact/com.aliyun.openservices/tablestore-jdbc                                                    |
| saphana           | com.sap.db.jdbc.Driver                              | jdbc:sap://localhost:39015                                             | https://mvnrepository.com/artifact/com.sap.cloud.db.jdbc/ngdbc                                                                |
| doris             | com.mysql.cj.jdbc.Driver                            | jdbc:mysql://localhost:3306/test                                       | https://mvnrepository.com/artifact/mysql/mysql-connector-java                                                                 |
| teradata          | com.teradata.jdbc.TeraDriver                        | jdbc:teradata://localhost/DBS_PORT=1025,DATABASE=test                  | https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc                                                                 |
| Snowflake         | net.snowflake.client.jdbc.SnowflakeDriver           | jdbc&#58;snowflake://<account_name>.snowflakecomputing.com             | https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc                                                               |
| Redshift          | com.amazon.redshift.jdbc42.Driver                   | jdbc:redshift://localhost:5439/testdb?defaultRowFetchSize=1000         | https://mvnrepository.com/artifact/com.amazon.redshift/redshift-jdbc42                                                        |
| Vertica           | com.vertica.jdbc.Driver                             | jdbc:vertica://localhost:5433                                          | https://repo1.maven.org/maven2/com/vertica/jdbc/vertica-jdbc/12.0.3-0/vertica-jdbc-12.0.3-0.jar                               |
| Kingbase          | com.kingbase8.Driver                                | jdbc:kingbase8://localhost:54321/db_test                               | https://repo1.maven.org/maven2/cn/com/kingbase/kingbase8/8.6.0/kingbase8-8.6.0.jar                                            |
| OceanBase         | com.oceanbase.jdbc.Driver                           | jdbc:oceanbase://localhost:2881                                        | https://repo1.maven.org/maven2/com/oceanbase/oceanbase-client/2.4.12/oceanbase-client-2.4.12.jar                              |
| Hive              | org.apache.hive.jdbc.HiveDriver                     | jdbc:hive2://localhost:10000                                           | https://repo1.maven.org/maven2/org/apache/hive/hive-jdbc/3.1.3/hive-jdbc-3.1.3-standalone.jar                                 |
| xugu              | com.xugu.cloudjdbc.Driver                           | jdbc:xugu://localhost:5138                                             | https://repo1.maven.org/maven2/com/xugudb/xugu-jdbc/12.2.0/xugu-jdbc-12.2.0.jar                                               |
| InterSystems IRIS | com.intersystems.jdbc.IRISDriver                    | jdbc:IRIS://localhost:1972/%SYS                                        | https://raw.githubusercontent.com/intersystems-community/iris-driver-distribution/main/JDBC/JDK18/intersystems-jdbc-3.8.4.jar |
| opengauss         | org.opengauss.Driver                                | jdbc:opengauss://localhost:5432/postgres                               | https://repo1.maven.org/maven2/org/opengauss/opengauss-jdbc/5.1.0-og/opengauss-jdbc-5.1.0-og.jar                              |
| Highgo            | com.highgo.jdbc.Driver                              | jdbc:highgo://localhost:5866/highgo                                    | https://repo1.maven.org/maven2/com/highgo/HgdbJdbc/6.2.3/HgdbJdbc-6.2.3.jar                                                   |
| Presto            | com.facebook.presto.jdbc.PrestoDriver               | jdbc:presto://localhost:8080/presto                                    | https://repo1.maven.org/maven2/com/facebook/presto/presto-jdbc/0.279/presto-jdbc-0.279.jar                                    |
| Trino             | io.trino.jdbc.TrinoDriver                           | jdbc:trino://localhost:8080/trino                                      | https://repo1.maven.org/maven2/io/trino/trino-jdbc/460/trino-jdbc-460.jar                                                     |

## Example

### simple

#### Case 1

```
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    query = "select * from type_bin"
}
```

#### Case 2 Use the select count(*) instead of analysis table for count table rows in dynamic chunk split stage

```
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    use_select_count = true 
    query = "select * from type_bin"
}
```

#### Case 3 Use the select NUM_ROWS from all_tables for the table rows but skip the analyze table.

```
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    skip_analyze = true 
    query = "select * from type_bin"
}
```

#### Case 4 Oracle Source with BLOB as string to Doris Sink

This example demonstrates how to handle Oracle's BLOB data as strings when transferring to Doris. This is useful for large BLOB fields.

```
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@oracle_host:1521/SERVICE_NAME"
    user = "username"
    password = "password"
    query = "SELECT ID, NAME, CONTENT_BLOB FROM MY_TABLE"
    handle_blob_as_string = true  # Enable BLOB to String conversion for Oracle
  }
}
```

### parallel by partition_column

```
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin"
        partition_column = "id"
        partition_num = 10 # Replace split.size with partition_num
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

### Parallel Boundary

> It is more efficient to specify the data within the upper and lower bounds of the query. It is more efficient to read your data source according to the upper and lower boundaries you configured.

```
source {
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "select * from type_bin"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
        properties {
         useSSL=false
        }
    }
}
```

### parallel by Primary Key or Unique Index

> Configuring `table_path` will turn on auto split, you can configure `split.*` to adjust the split strategy

```
env {
  parallelism = 10
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        table_path = "testdb.table1"
        query = "select * from testdb.table1"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### multiple table read

***Configuring `table_list` will turn on auto split, you can configure `split.*` to adjust the split strategy***

```hocon
Jdbc {
    url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
    driver = "com.mysql.cj.jdbc.Driver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"

    table_list = [
        {
          # e.g. table_path = "testdb.table1"、table_path = "test_schema.table1"、table_path = "testdb.test_schema.table1"
          table_path = "testdb.table1"
        },
        {
          table_path = "testdb.table2"
          # Use query filter rows & columns
          query = "select id, name from testdb.table2 where id > 100"
        },
        {
          # Using regex to match multiple tables
          table_path = "testdb.user_table\\d+"
          use_regex = true
        }
    ]
    #where_condition= "where id > 100"
    #split.size = 10000
    #split.even-distribution.factor.upper-bound = 100
    #split.even-distribution.factor.lower-bound = 0.05
    #split.sample-sharding.threshold = 1000
    #split.inverse-sampling.rate = 1000
}
```

## Changelog

<ChangeLog />