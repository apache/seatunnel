# Oracle

> JDBC Oracle Source Connector

## Description

Read external data source data through JDBC.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Supported DataSource Info

| Datasource |                    Supported Versions                    |          Driver          |                  Url                   |                               Maven                                |
|------------|----------------------------------------------------------|--------------------------|----------------------------------------|--------------------------------------------------------------------|
| Oracle     | Different dependency version has different driver class. | oracle.jdbc.OracleDriver | jdbc:oracle:thin:@datasource01:1523:xe | https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8 |

## Database Dependency

### For Spark/Flink Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/plugins/`.
> 2. To support the i18n character set, copy the `orai18n.jar` to the `$SEATUNNEL_HOME/plugins/` directory.

### For SeaTunnel Zeta Engine

> 1. You need to ensure that the [jdbc driver jar package](https://mvnrepository.com/artifact/com.oracle.database.jdbc/ojdbc8) has been placed in directory `${SEATUNNEL_HOME}/lib/`.
> 2. To support the i18n character set, copy the `orai18n.jar` to the `$SEATUNNEL_HOME/lib/` directory.

## Data Type Mapping

|                                             Oracle Data Type                                             | SeaTunnel Data Type |
|----------------------------------------------------------------------------------------------------------|---------------------|
| INTEGER                                                                                                  | DECIMAL(38,0)       |
| FLOAT                                                                                                    | DECIMAL(38, 18)     |
| NUMBER(precision <= 9, scale == 0)                                                                       | INT                 |
| NUMBER(9 < precision <= 18, scale == 0)                                                                  | BIGINT              |
| NUMBER(18 < precision, scale == 0)                                                                       | DECIMAL(38, 0)      |
| NUMBER(scale != 0)                                                                                       | DECIMAL(38, 18)     |
| BINARY_DOUBLE                                                                                            | DOUBLE              |
| BINARY_FLOAT<br/>REAL                                                                                    | FLOAT               |
| CHAR<br/>NCHAR<br/>VARCHAR<br/>NVARCHAR2<br/>VARCHAR2<br/>LONG<br/>ROWID<br/>NCLOB<br/>CLOB<br/>XML<br/> | STRING              |
| DATE                                                                                                     | TIMESTAMP           |
| TIMESTAMP<br/>TIMESTAMP WITH LOCAL TIME ZONE                                                             | TIMESTAMP           |
| BLOB<br/>RAW<br/>LONG RAW<br/>BFILE                                                                      | BYTES               |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:oracle:thin:@datasource01:1523:xe                                                                                                                                                                           |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use Oracle the value is `oracle.jdbc.OracleDriver`.                                                                                                                                     |
| user                         | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                     |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                      |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type,Only support numeric type primary key, and only can config one column.                                                                                                                     |
| partition_lower_bound        | BigDecimal | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                  |
| partition_upper_bound        | BigDecimal | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                  |
| partition_num                | Int        | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                                    |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| properties                   | Map        | No       | -               | Additional connection configuration parameters,when properties and URL have the same parameters, the priority is determined by the <br/>specific implementation of the driver. For example, in Oracle, properties take precedence over the URL.                    |
| table_path                                 | String     | No       | -               | The path to the full path of table, you can use this configuration instead of `query`. <br/>examples: <br/>mysql: "testdb.table1" <br/>oracle: "test_schema.table1" <br/>sqlserver: "testdb.test_schema.table1" <br/>postgresql: "testdb.test_schema.table1"                                                                                                                                                                                                                                                                                                                                                         |
| table_list                                 | Array      | No       | -               | The list of tables to be read, you can use this configuration instead of `table_path` example: ```[{ table_path = "testdb.table1"}, {table_path = "testdb.table2", query = "select * id, name from testdb.table2"}]```                                                                                                                                                                                                                                                                                                                                                                                               |
| where_condition                            | String     | No       | -               | Common row filter conditions for all tables/queries, must start with `where`. for example `where id > 100`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| split.size                                 | Int        | No       | 8096            | The split size (number of rows) of table, captured tables are split into multiple splits when read of table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| split.even-distribution.factor.lower-bound | Double     | No       | 0.05            | The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.  |
| split.even-distribution.factor.upper-bound | Double     | No       | 100             | The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0. |
| split.sample-sharding.threshold            | Int        | No       | 10000           | This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.                                                                                   |
| split.inverse-sampling.rate                | Int        | No       | 1000            | The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.                                                                                                                                                              |
| decimal_type_narrowing                     | Boolean | No       | true            | Decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now. Please refer to `decimal_type_narrowing` below                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| common-options                             |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |

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

#### split.even-distribution.factor.lower-bound

> Not recommended for use

The lower bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be greater than or equal to this lower bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is less, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 0.05.

#### split.even-distribution.factor.upper-bound

> Not recommended for use

The upper bound of the chunk key distribution factor. This factor is used to determine whether the table data is evenly distributed. If the distribution factor is calculated to be less than or equal to this upper bound (i.e., (MAX(id) - MIN(id) + 1) / row count), the table chunks would be optimized for even distribution. Otherwise, if the distribution factor is greater, the table will be considered as unevenly distributed and the sampling-based sharding strategy will be used if the estimated shard count exceeds the value specified by `sample-sharding.threshold`. The default value is 100.0.

#### split.sample-sharding.threshold

This configuration specifies the threshold of estimated shard count to trigger the sample sharding strategy. When the distribution factor is outside the bounds specified by `chunk-key.even-distribution.factor.upper-bound` and `chunk-key.even-distribution.factor.lower-bound`, and the estimated shard count (calculated as approximate row count / chunk size) exceeds this threshold, the sample sharding strategy will be used. This can help to handle large datasets more efficiently. The default value is 1000 shards.

#### split.inverse-sampling.rate

The inverse of the sampling rate used in the sample sharding strategy. For example, if this value is set to 1000, it means a 1/1000 sampling rate is applied during the sampling process. This option provides flexibility in controlling the granularity of the sampling, thus affecting the final number of shards. It's especially useful when dealing with very large datasets where a lower sampling rate is preferred. The default value is 1000.

#### partition_column [string]

The column name for split data.

#### partition_upper_bound [BigDecimal]

The partition_column max value for scan, if not set SeaTunnel will query database get max value.

#### partition_lower_bound [BigDecimal]

The partition_column min value for scan, if not set SeaTunnel will query database get min value.

#### partition_num [int]

> Not recommended for use, The correct approach is to control the number of split through `split.size`

How many splits do we need to split into, only support positive integer. default value is job parallelism.

## tips

> If the table can not be split(for example, table have no Primary Key or Unique Index, and `partition_column` is not set), it will run in single concurrency.
>
> Use `table_path` to replace `query` for single table reading. If you need to read multiple tables, use `table_list`.

## Task Example

### Simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its fields. You can also specify which fields to query for final output to the console.

```
# Defining the runtime environment
env {
  parallelism = 4
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        user = "root"
        password = "123456"
        query = "SELECT * FROM TEST_TABLE"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### parallel by partition_column

> Read your query table in parallel with the shard field you configured and the shard data  You can do this if you want to read the whole table

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "SELECT * FROM TEST_TABLE"
        # Parallel sharding reads fields
        partition_column = "ID"
        # Number of fragments
        partition_num = 10
        properties {
        database.oracle.jdbc.timezoneAsRegion = "false"
        }
    }
}
sink {
  Console {}
}
```

### parallel by Primary Key or Unique Index

> Configuring `table_path` will turn on auto split, you can configure `split.*` to adjust the split strategy

```
env {
  parallelism = 4
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        table_path = "DA.SCHEMA1.TABLE1"
        query = "select * from SCHEMA1.TABLE1"
        split.size = 10000
    }
}

sink {
  Console {}
}
```

### Parallel Boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read your data source according to the upper and lower boundaries you configured

```
source {
    Jdbc {
        url = "jdbc:oracle:thin:@datasource01:1523:xe"
        driver = "oracle.jdbc.OracleDriver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # Define query logic as required
        query = "SELECT * FROM TEST_TABLE"
        partition_column = "ID"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

### Multiple table read:

***Configuring `table_list` will turn on auto split, you can configure `split.*` to adjust the split strategy***

```hocon
env {
  job.mode = "BATCH"
  parallelism = 4
}
source {
  Jdbc {
    url = "jdbc:oracle:thin:@datasource01:1523:xe"
    driver = "oracle.jdbc.OracleDriver"
    connection_check_timeout_sec = 100
    user = "root"
    password = "123456"
    "table_list"=[
        {
            "table_path"="XE.TEST.USER_INFO"
        },
        {
            "table_path"="XE.TEST.YOURTABLENAME"
        }
    ]
    #where_condition= "where id > 100"
    split.size = 10000
    #split.even-distribution.factor.upper-bound = 100
    #split.even-distribution.factor.lower-bound = 0.05
    #split.sample-sharding.threshold = 1000
    #split.inverse-sampling.rate = 1000
  }
}

sink {
  Console {}
}
```

