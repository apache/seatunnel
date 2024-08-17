# HiveJdbc

> JDBC Hive Source Connector

## Support Hive Version

- Definitely supports 3.1.3 and 3.1.2, other versions need to be tested.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

> supports query SQL and can achieve projection effect.

## Description

Read external data source data through JDBC.

## Supported DataSource Info

| Datasource |                    Supported versions                    |             Driver              |                 Url                  |                                  Maven                                   |
|------------|----------------------------------------------------------|---------------------------------|--------------------------------------|--------------------------------------------------------------------------|
| Hive       | Different dependency version has different driver class. | org.apache.hive.jdbc.HiveDriver | jdbc:hive2://localhost:10000/default | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc) |

## Database Dependency

> Please download the support list corresponding to 'Maven' and copy it to the '$SEATNUNNEL_HOME/plugins/jdbc/lib/'
> working directory<br/>
> For example Hive datasource: cp hive-jdbc-xxx.jar $SEATNUNNEL_HOME/plugins/jdbc/lib/

## Data Type Mapping

|                                      Hive Data Type                                       | SeaTunnel Data Type |
|-------------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                                   | BOOLEAN             |
| TINYINT<br/> SMALLINT                                                                     | SHORT               |
| INT<br/>INTEGER                                                                           | INT                 |
| BIGINT                                                                                    | LONG                |
| FLOAT                                                                                     | FLOAT               |
| DOUBLE<br/>DOUBLE PRECISION                                                               | DOUBLE              |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.<38) | DECIMAL(x,y)        |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.>38) | DECIMAL(38,18)      |
| CHAR<br/>VARCHAR<br/>STRING                                                               | STRING              |
| DATE                                                                                      | DATE                |
| DATETIME<br/>TIMESTAMP                                                                    | TIMESTAMP           |
| BINARY<br/>  ARRAY <br/>INTERVAL <br/>MAP   <br/>STRUCT<br/>UNIONTYPE                     | Not supported yet   |

## Source Options

|             Name             |    Type    | Required |     Default     |                                                                                                                            Description                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | Yes      | -               | The URL of the JDBC connection. Refer to a case: jdbc:hive2://localhost:10000/default                                                                                                                                                                             |
| driver                       | String     | Yes      | -               | The jdbc class name used to connect to the remote data source,<br/> if you use Hive the value is `org.apache.hive.jdbc.HiveDriver`.                                                                                                                               |
| user                         | String     | No       | -               | Connection instance user name                                                                                                                                                                                                                                     |
| password                     | String     | No       | -               | Connection instance password                                                                                                                                                                                                                                      |
| query                        | String     | Yes      | -               | Query statement                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int        | No       | 30              | The time in seconds to wait for the database operation used to validate the connection to complete                                                                                                                                                                |
| partition_column             | String     | No       | -               | The column name for parallelism's partition, only support numeric type,Only support numeric type primary key, and only can config one column.                                                                                                                     |
| partition_lower_bound        | BigDecimal | No       | -               | The partition_column min value for scan, if not set SeaTunnel will query database get min value.                                                                                                                                                                  |
| partition_upper_bound        | BigDecimal | No       | -               | The partition_column max value for scan, if not set SeaTunnel will query database get max value.                                                                                                                                                                  |
| partition_num                | Int        | No       | job parallelism | The number of partition count, only support positive integer. default value is job parallelism                                                                                                                                                                    |
| fetch_size                   | Int        | No       | 0               | For queries that return a large number of objects,you can configure<br/> the row fetch size used in the query toimprove performance by<br/> reducing the number database hits required to satisfy the selection criteria.<br/> Zero means use jdbc default value. |
| common-options               |            | No       | -               | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details                                                                                                                                                 |
| useKerberos                  | Boolean    | No       | no              | Whether to enable Kerberos, default is false                                                                                                                                                                                                                      |
| kerberos_principal           | String     | No       | -               | When use kerberos, we should set kerberos principal such as 'test_user@xxx'.                                                                                                                                                                                      |
| kerberos_keytab_path         | String     | No       | -               | When use kerberos, we should set kerberos principal file path such as '/home/test/test_user.keytab' .                                                                                                                                                             |
| krb5_path                    | String     | No       | /etc/krb5.conf  | When use kerberos, we should set krb5 path file path such as '/seatunnel/krb5.conf' or use the default path '/etc/krb5.conf '.                                                                                                                                    |

### Tips

> If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed
> in parallel according to the concurrency of tasks , When your shard read field is a large number type such as bigint(
> and above and the data is not evenly distributed, it is recommended to set the parallelism level to 1 to ensure that
> the
> data skew problem is resolved

## Task Example

### Simple:

> This example queries type_bin 'table' 16 data in your test "database" in single parallel and queries all of its
> fields. You can also specify which fields to query for final output to the console.

```
# Defining the runtime environment
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        query = "select * from type_bin limit 16"
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

### Parallel:

> Read your query table in parallel with the shard field you configured and the shard data You can do this if you want
> to read the whole table

```
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        # Parallel sharding reads fields
        partition_column = "id"
        # Number of fragments
        partition_num = 10
    }
}
```

### Parallel Boundary:

> It is more efficient to specify the data within the upper and lower bounds of the query It is more efficient to read
> your data source according to the upper and lower boundaries you configured

```
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

