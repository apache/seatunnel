# JDBC

> JDBC source connector

## Description

Read external data source data through JDBC.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)

supports query SQL and can achieve projection effect.

- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name                         | type   | required | default value |
|------------------------------|--------|----------|---------------|
| url                          | String | Yes      | -             |
| driver                       | String | Yes      | -             |
| user                         | String | No       | -             |
| password                     | String | No       | -             |
| query                        | String | Yes      | -             |
| connection_check_timeout_sec | Int    | No       | 30            |
| partition_column             | String | No       | -             |
| partition_upper_bound        | Long   | No       | -             |
| partition_lower_bound        | Long   | No       | -             |

### driver [string]
The jdbc class name used to connect to the remote data source, if you use MySQL the value is com.mysql.cj.jdbc.Driver.
Warn: for license compliance, you have to provide MySQL JDBC driver yourself, e.g. copy mysql-connector-java-xxx.jar to $SEATNUNNEL_HOME/lib for Standalone.

### user [string]
userName

### password [string]
password

### url [string]
The URL of the JDBC connection. Refer to a case: jdbc:postgresql://localhost/test

### query [string]
Query statement

### connection_check_timeout_sec [int]

The time in seconds to wait for the database operation used to validate the connection to complete.

### partition_column [string]
The column name for parallelism's partition, only support numeric type.


### partition_upper_bound [long]
The partition_column max value for scan, if not set SeaTunnel will query database get max value.


### partition_lower_bound [long]
The partition_column min value for scan, if not set SeaTunnel will query database get min value.

## tips
If partition_column is not set, it will run in single concurrency, and if partition_column is set, it will be executed in parallel according to the concurrency of tasks.


## appendix
there are some reference value for params above.

| datasource | driver                   | url                                       | maven                                                         |
|------------|--------------------------|-------------------------------------------|---------------------------------------------------------------|
| mysql      | com.mysql.cj.jdbc.Driver | jdbc:mysql://localhost:3306/test          | https://mvnrepository.com/artifact/mysql/mysql-connector-java |
| postgresql | org.postgresql.Driver    | jdbc:postgresql://localhost:5432/postgres | https://mvnrepository.com/artifact/org.postgresql/postgresql  |                                                             |
| dm         | dm.jdbc.driver.DmDriver  | jdbc:dm://localhost:5236                  | https://mvnrepository.com/artifact/com.dameng/DmJdbcDriver18  |

## Example
simple:
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
parallel:
```
    Jdbc {
        url = "jdbc:mysql://localhost/test?serverTimezone=GMT%2b8"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from type_bin"
        partition_column= "id"
    }
```
